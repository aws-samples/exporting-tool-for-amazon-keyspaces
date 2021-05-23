/*
This sample, non-production-ready Export Tool.
© 2020 Amazon Web Services, Inc. or its affiliates. All Rights Reserved.
This AWS Content is provided subject to the terms of the AWS Customer Agreement available at http://aws.amazon.com/agreement or
other written agreement between Customer and eitherAmazon Web Services, Inc. or Amazon Web Services EMEA SARL or both.
*/

package com.amazon.aws.keyspaces;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.kitesdk.data.spi.JsonUtil;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;

import java.security.NoSuchAlgorithmException;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;
import java.util.logging.Level;

public class PersistCassandraRowToParquet {

    private static final Logger LOG = Logger.getLogger(PersistCassandraRowToParquet.class.getName());
    private static long processedPage = 0;

    private String path;
    private Schema schema;
    private String query;
    private CqlSession session1;
    private CountDownLatch latch;
    private long startingPage;
    private RateLimiter pageLimiter;

    public static void writeToParquet(List<String> recordsToWrite, String path, Schema SCHEMA) throws IOException {
        //Let's turned off INFO level for org.apache.parquet.hadoop
        Logger.getLogger("org.apache.parquet.hadoop").setLevel(Level.SEVERE);

        long epoch = Instant.now().toEpochMilli();
        String fileName = processedPage+ "-" + epoch;
        Path fileToWrite = new Path(path + fileName+".parquet");
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                .<GenericData.Record>builder(fileToWrite)
                .withSchema(SCHEMA)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            for (String record : recordsToWrite) {
                writer.write(parseJson(record, SCHEMA));
            }
        }
    }

    private static GenericData.Record parseJson(String json, Schema schema)
            throws IOException {
        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, json);

        DatumReader<GenericData.Record> reader =
                new GenericDatumReader<>(schema);
        return reader.read(null, decoder);
    }

    void processRowsAsync(AsyncResultSet rs, Throwable error) {
        if (error != null) {
            LOG.warning(error.getMessage());
            LOG.info("You can restart the exporting process with following parameters: "+"\""+query+"\""+" "+"\""+path+"\""+" -p "+(processedPage)+" -r 10"+ " " +
                    "if the error persists please lower the page rate(-r) ");
            LOG.info("Processed pages:" + processedPage);
            LOG.info("Starting page:" + startingPage);
            State state = new State(startingPage, processedPage, pageLimiter.getRate(), query, path, error.getMessage());
            Utils.writeState(state);
            System.exit(-1);

        } else {
            if (processedPage>=startingPage) {
                List<String> records = new ArrayList<>();
                for (Row row : rs.currentPage()) {
                    records.add(row.getString(0));
                }
                try {
                    writeToParquet(records, path, schema);
                } catch (IOException e) {
                    System.err.println(e.getMessage());
                    System.exit(-1);
                }
            } else {
                LOG.fine("Skipping the page "+processedPage);
            }

            if (rs.hasMorePages()) {
                pageLimiter.acquire();
                rs.fetchNextPage().whenComplete(this::processRowsAsync);
                processedPage++;
                }
            else {
                LOG.info("Completed to read pages");
                LOG.info("Starting page was "+ startingPage);
                LOG.info("Pages were processed " + processedPage);
                latch.countDown();
            }
        }
    }

    public PersistCassandraRowToParquet(String query, String path, CountDownLatch countDownLatch, File driverConfig, long startingPage, int rateLimiter) throws NoSuchAlgorithmException, JsonProcessingException, InterruptedException {
        this.query = query;
        this.latch = countDownLatch;
        this.startingPage = startingPage;
        this.pageLimiter = RateLimiter.create(rateLimiter);

        Logger.getLogger("com.amazon.aws.keyspaces.PersistCassandraRowToParquet").setLevel(Level.INFO);

        session1 = CqlSession.builder()
                .withConfigLoader(DriverConfigLoader.fromFile(driverConfig))
                .withSslContext(SSLContext.getDefault())
                .addTypeCodecs(TypeCodecs.ZONED_TIMESTAMP_UTC)
                .build();

        LOG.info("Sampling the table to inference the avro schema");
        ResultSet rs = session1.execute(this.query);
        String json = rs.one().getString(0);
        LOG.info("Sample:" + json);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = mapper.readTree(json);
        Schema avroSchema = JsonUtil.inferSchema(actualObj, "exportedData");
        LOG.info("Avro Schema:" + avroSchema.toString());
        LOG.info("Sampling is completed");
        this.path = path;
        this.schema = avroSchema;
    }

    public void start() {
        CompletionStage<AsyncResultSet> futureRs =
                session1.executeAsync(this.query);
        futureRs.whenComplete(this::processRowsAsync);
    }

    public void close() {
        session1.close();
    }


}
