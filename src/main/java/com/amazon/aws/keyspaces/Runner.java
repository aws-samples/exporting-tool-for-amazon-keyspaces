/*
This sample, non-production-ready Export Tool.
© 2020 Amazon Web Services, Inc. or its affiliates. All Rights Reserved.
This AWS Content is provided subject to the terms of the AWS Customer Agreement available at http://aws.amazon.com/agreement or
other written agreement between Customer and eitherAmazon Web Services, Inc. or Amazon Web Services EMEA SARL or both.
*/

package com.amazon.aws.keyspaces;


import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import java.time.Instant;
import java.util.concurrent.*;
import java.util.logging.Logger;

@CommandLine.Command(name = "AKExportTool", mixinStandardHelpOptions = true, version = "AKExportTool 1.0",
        description = "Exports data from Amazon Keyspaces to HDFS in the parquet format.")

public class Runner implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "Destination of parquet files")
    private String dataLocation = System.getProperty("user.dir")+"/"+"output";
    @CommandLine.Parameters(index = "1", description = "Query")
    private String query;
    @CommandLine.Option(names = {"-p", "--page"}, description = "Starting page")
    private int startingPage=0;
    @CommandLine.Option(names = {"-r", "--rateLimiter"}, description = "A rate limiter")
    private int rateLimiter=3000;
    @CommandLine.Option(names = "--recover", description = "Recovering mode")
    boolean recoveringMode = false;


    private File configFile = new File(System.getProperty("user.dir")+"/application.conf");
    private static final Logger LOG = Logger.getLogger(Runner.class.getName());

    @Override
    public Integer call() throws Exception {
        File stateFile = new File("state.ser");
        if (stateFile.exists() == false) {
            long startTime = System.nanoTime();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            PersistCassandraRowToParquet persistCassandraRowToParquet = new PersistCassandraRowToParquet(query,
                    dataLocation,
                    countDownLatch,
                    configFile,
                    startingPage,
                    rateLimiter);
            LOG.info("Export in progress...");
            LOG.info("Dest:" + dataLocation);
            persistCassandraRowToParquet.start();
            countDownLatch.await(604800, TimeUnit.SECONDS);
            persistCassandraRowToParquet.close();
            LOG.info("Amazon Keyspaces Session is closed");
            long elapsedTime = System.nanoTime() - startTime;
            LOG.info("Elapsed time in seconds:" + TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS));
        }

        if (stateFile.exists() == true &&
                recoveringMode == true)
        {

            LOG.info("Found state.ser file");
            State state = Utils.readState();
            LOG.info("Overriding the parameters by parameters from state.ser");
            LOG.info("Restoring the exporting process from the error:"+state.errorMessage);
            LOG.info("Restoring position " + state.processedPages);
            long startTime = System.nanoTime();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            PersistCassandraRowToParquet persistCassandraRowToParquet = new PersistCassandraRowToParquet(
                    state.query,
                    state.path,
                    countDownLatch,
                    configFile,
                    state.processedPages,
                    Math.min(11, (int) Math.round(state.pageRate))
            );

            LOG.info("Export in progress...");
            LOG.info("Dest:" + dataLocation);
            persistCassandraRowToParquet.start();
            countDownLatch.await(604800, TimeUnit.SECONDS);
            persistCassandraRowToParquet.close();
            LOG.info("Amazon Keyspaces Session is closed");
            long elapsedTime = System.nanoTime() - startTime;
            LOG.info("Elapsed time in seconds:" + TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS));
            // Let's rename our old state.ser to another file
            File stateFileOld = new File("state_"+Instant.now().toString()+".ser");
            stateFile.renameTo(stateFileOld);
        }
        return 0;
    }

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException, InterruptedException {
        int exitCode = new CommandLine(new Runner()).execute(args);
        System.exit(exitCode);
    }
}
