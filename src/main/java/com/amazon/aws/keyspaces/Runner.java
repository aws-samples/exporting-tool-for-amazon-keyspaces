// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.aws.keyspaces;


import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

@CommandLine.Command(name = "AKExportTool", mixinStandardHelpOptions = true, version = "1.0",
        description = "Exports data from Amazon Keyspaces to HDFS in the parquet format.")

public class Runner implements Callable<Integer> {

    private static final Logger LOG = Logger.getLogger(Runner.class.getName());
    // Default timeout is 86,400 seconds
    private static final long ONE_DAY = 1*24*60*60;


    @CommandLine.Option(names = "--recover", description = "Recovering mode")
    boolean recoveringMode = false;
    @CommandLine.Parameters(index = "0", description = "Destination of parquet files")
    private String dataLocation = System.getProperty("user.dir") + "/" + "output";
    @CommandLine.Parameters(index = "1", description = "Query")
    private String query;
    @CommandLine.Option(names = {"-p", "--page"}, description = "Starting page")
    private int startingPage = 0;
    @CommandLine.Option(names = {"-r", "--rateLimiter"}, description = "A rate limiter")
    private int rateLimiter = 3000;
    @CommandLine.Option(names = {"-t", "--timeout"}, description = "Override default timeout 86400 seconds")
    private long timeout = ONE_DAY;
    private final File configFile = new File(System.getProperty("user.dir") + "/application.conf");

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException, InterruptedException {
        int exitCode = new CommandLine(new Runner()).execute(args);
        System.exit(exitCode);
    }

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
            countDownLatch.await(timeout, TimeUnit.SECONDS);
            persistCassandraRowToParquet.close();
            LOG.info("Amazon Keyspaces Session is closed");
            long elapsedTime = System.nanoTime() - startTime;
            LOG.info("Elapsed time in seconds:" + TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS));
        }

        if (stateFile.exists() == true &&
                recoveringMode == true) {

            LOG.info("Found state.ser file");
            State state = Utils.readState();
            LOG.info("Overriding the parameters by parameters from state.ser");
            LOG.info("Restoring the exporting process from the error:" + state.getErrorMessage());
            LOG.info("Restoring position " + state.getProcessedPages());
            long startTime = System.nanoTime();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            PersistCassandraRowToParquet persistCassandraRowToParquet = new PersistCassandraRowToParquet(
                    state.getQuery(),
                    state.getPath(),
                    countDownLatch,
                    configFile,
                    state.getProcessedPages(),
                    // 11 pages * 3500 rows per page * 100 bytes per row = 3,759 kilobytes per partition
                    Math.min(11, (int) Math.round(state.getPageRate()))
            );

            LOG.info("Export in progress...");
            LOG.info("Dest:" + dataLocation);
            persistCassandraRowToParquet.start();
            countDownLatch.await(timeout, TimeUnit.SECONDS);
            persistCassandraRowToParquet.close();
            LOG.info("Amazon Keyspaces Session is closed");
            long elapsedTime = System.nanoTime() - startTime;
            LOG.info("Elapsed time in seconds:" + TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS));
            // Let's rename our old state.ser to another file
            File stateFileOld = new File("state_" + Instant.now().toString() + ".ser");
            stateFile.renameTo(stateFileOld);
        }
        return 0;
    }
}
