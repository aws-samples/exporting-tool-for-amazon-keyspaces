// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.aws.keyspaces;

import java.io.Serializable;

public class State implements Serializable {
    private static final long serialVersionUID = 13487495895819393L;
    private long startingPage;
    private long processedPages;
    private double pageRate;
    private String query;
    private String path;
    private String errorMessage;

    public State() {

    }

    public State(long startingPage, long processedPages, double pageRate, String query, String path, String errorMessage) {
        this.startingPage = startingPage;
        this.processedPages = processedPages;
        this.pageRate = pageRate;
        this.query = query;
        this.path = path;
        this.errorMessage = errorMessage;
    }

    public long getStartingPage() {
        return startingPage;
    }

    public void setStartingPage(long startingPage) {
        this.startingPage = startingPage;
    }

    public long getProcessedPages() {
        return processedPages;
    }

    public void setProcessedPages(long processedPages) {
        this.processedPages = processedPages;
    }

    public double getPageRate() {
        return pageRate;
    }

    public void setPageRate(long pageRate) {
        this.pageRate = pageRate;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}

