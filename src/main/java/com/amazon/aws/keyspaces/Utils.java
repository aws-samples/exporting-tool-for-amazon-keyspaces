// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.aws.keyspaces;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.logging.Logger;

public class Utils {
    private static final Logger LOG = Logger.getLogger(Utils.class.getName());

    public static void writeState(State state) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            FileOutputStream myFileOutputStream = new FileOutputStream(System.getProperty("user.dir") + "/state.ser");
            mapper.writeValue(myFileOutputStream, state);
        } catch (Exception e) {
            LOG.severe("Error when saving to the state file." + e.getMessage());
        }
    }

    public static State readState() {
        State state = new State();
        try {
            ObjectMapper mapper = new ObjectMapper();
            FileInputStream myFileInputStream = new FileInputStream(System.getProperty("user.dir") + "/state.ser");
            state = mapper.readValue(myFileInputStream, State.class);
        } catch (Exception e) {
            LOG.severe("Error when loading from the state file." + e.getMessage());
        }
        return state;
    }

}
