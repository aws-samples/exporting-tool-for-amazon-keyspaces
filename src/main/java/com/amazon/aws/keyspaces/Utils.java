// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.aws.keyspaces;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.logging.Logger;

public class Utils {
    private static final Logger LOG = Logger.getLogger(Utils.class.getName());

    public static void writeState(State state) {
        try {
            FileOutputStream myFileOutputStream = new FileOutputStream(System.getProperty("user.dir") + "/state.ser");
            ObjectOutputStream myObjectOutputStream = new ObjectOutputStream(myFileOutputStream);
            myObjectOutputStream.writeObject(state);
            myObjectOutputStream.close();
        } catch (Exception e) {
            LOG.severe("Error when saving to file." + e.getMessage());
        }
    }

    public static State readState() {
        State state = new State();
        try {
            FileInputStream myFileInputStream = new FileInputStream(System.getProperty("user.dir") + "/state.ser");
            ObjectInputStream myObjectInputStream = new ObjectInputStream(myFileInputStream);
            state = (State) myObjectInputStream.readObject();
            myObjectInputStream.close();
        } catch (Exception e) {
            LOG.severe("Error when loading from file." + e.getMessage());
        }
        return state;
    }
}
