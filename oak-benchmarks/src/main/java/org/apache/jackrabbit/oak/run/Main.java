/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.run;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.commons.run.Command;
import org.apache.jackrabbit.oak.commons.run.Modes;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

import static java.util.Arrays.copyOfRange;
import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;

public final class Main {
    private static final Modes MODES = new Modes(ImmutableMap.<String, Command>of(
        "benchmark", new BenchmarkCommand(),
        "scalability", new ScalabilityCommand()
    ));

    private Main() {
        // Prevent instantiation.
    }

    public static void main(String[] args) throws Exception {
        printProductInfo(args);

        Command c = MODES.getCommand("benchmark");
        if (args.length > 0) {
            c = MODES.getCommand(args[0]);

            if (c == null) {
                c = MODES.getCommand("benchmark");
            }

            args = copyOfRange(args, 1, args.length);
        }

        c.execute(args);
    }

    public static String getProductInfo(){
        String version = getProductVersion();

        if (version == null) {
            return "Apache Jackrabbit Oak";
        }

        return "Apache Jackrabbit Oak " + version;
    }

    private static String getProductVersion() {
        InputStream stream = Main.class.getResourceAsStream("/META-INF/maven/org.apache.jackrabbit/oak-benchmarks/pom.properties");

        try {
            if (stream == null) {
                return null;
            } else {
                return getProductVersion(stream);
            }
        } finally {
            closeQuietly(stream);
        }
    }

    private static String getProductVersion(InputStream stream) {
        Properties properties = new Properties();

        try {
            properties.load(stream);
        } catch (IOException e) {
            return null;
        }

        return properties.getProperty("version");
    }

    private static void printProductInfo(String[] args) {
        if(!Arrays.asList(args).contains("--quiet")) {
            System.out.println(getProductInfo());
        }
    }

//    private static Mode getMode(String name) {
//        try {
//            return Mode.valueOf(name.toUpperCase(Locale.ENGLISH));
//        } catch (IllegalArgumentException e) {
//            return null;
//        }
//    }

}
