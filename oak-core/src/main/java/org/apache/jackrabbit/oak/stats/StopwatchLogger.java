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
package org.apache.jackrabbit.oak.stats;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to be used for tracking of timing within methods. It makes use of the
 * {@link Clock.Fast} for speeding up the operation despite loosing accuracy.
 */
public class StopwatchLogger {
    private static final Logger LOG = LoggerFactory.getLogger(StopwatchLogger.class);
    private static final ScheduledExecutorService EXECUTOR = newSingleThreadScheduledExecutor();
    
    private final String clazz;
    
    private Clock clock;
    
    public StopwatchLogger(@Nonnull final String clazz) {
        this.clazz = checkNotNull(clazz);
    }

    public StopwatchLogger(@Nonnull final Class<?> clazz) {
        this.clazz = checkNotNull(clazz).getName().toString();
    }

    /**
     * starts the clock
     */
    public void start() {
        clock = new Clock.Fast(EXECUTOR);
    }
    
    /**
     * track of an intermediate time without stopping the ticking.
     * 
     * @param message
     */
    public void split(@Nullable final String message) {
        track(clock, clazz, message);
    }
    
    /**
     * track the time and stop the clock.
     * 
     * @param message
     */
    public void stop(@Nullable final String message) {
        track(clock, clazz, message);
        clock = null;
    }

    /**
     * convenience method for tracking the messages
     * 
     * @param clock
     * @param clazz
     * @param message
     */
    private static void track(@Nullable final Clock clock,
                              @Nonnull final String clazz,
                              @Nullable final String message) {
        
        if (clock == null) {
            LOG.debug("{} - clock has not been started yet.", clazz);
        } else {
            LOG.debug(
                "{} - {} {}",
                new Object[] { checkNotNull(clazz), message == null ? "" : message,
                              clock.getTimeMonotonic() });
        }
    }
}
