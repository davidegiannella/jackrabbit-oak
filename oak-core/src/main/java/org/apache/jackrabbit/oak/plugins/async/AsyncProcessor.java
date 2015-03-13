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
package org.apache.jackrabbit.oak.plugins.async;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.commit.AnnotatingConflictHandler;
import org.apache.jackrabbit.oak.plugins.commit.ConflictHook;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;

/**
 * abstract class from which an asynchronous processor could extend in order to inherit some utility
 * methods
 */
public abstract class AsyncProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncProcessor.class);
    
    /**
     * Name of the hidden node under which information about the checkpoints
     * seen and indexed by each async indexer is kept.
     */
    public static final String ASYNC = ":async";
    public static final long DEFAULT_LIFETIME = TimeUnit.DAYS.toMillis(1000);
    
    /**
     * Default delay in seconds on which the async process will run. Can be changed by providing on
     * startup something like {@code -Doak.async.delay=10} where {@code 10} are the desired seconds
     */
    public static final long DEFAULT_DELAY = Long.getLong("oak.async.delay", 5L);
    
    protected static final CommitFailedException CONCURRENT_UPDATE = new CommitFailedException(
                "Async", 1, "Concurrent update detected"); 

    /**
     * Timeout in milliseconds after which an async job would be considered as
     * timed out. Another node in cluster would wait for timeout before
     * taking over a running job
     */
    protected static final long ASYNC_TIMEOUT;

    static {
        int value = 15;
        try {
            value = Integer.parseInt(System.getProperty(
                    "oak.async.lease.timeout", "15"));
        } catch (NumberFormatException e) {
            // use default
        }
        ASYNC_TIMEOUT = TimeUnit.MINUTES.toMillis(value);
    }

    protected static void mergeWithConcurrencyCheck(final NodeBuilder builder, 
                                                    final String checkpoint, 
                                                    final long lease,
                                                    final String name,
                                                    final NodeStore store) throws CommitFailedException {
        CommitHook concurrentUpdateCheck = new CommitHook() {
            @Override @Nonnull
            public NodeState processCommit(
                    NodeState before, NodeState after, CommitInfo info)
                    throws CommitFailedException {
                // check for concurrent updates by this async task
                NodeState async = before.getChildNode(ASYNC);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("concurrentUpdateCheck.processCommit()");
                    LOG.trace("checkpoint: {}", checkpoint);
                    LOG.trace("async.name: {}", async.getString(name));
                    LOG.trace("async.lease: {}", async.getLong(name + "-lease"));
                    LOG.trace("lease: {}", lease);
                }
                if (checkpoint == null || Objects.equal(checkpoint, async.getString(name))
                        && lease == async.getLong(name + "-lease")) {
                    return after;
                } else {
                    throw CONCURRENT_UPDATE;
                }
            }
        };
        CompositeHook hooks = new CompositeHook(
                new ConflictHook(new AnnotatingConflictHandler()),
                new EditorHook(new ConflictValidatorProvider()),
                concurrentUpdateCheck);
        store.merge(builder, hooks, CommitInfo.EMPTY);
    }
}
