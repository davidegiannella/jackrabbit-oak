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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.commit.AsyncEditorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StopwatchLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

/**
 * the director which orchestrate the asynchronous editors
 */
public class AsyncEditorProcessor extends AsyncProcessor implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncEditorProcessor.class);
    
    /**
     * the list of asynchronous editor providers
     */
    private final List<AsyncEditorProvider> editorProviders;
    private final NodeStore store;
    private final String name;
    
    /**
     * instantiate an asynchronous processor for editor
     * 
     * @param name the name to be associated to the process. Cannot be null or empty.
     * @param store the current NodeStore. Cannot be null
     * @param editorProviders the list of editorProviders. Cannot be null.
     */
    public AsyncEditorProcessor(@Nonnull final String name, 
                                @Nonnull final NodeStore store, 
                                @Nonnull final List<AsyncEditorProvider> editorProviders) {
        
        checkArgument(!Strings.isNullOrEmpty(name), "the process name cannot be null");
        
        this.editorProviders = Collections.unmodifiableList(checkNotNull(editorProviders));
        this.store = checkNotNull(store);
        this.name = name;
    }
    
    @Override
    public void run() {
        StopwatchLogger swl = new StopwatchLogger(LOG);
        swl.start("Processing asynchronous editors started -");
        
        if (!editorProviders.isEmpty()) {
            NodeState root = store.getRoot();

            // check for concurrent updates
            NodeState async = root.getChildNode(ASYNC);
            long leaseEndTime = async.getLong(name + "-lease");
            long currentTime = System.currentTimeMillis();
            if (leaseEndTime > currentTime) {
                swl.stop(
                    String.format(
                        "Another copy of '%s' is running. " +
                        "Time left before expiration: %ds. Skipping. Process completed in",
                        name,
                        (leaseEndTime - currentTime) / 1000
                        )
                    );
                closeStopwatch(swl);
                return;
            }
            
            // find the last checkpoint state, and check if there are recent changes
            NodeState before;
            String beforeCheckpoint = async.getString(name);
            if (beforeCheckpoint != null) {
                NodeState state = store.retrieve(beforeCheckpoint);
                if (state == null) {
                    LOG.warn("Failed to retrieve previously checkpoint '{}'"
                            + " re-running the initial update for '{}'",
                            beforeCheckpoint, name);
                    beforeCheckpoint = null;
                    before = MISSING_NODE;
                } else if (noVisibleChanges(state, root)) {
                    swl.stop("No changes since last checkpoint;"
                            + " skipping the update");
                    closeStopwatch(swl);
                    return;
                } else {
                    before = state;
                }
            } else {
                LOG.info("Initial '{}' update", name);
                before = MISSING_NODE;
            }

        }
        
        swl.stop("Processing asynchronous editors completed in");
        closeStopwatch(swl);
    }
    
    private static void closeStopwatch(@Nonnull final StopwatchLogger swl) {
        try {
            checkNotNull(swl).close();
        } catch (IOException e) {
            LOG.debug("ignoring", e);
        }
    }
}
