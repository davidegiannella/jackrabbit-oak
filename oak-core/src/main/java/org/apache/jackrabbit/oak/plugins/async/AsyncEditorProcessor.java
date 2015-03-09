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
import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.spi.commit.AsyncEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditor;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StopwatchLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

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
        swl.start("Started -");
        
        if (!editorProviders.isEmpty()) {
            NodeState root = store.getRoot();
            NodeBuilder builder = store.getRoot().builder();

            // check for concurrent updates
            NodeState async = root.getChildNode(ASYNC);
            
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
                LOG.info("Initial update");
                before = MISSING_NODE;
            }
            
            // creating the checkpoint
            String afterCheckpoint = store.checkpoint(DEFAULT_LIFETIME, ImmutableMap.of(
                    "creator", AsyncEditorProcessor.class.getSimpleName(),
                    "thread", Thread.currentThread().getName()));
            NodeState after = store.retrieve(afterCheckpoint);
            if (after == null) {
                swl.stop(
                    String.format(
                        "Unable to retrieve newly created checkpoint %s, skipping the update",
                        afterCheckpoint
                    ));
                closeStopwatch(swl);
                return;
            }

            swl.split(String.format("checkpoints before: '%s', after: '%s' done in",
                beforeCheckpoint, afterCheckpoint));
            
            String checkpointToRelease = afterCheckpoint;

            try {
                // process commit hooks
                List<Editor> editors = newArrayList();
                for (EditorProvider provider : editorProviders) {
                    editors.add(provider.getRootEditor(before, after, builder, CommitInfo.EMPTY));
                }
                Editor editor = CompositeEditor.compose(editors);
                CommitFailedException exception = EditorDiff.process(editor,
                    before, after);
                
                if (exception != null) {
                    LOG.debug("Exception found. Throwing it up.");
                    throw exception;
                }
                store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                checkpointToRelease = beforeCheckpoint;
            } catch (CommitFailedException e) {
                LOG.error("Error while processing commit hooks", e);
            } finally {
                if (checkpointToRelease != null) {
                    LOG.debug("releasing checkpoint: {}", checkpointToRelease);
                    store.release(checkpointToRelease);
                }
            }
            
            // cleaning up lease time 
            builder.child(ASYNC).removeProperty(name + "-lease");
        }
        
        swl.stop("Completed in");
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
