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
package org.apache.jackrabbit.oak.plugins.atomic;

import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Provide an instance of {@link AtomicCounterEditor}
 */
public class AtomicCounterEditorProvider implements EditorProvider {
    private String instanceId;
    private ScheduledExecutorService executor;
    
    /**
     * See
     * {@link AtomicCounterEditor#AtomicCounterEditor(NodeBuilder, String, ScheduledExecutorService)}
     * for details on behaviour when {@code instanceId} and/or {@code executor} are null.
     * 
     * @param instanceId the current Oak instance Id.
     * @param executor the Oak executor on which the consolidating thread will be scheduled.
     */
    public AtomicCounterEditorProvider(@Nullable String instanceId, 
                                       @Nullable ScheduledExecutorService executor) {
        this.instanceId = instanceId;
        this.executor = executor;
    }
    
    @Override
    public Editor getRootEditor(final NodeState before, final NodeState after,
                                final NodeBuilder builder, final CommitInfo info)
                                    throws CommitFailedException {        
        return new AtomicCounterEditor(builder, instanceId, executor);
    }
}
