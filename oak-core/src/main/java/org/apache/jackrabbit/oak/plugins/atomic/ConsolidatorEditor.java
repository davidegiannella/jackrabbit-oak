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

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsolidatorEditor extends DefaultEditor {
    private static final Logger LOG = LoggerFactory.getLogger(ConsolidatorEditor.class);
    private NodeBuilder builder;
    private String path;
    
    private static boolean isProperty(@Nonnull final PropertyState p) {
        return checkNotNull(p).getName().startsWith(AtomicCounterEditor.PREFIX_PROP_COUNTER);
    }
     public ConsolidatorEditor(NodeBuilder builder) {
        this("", builder);
    }
    
    public ConsolidatorEditor(String path, NodeBuilder builder) {
        this.path = path;
        this.builder = builder;
    }
    
    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        if (isProperty(after)) {
            LOG.debug("propertyAdded");
            AtomicCounterEditor.consolidateCount(builder);
        }
    }
    
    @Override
    public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
        return new ConsolidatorEditor(path + '/' + name, builder.getChildNode(name));
    }

    @Override
    public Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        return new ConsolidatorEditor(path + '/' + name, builder.getChildNode(name));
    }

}
