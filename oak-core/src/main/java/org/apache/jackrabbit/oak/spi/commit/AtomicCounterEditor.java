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
package org.apache.jackrabbit.oak.spi.commit;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.MIX_ATOMIC_COUNTER;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

public class AtomicCounterEditor extends DefaultEditor {
    private static final Logger LOG = LoggerFactory.getLogger(AtomicCounterEditor.class);
    private final NodeBuilder builder;
    
    public AtomicCounterEditor(@Nonnull final NodeBuilder builder) {
        checkNotNull(builder);
        this.builder = builder;
    }

    private static boolean shallWeProcess(@Nonnull final NodeState state) {
        checkNotNull(state);
        PropertyState mixin = state.getProperty(JCR_MIXINTYPES);
        return mixin != null
               && Iterators.contains(mixin.getValue(NAMES).iterator(), MIX_ATOMIC_COUNTER);
    }
    
    @Override
    public void enter(NodeState before, NodeState after) throws CommitFailedException {
        LOG.debug("enter - before: {}, after: {}", before, after);
        super.enter(before, after);
    }

    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        LOG.debug("leave - before: {}, after: {}", before, after);
        super.leave(before, after);
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        LOG.debug("propertyAdded - after: {}", after);
        super.propertyAdded(after);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        LOG.debug("propertyChanged - before: {}, after: {}", before, after);
        super.propertyChanged(before, after);
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        LOG.debug("propertyDeleted - before: {}", before);
        super.propertyDeleted(before);
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
        LOG.debug("childNodeAdded  - name: {}, after: {}", name, after);
        if (shallWeProcess(after)) {
            return new AtomicCounterEditor(builder.getChildNode(name));
        }
        return null;
    }

    @Override
    public Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        LOG.debug("childNodeChanged - name: {}, before: {}, after: {}", name, before, after);
        return super.childNodeChanged(name, before, after);
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        LOG.debug("childNodeDeleted - name: {}, before: {}", name, before);
        return super.childNodeDeleted(name, before);
    }
}
