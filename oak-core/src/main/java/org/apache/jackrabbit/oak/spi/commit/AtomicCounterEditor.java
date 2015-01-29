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
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.MIX_ATOMIC_COUNTER;

import java.util.UUID;

import javax.annotation.Nonnull;
import javax.jcr.query.qom.NodeName;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

public class AtomicCounterEditor extends DefaultEditor {
    /**
     * property to be set for incrementing/decrementing the counter
     */
    public static final String PROP_INCREMENT = "oak:increment";
    
    /**
     * property with the consolidated counter
     */
    public static final String PROP_COUNTER = "oak:counter";
    
    /**
     * prefix used internally for tracking the counting requests
     */
    public static final String PREFIX_PROP_COUNTER = ":oak-counter-";
    
    private static final Logger LOG = LoggerFactory.getLogger(AtomicCounterEditor.class);
    private final NodeBuilder builder;
    private final String nodeName;
    
    public AtomicCounterEditor(@Nonnull final NodeBuilder builder) {
        this(null, builder);
    }
    
    private AtomicCounterEditor(final String nodeName, @Nonnull final NodeBuilder builder) {
        checkNotNull(builder);
        this.builder = builder;
        this.nodeName = null;
    }
    
    private static boolean shallWeProcessNode(@Nonnull final NodeBuilder state) {
        checkNotNull(state);
        PropertyState mixin = state.getProperty(JCR_MIXINTYPES);
        return mixin != null
               && Iterators.contains(mixin.getValue(NAMES).iterator(), MIX_ATOMIC_COUNTER);
    }
    
    private static boolean shallWeProcessProperty(final PropertyState property, final String nodeName, final NodeBuilder builder) {
        boolean process = false;
        if (shallWeProcessNode(builder) && property != null && PROP_INCREMENT.equals(property.getName())) {
            if (LONG.equals(property.getType())) {
                process = true;
            } else {
                LOG.warn(
                    "although the {} property is set is not of the right value: LONG. Not processing node: {}.",
                    PROP_INCREMENT, nodeName);
            }
        }
        return process;
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
        if (shallWeProcessProperty(after, nodeName, builder)) {
            builder.setProperty(PREFIX_PROP_COUNTER + UUID.randomUUID(), after.getValue(LONG), LONG);
        }
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
        if (shallWeProcessNode(new ReadOnlyBuilder(after))) {
            return new AtomicCounterEditor(name, builder.getChildNode(name));
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
