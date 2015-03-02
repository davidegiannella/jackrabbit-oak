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
import static org.apache.jackrabbit.oak.api.Type.LONG;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AtomicCounterConsolidator extends AtomicCounterEditor {
    private static final Logger LOG = LoggerFactory.getLogger(AtomicCounterConsolidator.class);
    private final NodeBuilder builder;
    private final String path;
    private boolean update;
    
    public AtomicCounterConsolidator(@Nonnull final NodeBuilder builder) {
        this("", checkNotNull(builder));
    }

    
    private AtomicCounterConsolidator(final String path, final NodeBuilder builder) {
        this.builder = checkNotNull(builder);
        this.path = path;
    }
    
    @Override
    public void propertyAdded(final PropertyState after) throws CommitFailedException {
        LOG.debug("propertyAdded() - {}", after);
        update = shallWeProcessProperty(after, path, builder);
    }

    /**
     * <p>
     * consolidate the {@link #PREFIX_PROP_COUNTER} properties and sum them into the
     * {@link #PROP_COUNTER}
     * </p>
     * 
     * <p>
     * The passed in {@code NodeBuilder} must have
     * {@link org.apache.jackrabbit.JcrConstants#JCR_MIXINTYPES} with
     * {@link org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants#MIX_ATOMIC_COUNTER}.
     * If not it will be silently ignored.
     * </p>
     * 
     * @param builder the builder to work on. Cannot be null.
     */
    public static void consolidateCount(@Nonnull final NodeBuilder builder) {
        LOG.debug("consolidating...");
        long count = builder.hasProperty(PROP_COUNTER)
                        ? builder.getProperty(PROP_COUNTER).getValue(LONG)
                        : 0;

        for (PropertyState p : builder.getProperties()) {
            if (p.getName().startsWith(PREFIX_PROP_COUNTER)) {
                count += p.getValue(LONG);
                builder.removeProperty(p.getName());
            }
        }

        builder.setProperty(PROP_COUNTER, count);
    }
    
    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        if (update) {
            consolidateCount(builder);
        }
    }

    @Override
    public Editor childNodeAdded(final String name, final NodeState after) throws CommitFailedException {
        return new AtomicCounterConsolidator(path + '/' + name, builder.getChildNode(name));
    }

    @Override
    public Editor childNodeChanged(final String name, 
                                   final NodeState before, 
                                   final NodeState after) throws CommitFailedException {
        return new AtomicCounterConsolidator(path + '/' + name, builder.getChildNode(name));
    }
}
