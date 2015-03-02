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

import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * <p>
 * Manages a node as <em>Atomic Counter</em>: a node which will handle at low level a protected
 * property ({@link #PROP_COUNTER}) in an atomic way. This will represent an increment or decrement
 * of a counter in the case, for example, of <em>Likes</em> or <em>Voting</em>.
 * </p>
 * 
 * <p>
 * Whenever you add a {@link NodeTypeConstants#MIX_ATOMIC_COUNTER} mixin to a node it will turn it
 * into an atomic counter. Then in order to increment or decrement the {@code oak:counter} property
 * you'll need to set the {@code oak:increment} one ({@link #PROP_INCREMENT). Please note that the
 * <strong>{@code oak:incremement} will never be saved</strong>, only the {@code oak:counter} will
 * be amended accordingly.
 * </p>
 * 
 * <p>
 *  So in order to deal with the counter from a JCR point of view you'll do something as follows 
 * </p>
 * 
 * <pre>
 *  Session session = ...
 *  
 *  // creating a counter node
 *  Node counter = session.getRootNode().addNode("mycounter");
 *  counter.addMixin("mix:atomicCounter"); // or use the NodeTypeConstants
 *  session.save();
 *  
 *  // Will output 0. the default value
 *  System.out.println("counter now: " + counter.getProperty("oak:counter").getLong());
 *  
 *  // incrementing by 5 the counter
 *  counter.setProperty("oak:increment", 5);
 *  session.save();
 *  
 *  // Will output 5
 *  System.out.println("counter now: " + counter.getProperty("oak:counter").getLong());
 *  
 *  // decreasing by 1
 *  counter.setProperty("oak:increment", -1);
 *  session.save();
 *  
 *  // Will output 4
 *  System.out.println("counter now: " + counter.getProperty("oak:counter").getLong());
 *  
 *  session.logout();
 * </pre>
 */
public class AtomicCounterPreProcess extends AtomicCounterEditor {
    static final Logger LOG = LoggerFactory.getLogger(AtomicCounterPreProcess.class);
    private final NodeBuilder builder;
    private final String path;
    
    public AtomicCounterPreProcess(@Nonnull final NodeBuilder builder) {
        this("", checkNotNull(builder));
    }

    private AtomicCounterPreProcess(final String path, final NodeBuilder builder) {
        this.builder = checkNotNull(builder);
        this.path = path;
    }

    private void setUniqueCounter(final long value) {
        builder.setProperty(PREFIX_PROP_COUNTER + UUID.randomUUID(), value, LONG);
    }
    
    @Override
    public void propertyAdded(final PropertyState after) throws CommitFailedException {
        if (shallWeProcessProperty(after, path, builder)) {
            setUniqueCounter(after.getValue(LONG));
            builder.removeProperty(PROP_INCREMENT);
        }
    }

    @Override
    public Editor childNodeAdded(final String name, final NodeState after) throws CommitFailedException {
        return new AtomicCounterPreProcess(path + '/' + name, builder.getChildNode(name));
    }

    @Override
    public Editor childNodeChanged(final String name, 
                                   final NodeState before, 
                                   final NodeState after) throws CommitFailedException {
        return new AtomicCounterPreProcess(path + '/' + name, builder.getChildNode(name));
    }
}
