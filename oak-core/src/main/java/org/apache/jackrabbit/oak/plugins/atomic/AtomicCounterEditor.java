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
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.MIX_ATOMIC_COUNTER;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Iterators;

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
 * 
 * <p>
 * TODO add details on how it works with consolidation and asynchronous lazy thread.
 * </p>
 */
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
    private final String path;
    private final String instanceId;
    private final ScheduledExecutorService executor;
    private final NodeStore store;
    
    /**
     * instruct whether to update the node on leave.
     */
    private boolean update;
    
    /**
     * If {@code instanceId} or {@code executor} are null, the editor will switch to a "Synchronous"
     * way of consolidating the counter. Which means it will update it straight away rather than
     * lazily do it in a separate thread.
     * 
     * @param builder the build on which to work. Cannot be null.
     * @param instanceId the current Oak instance Id.
     * @param executor the current Oak executor service.
     */
    public AtomicCounterEditor(@Nonnull final NodeBuilder builder, @Nullable String instanceId,
                               @Nullable ScheduledExecutorService executor) {
        this("", checkNotNull(builder), instanceId, executor);
    }

    private AtomicCounterEditor(final String path, final NodeBuilder builder, 
                                @Nullable String instanceId, 
                                @Nullable ScheduledExecutorService executor) {
        this.builder = checkNotNull(builder);
        this.path = path;
        this.instanceId = Strings.isNullOrEmpty(instanceId) ? null : instanceId;
        this.executor = executor;
        this.store = null;
    }

    private static boolean shallWeProcessProperty(final PropertyState property,
                                                  final String path,
                                                  final NodeBuilder builder) {
        boolean process = false;
        PropertyState mixin = checkNotNull(builder).getProperty(JCR_MIXINTYPES);
        if (mixin != null && PROP_INCREMENT.equals(property.getName()) &&
                Iterators.contains(mixin.getValue(NAMES).iterator(), MIX_ATOMIC_COUNTER)) {
            if (LONG.equals(property.getType())) {
                process = true;
            } else {
                LOG.warn(
                    "although the {} property is set is not of the right value: LONG. Not processing node: {}.",
                    PROP_INCREMENT, path);
            }
        }
        return process;
    }
    
    /**
     * <p>
     * consolidate the {@link #PREFIX_PROP_COUNTER} properties and sum them into the
     * {@link #PROP_COUNTER}
     * </p>
     * 
     * <p>
     * The passed in {@code NodeBuilder} must have
     * {@link org.apache.jackrabbit.JcrConstants#JCR_MIXINTYPES JCR_MIXINTYPES} with
     * {@link org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants#MIX_ATOMIC_COUNTER MIX_ATOMIC_COUNTER}.
     * If not it will be silently ignored.
     * </p>
     * 
     * @param builder the builder to work on. Cannot be null.
     */
    public static void consolidateCount(@Nonnull final NodeBuilder builder) {
        long count = 0;
        for (PropertyState p : builder.getProperties()) {
            if (p.getName().startsWith(PREFIX_PROP_COUNTER)) {
                count += p.getValue(LONG);
            }
        }

        builder.setProperty(PROP_COUNTER, count);
    }

    private void setUniqueCounter(final long value) {
        update = true;
        String propName = instanceId == null ? PREFIX_PROP_COUNTER : 
                                                PREFIX_PROP_COUNTER + instanceId;
        PropertyState p = builder.getProperty(propName);
        long currentValue = 0;
        if (p != null) {
            currentValue = p.getValue(LONG).longValue();
        }
        currentValue += value;
        builder.setProperty(propName, currentValue, LONG);
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
        return new AtomicCounterEditor(path + '/' + name, builder.getChildNode(name), instanceId,
            executor);
    }

    @Override
    public Editor childNodeChanged(final String name, 
                                   final NodeState before, 
                                   final NodeState after) throws CommitFailedException {
        return new AtomicCounterEditor(path + '/' + name, builder.getChildNode(name), instanceId,
            executor);
    }

    @Override
    public void leave(final NodeState before, final NodeState after) throws CommitFailedException {
        if (update) {
            // TODO here is where the Async check could be done
            consolidateCount(builder);
            
            executor.schedule(new Callable<Void>() {
                
                @Override
                public Void call() throws Exception {
                    try {
                        consolidateCount(builder);
                    } catch (Exception e) {
                        LOG.debug("sdfsdsdfsdf");
                        executor.schedule(this, 1, TimeUnit.SECONDS);
                    }
                    
                    return null;
                }
                
            }, 0, TimeUnit.SECONDS);
        }
    }
}
