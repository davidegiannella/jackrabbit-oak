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
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
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
 * you'll need to set the {@code oak:increment} one ({@link #PROP_INCREMENT}). Please note that the
 * <strong>{@code oak:incremement} will never be saved</strong>, only the {@code oak:counter} will
 * be amended accordingly.
 * </p>
 * 
 * <p>
 * So in order to deal with the counter from a JCR point of view you'll do something as follows
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
 * <h3>Internal behavioural details</h3>
 * 
 * <p>
 * The related jira ticket is <a href="https://issues.apache.org/jira/browse/OAK-2472">OAK-2472</a>.
 * In a nutshell when you save an {@code oak:increment} behind the scene it takes its value and
 * increment an internal counter. There will be an individual counter for each cluster node.
 * </p>
 * 
 * <p>
 * Then it will consolidate all the internal counters into a single one: {@code oak:counter}. The
 * consolidation process can happen either synchronously or asynchronously. Refer to
 * {@link #AtomicCounterEditor(NodeBuilder, String, ScheduledExecutorService, NodeStore, Whiteboard)}
 * for details on when it consolidate one way or the other.
 * </p>
 * 
 * <p>
 * <strong>synchronous</strong>. It means the consolidation, sum of all the internal counters, will
 * happen in the same thread. During the lifecycle of the same commit.
 * </p>
 * 
 * <p>
 * <strong>asynchronous</strong>. It means the internal counters will be set during the same commit;
 * but it will eventually schedule a separate thread in which will retry some times to consolidate
 * them.
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
    
    /**
     * prefix used internally for tracking the cluster node related revision numbers
     */
    public static final String PREFIX_PROP_REVISION = ":rev-";
    
    private static final Logger LOG = LoggerFactory.getLogger(AtomicCounterEditor.class);
    private final NodeBuilder builder;
    private final String path;
    private final String instanceId;
    private final ScheduledExecutorService executor;
    private final NodeStore store;
    private final Whiteboard board;
    
    /**
     * the current counter property name
     */
    private final String counterName;
    
    /**
     * the current revision property name
     */
    private final String revisionName;
    
    /**
     * instruct whether to update the node on leave.
     */
    private boolean update;
    
    /**
     * <p>
     * Create an instance of the editor for atomic increments. It can works synchronously as well as
     * asynchronously. See class javadoc for details around it.
     * </p>
     * <p>
     * If {@code instanceId} OR {@code executor} OR {@code store} OR {@code board} are null, the
     * editor will switch to synchronous behaviour for consolidation. If no {@link CommitHook} will
     * be found in the whiteboard, a {@link EmptyHook} will be provided to the {@link NodeStore} for
     * merging.
     * </p>
     * 
     * @param builder the build on which to work. Cannot be null.
     * @param instanceId the current Oak instance Id. If null editor will be synchronous.
     * @param executor the current Oak executor service. If null editor will be synchronous.
     * @param store the current Oak node store. If null the editor will be synchronous.
     * @param board the current Oak {@link Whiteboard}.
     */
    public AtomicCounterEditor(@Nonnull final NodeBuilder builder, 
                               @Nullable String instanceId,
                               @Nullable ScheduledExecutorService executor,
                               @Nullable NodeStore store,
                               @Nullable Whiteboard board) {
        this("", checkNotNull(builder), instanceId, executor, store, board);
    }

    private AtomicCounterEditor(final String path, 
                                final NodeBuilder builder, 
                                @Nullable String instanceId, 
                                @Nullable ScheduledExecutorService executor,
                                @Nullable NodeStore store,
                                @Nullable Whiteboard board) {
        this.builder = checkNotNull(builder);
        this.path = path;
        this.instanceId = Strings.isNullOrEmpty(instanceId) ? null : instanceId;
        this.executor = executor;
        this.store = store;
        this.board = board;
        
        counterName = instanceId == null ? PREFIX_PROP_COUNTER : 
            PREFIX_PROP_COUNTER + instanceId;
        revisionName = instanceId == null ? PREFIX_PROP_REVISION :
            PREFIX_PROP_REVISION + instanceId;

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
        
        PropertyState counter = builder.getProperty(counterName);
        PropertyState revision = builder.getProperty(revisionName);
        
        long currentValue = 0;
        if (counter != null) {
            currentValue = counter.getValue(LONG).longValue();
        }
        
        long currentRevision = 0;
        if (revision != null) {
            currentRevision = revision.getValue(LONG).longValue();
        }
        
        currentValue += value;
        currentRevision += 1;

        builder.setProperty(counterName, currentValue, LONG);
        builder.setProperty(revisionName, currentRevision, LONG);
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
            executor, store, board);
    }

    @Override
    public Editor childNodeChanged(final String name, 
                                   final NodeState before, 
                                   final NodeState after) throws CommitFailedException {
        return new AtomicCounterEditor(path + '/' + name, builder.getChildNode(name), instanceId,
            executor, store, board);
    }

    @Override
    public void leave(final NodeState before, final NodeState after) throws CommitFailedException {
        if (update) {
            if (instanceId == null || store == null || executor == null || board == null) {
                LOG.trace(
                    "Executing synchronously. instanceId: {}, store: {}, executor: {}, board: {}",
                    new Object[] { instanceId, store, executor, board });
                consolidateCount(builder);
            } else {
                CommitHook hook = WhiteboardUtils.getService(board, CommitHook.class);
                if (hook == null) {
                    LOG.trace("CommitHook not found in the Whiteboard. Defaulting to EmptyHook");
                    hook = new EmptyHook();
                }
                
                LOG.trace("Scheduling process");
                executor.schedule(
                    new ConsolidatorTask(path, builder.getProperty(revisionName), store, executor), 
                    0, TimeUnit.SECONDS);
            }
        }
    }
    
    private static class ConsolidatorTask implements Callable<Void> {
        private final String p;
        private final PropertyState rev;
        private final NodeStore s;
        private final ScheduledExecutorService exec;
        
        public ConsolidatorTask(@Nonnull String path, 
                                @Nullable PropertyState revision, 
                                @Nonnull NodeStore store,
                                @Nonnull ScheduledExecutorService exec) {
            p = checkNotNull(path);
            rev = revision;
            s = checkNotNull(store);
            this.exec = checkNotNull(exec);
        }
        
        @Override
        public Void call() throws Exception {            
            try {
                LOG.trace("Async consolidation running: path: {}, revision: {}, store: {}", p, rev, s);
                NodeBuilder b = builderFromPath(s.getRoot().builder(), p);
                
                if (!b.exists()) {
                    LOG.debug("The builder from ");
                }
                
            } catch (Exception e) {
                LOG.debug("caught Exception. Re-scheduling. {}", e.getMessage());
                if (LOG.isTraceEnabled()) {
                    LOG.trace("{}", e);
                }
            }
            
            return null;
        }
    }
    
    /**
     * checks that the revision provided in the PropertyState is less or equal than the one within
     * the builder.
     * 
     * if {@code revision} is null it will always be {@code true}.
     * 
     * If {@code builder} does not contain the property it will always return true.
     * 
     * @param builder
     * @param revision
     * @return
     */
    static boolean checkRevision(@Nonnull NodeBuilder builder, @Nullable PropertyState revision) {
        if (revision == null) {
            return true;
        }
        String pName = revision.getName();
        PropertyState builderRev = builder.getProperty(pName);
        if (builderRev == null) {
            return true;
        }
        
        long brValue = builderRev.getValue(Type.LONG).longValue();
        long rValue = revision.getValue(Type.LONG).longValue();
        
        if (brValue >= rValue) {
            return true;
        }
        return false;
    }
    
    private static NodeBuilder builderFromPath(@Nonnull NodeBuilder ancestor, @Nonnull String path) {
        NodeBuilder b = checkNotNull(ancestor);
        for (String name : PathUtils.elements(checkNotNull(path))) {
            b = b.getChildNode(name);
        }
        return b;
    }
}
