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
import static com.google.common.collect.ImmutableList.of;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PREFIX_PROP_COUNTER;
import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PREFIX_PROP_REVISION;
import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PROP_COUNTER;
import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PROP_INCREMENT;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.MIX_ATOMIC_COUNTER;
import static org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.NodeStoreFixture;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.sqs.model.UnsupportedOperationException;
import com.google.common.collect.ImmutableSet;

public class AtomicCounterEditorTest {
    private static final EditorHook HOOK_NO_CLUSTER = new EditorHook(
        new AtomicCounterEditorProvider(null, null, null));
    private static final EditorHook HOOK_1_SYNC = new EditorHook(
        new AtomicCounterEditorProvider("1", null, null));
    private static final EditorHook HOOK_2_SYNC = new EditorHook(
        new AtomicCounterEditorProvider("2", null, null));

    private static final PropertyState INCREMENT_BY_1 = PropertyStates.createProperty(
        PROP_INCREMENT, 1L);
    private static final PropertyState INCREMENT_BY_2 = PropertyStates.createProperty(
        PROP_INCREMENT, 2L);
    
    @Test
    public void increment() throws CommitFailedException {
        NodeBuilder builder;
        Editor editor;
        
        builder = EMPTY_NODE.builder();
        editor = new AtomicCounterEditor(builder, null, null, null);
        editor.propertyAdded(INCREMENT_BY_1);
        assertNoCounters(builder.getProperties());
        
        builder = EMPTY_NODE.builder();
        builder = setMixin(builder);
        editor = new AtomicCounterEditor(builder, null, null, null);
        editor.propertyAdded(INCREMENT_BY_1);
        assertNull("the oak:increment should never be set", builder.getProperty(PROP_INCREMENT));
        assertTotalCountersValue(builder.getProperties(), 1);
    }
    
    @Test
    public void consolidate() throws CommitFailedException {
        NodeBuilder builder;
        Editor editor;
        
        builder = EMPTY_NODE.builder();
        builder = setMixin(builder);
        editor = new AtomicCounterEditor(builder, null, null, null);
        
        editor.propertyAdded(INCREMENT_BY_1);
        assertTotalCountersValue(builder.getProperties(), 1);
        editor.propertyAdded(INCREMENT_BY_1);
        assertTotalCountersValue(builder.getProperties(), 2);
        AtomicCounterEditor.consolidateCount(builder);
        assertCounterNodeState(builder, ImmutableSet.of(PREFIX_PROP_COUNTER), 2);
    }

    /**
     * that a list of properties does not contains any property with name starting with
     * {@link AtomicCounterEditor#PREFIX_PROP_COUNTER}
     * 
     * @param properties
     */
    private static void assertNoCounters(@Nonnull final Iterable<? extends PropertyState> properties) {
        checkNotNull(properties);
        
        for (PropertyState p : properties) {
            assertFalse("there should be no counter property",
                p.getName().startsWith(PREFIX_PROP_COUNTER));
        }
    }
    
    /**
     * assert the total amount of {@link AtomicCounterEditor#PREFIX_PROP_COUNTER}
     * 
     * @param properties
     */
    private static void assertTotalCountersValue(@Nonnull final Iterable<? extends PropertyState> properties,
                                            int expected) {
        int total = 0;
        for (PropertyState p : checkNotNull(properties)) {
            if (p.getName().startsWith(PREFIX_PROP_COUNTER)) {
                total += p.getValue(LONG);
            }
        }
        
        assertEquals("the total amount of :oak-counter properties does not match", expected, total);
    }
    
    private static NodeBuilder setMixin(@Nonnull final NodeBuilder builder) {
        return checkNotNull(builder).setProperty(JCR_MIXINTYPES, of(MIX_ATOMIC_COUNTER), NAMES);
    }
    
    
    private static void assertCounterNodeState(@Nonnull NodeBuilder builder, 
                                               @Nonnull Set<String> hiddenProps, 
                                               long expectedCounter) {
        checkNotNull(builder);
        int totalHiddenProps = 0;
        long totalHiddenValue = 0;
        PropertyState counter = builder.getProperty(PROP_COUNTER);

        assertNotNull("counter property cannot be null", counter);
        assertNull("The increment property should not be there",
            builder.getProperty(PROP_INCREMENT));
        for (PropertyState p : builder.getProperties()) {
            String name = p.getName();
            if (name.startsWith(PREFIX_PROP_COUNTER)) {
                totalHiddenProps++;
                totalHiddenValue += p.getValue(LONG).longValue();
                assertTrue("unexpected property found: " + name,
                    checkNotNull(hiddenProps.contains(name)));
            }
        }
        assertEquals("The amount of hidden properties does not match", hiddenProps.size(),
            totalHiddenProps);
        assertEquals("The sum of the hidden properties does not match the counter", counter
            .getValue(LONG).longValue(), totalHiddenValue);
        assertEquals("The counter does not match the expected value", expectedCounter, counter
            .getValue(LONG).longValue());
    }

    private static NodeBuilder incrementBy(@Nonnull NodeBuilder builder, @Nonnull PropertyState increment) {
        return checkNotNull(builder).setProperty(checkNotNull(increment));
    }
    
    @Test
    public void notCluster() throws CommitFailedException {
        NodeBuilder builder;
        NodeState before, after;
        
        builder = EMPTY_NODE.builder();
        before = builder.getNodeState();
        builder = setMixin(builder);
        builder = incrementBy(builder, INCREMENT_BY_1);
        after = builder.getNodeState();
        builder = HOOK_NO_CLUSTER.processCommit(before, after, EMPTY).builder();
        assertCounterNodeState(builder, ImmutableSet.of(PREFIX_PROP_COUNTER), 1);

        before = builder.getNodeState();
        builder = incrementBy(builder, INCREMENT_BY_2);
        after = builder.getNodeState(); 
        builder = HOOK_NO_CLUSTER.processCommit(before, after, EMPTY).builder();
        assertCounterNodeState(builder, ImmutableSet.of(PREFIX_PROP_COUNTER), 3);
    }
    
    /**
     * simulates the update from multiple oak instances
     * @throws CommitFailedException 
     */
    @Test
    public void multipleNodeUpdates() throws CommitFailedException {
        NodeBuilder builder;
        NodeState before, after;
        
        builder = EMPTY_NODE.builder();
        before = builder.getNodeState(); 
        builder = setMixin(builder);
        builder = incrementBy(builder, INCREMENT_BY_1);
        after = builder.getNodeState();
        builder = HOOK_1_SYNC.processCommit(before, after, EMPTY).builder();
        assertCounterNodeState(builder, ImmutableSet.of(PREFIX_PROP_COUNTER + "1"), 1);
        
        before = builder.getNodeState();
        builder = incrementBy(builder, INCREMENT_BY_1);
        after = builder.getNodeState();
        builder = HOOK_2_SYNC.processCommit(before, after, EMPTY).builder();
        assertCounterNodeState(builder,
            ImmutableSet.of(PREFIX_PROP_COUNTER + "1", PREFIX_PROP_COUNTER + "2"), 2);
    }
    
    /**
     * covers the revision increments aspect
     * @throws CommitFailedException 
     */
    @Test
    public void revisionIncrements() throws CommitFailedException {
        NodeBuilder builder;
        NodeState before, after;
        PropertyState rev;
        
        builder = EMPTY_NODE.builder();
        before = builder.getNodeState();
        builder = setMixin(builder);
        builder = incrementBy(builder, INCREMENT_BY_1);
        after = builder.getNodeState();
        builder = HOOK_1_SYNC.processCommit(before, after, EMPTY).builder();
        rev = builder.getProperty(PREFIX_PROP_REVISION + "1");
        assertNotNull(rev);
        assertEquals(1, rev.getValue(LONG).longValue());

        before = builder.getNodeState();
        builder = incrementBy(builder, INCREMENT_BY_2);
        after = builder.getNodeState();
        builder = HOOK_1_SYNC.processCommit(before, after, EMPTY).builder();
        rev = builder.getProperty(PREFIX_PROP_REVISION + "1");
        assertNotNull(rev);
        assertEquals(2, rev.getValue(LONG).longValue());

        before = builder.getNodeState();
        builder = incrementBy(builder, INCREMENT_BY_1);
        after = builder.getNodeState();
        builder = HOOK_2_SYNC.processCommit(before, after, EMPTY).builder();
        rev = builder.getProperty(PREFIX_PROP_REVISION + "1");
        assertNotNull(rev);
        assertEquals(2, rev.getValue(LONG).longValue());
        rev = builder.getProperty(PREFIX_PROP_REVISION + "2");
        assertNotNull(rev);
        assertEquals(1, rev.getValue(LONG).longValue());
    }
    
    @Test
    public void singleNodeAsync() throws CommitFailedException, InterruptedException, ExecutionException {
        NodeStore store = NodeStoreFixture.MEMORY_NS.createNodeStore();
        String instanceId1 = "1";
        MyExecutor exec1 = new MyExecutor();
        EditorHook hook1 = new EditorHook(new AtomicCounterEditorProvider(instanceId1, exec1, store));
        NodeBuilder builder;
        NodeState before, after;
        PropertyState p;
        
        builder = store.getRoot().builder();
        builder = builder.child("c");
        builder = setMixin(builder);
        before = builder.getNodeState();
        builder = incrementBy(builder, INCREMENT_BY_1);
        after = builder.getNodeState();
        builder = hook1.processCommit(before, after, EMPTY).builder();
        
        // as we're providing all the information we expect the counter not to be consolidated for
        // as long as the scheduled process has run
        p = builder.getProperty(PROP_COUNTER);
        assertNull("First run before consolidation we expect oak:counter to be null", p);
        exec1.execute();
        // TODO assertCounterNodeState(builder, hiddenProps, expectedCounter);
        // TODO to be completed by retrieving the store status. What shall I do? store.merge?
        fail("to be completed");
    }
    
    /**
     * a fake {@link ScheduledExecutorService} which does not schedule and wait for a call on
     * {@link #execute()} to execute the first scheduled task. It works in a FIFO manner.
     */
    private static class MyExecutor extends AbstractExecutorService implements ScheduledExecutorService {
        private static final Logger LOG = LoggerFactory.getLogger(MyExecutor.class);

        /** Base of nanosecond timings, to avoid wrapping */
        private static final long NANO_ORIGIN = System.nanoTime();
        
        @SuppressWarnings("rawtypes")
        private Queue<ScheduledFuture> fifo = new LinkedList<ScheduledFuture>();
        
        @Override
        public void shutdown() {
        }

        @Override
        public List<Runnable> shutdownNow() {
            return null;
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return false;
        }

        @Override
        public void execute(Runnable command) {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException("Not implemented");
        }

        /**
         * Returns nanosecond time offset by origin
         */
        final long now() {
            return System.nanoTime() - NANO_ORIGIN;
        }

        private synchronized void addToQueue(@Nonnull ScheduledFuture future) {
            fifo.add(future);
        }
        
        private synchronized ScheduledFuture getFromQueue() {
            if (fifo.isEmpty()) {
                return null;
            } else {
                return fifo.remove();
            }
        }
        
        @Override
        public <V> ScheduledFuture<V> schedule(final Callable<V> callable, long delay, TimeUnit unit) {
            LOG.debug("Scheduling with delay: {} and unit: {} the process {}", delay, unit, callable);
            
            checkNotNull(callable);
            checkNotNull(unit);
            if (delay < 0) {
                delay = 0;
            }
            
            long triggerTime = now() + unit.toNanos(delay); 
            
            ScheduledFuture<V> future = new ScheduledFuture<V>() {
                final Callable<V> c = callable;
                
                @Override
                public long getDelay(TimeUnit unit) {
                    throw new UnsupportedOperationException("Not implemented");
                }

                @Override
                public int compareTo(Delayed o) {
                    throw new UnsupportedOperationException("Not implemented");
                }

                @Override
                public boolean cancel(boolean mayInterruptIfRunning) {
                    throw new UnsupportedOperationException("Not implemented");
                }

                @Override
                public boolean isCancelled() {
                    throw new UnsupportedOperationException("Not implemented");
                }

                @Override
                public boolean isDone() {
                    throw new UnsupportedOperationException("Not implemented");
                }

                @Override
                public V get() throws InterruptedException, ExecutionException {
                    try {
                        return c.call();
                    } catch (Exception e) {
                        throw new ExecutionException(e);
                    }
                }

                @Override
                public V get(long timeout, TimeUnit unit) throws InterruptedException,
                                                         ExecutionException, TimeoutException {
                    throw new UnsupportedOperationException("Not implemented");
                }
            };
            
            addToQueue(future);
            return future;
        }

        /**
         * executes the first item scheduled in the queue. If the queue is empty it will silently
         * return {@code null} which can easily be the same returned from the scheduled process.
         * 
         * @return the result of the {@link ScheduledFuture} or {@code null} if the queue is empty.
         * @throws InterruptedException
         * @throws ExecutionException
         */
        @CheckForNull
        public Object execute() throws InterruptedException, ExecutionException {
            ScheduledFuture<?> f = getFromQueue();
            if (f == null) {
                return null;
            } else {
                return f.get();
            }
        }
        
        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay,
                                                      long period, TimeUnit unit) {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay,
                                                         long delay, TimeUnit unit) {
            throw new UnsupportedOperationException("Not implemented");
        }
    }
}
