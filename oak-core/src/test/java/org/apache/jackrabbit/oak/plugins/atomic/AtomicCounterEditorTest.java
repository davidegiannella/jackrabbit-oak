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

import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class AtomicCounterEditorTest {
    private static final EditorHook HOOK_NO_CLUSTER = new EditorHook(
        new AtomicCounterEditorProvider(null, null));
    private static final EditorHook HOOK_1_SYNC = new EditorHook(
        new AtomicCounterEditorProvider("1", null));
    private static final EditorHook HOOK_2_SYNC = new EditorHook(
        new AtomicCounterEditorProvider("2", null));

    private static final PropertyState INCREMENT_BY_1 = PropertyStates.createProperty(
        PROP_INCREMENT, 1L);
    private static final PropertyState INCREMENT_BY_2 = PropertyStates.createProperty(
        PROP_INCREMENT, 2L);
    
    @Test
    public void increment() throws CommitFailedException {
        NodeBuilder builder;
        Editor editor;
        
        builder = EMPTY_NODE.builder();
        editor = new AtomicCounterEditor(builder, null, null);
        editor.propertyAdded(INCREMENT_BY_1);
        assertNoCounters(builder.getProperties());
        
        builder = EMPTY_NODE.builder();
        builder = setMixin(builder);
        editor = new AtomicCounterEditor(builder, null, null);
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
        editor = new AtomicCounterEditor(builder, null, null);
        
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
}
