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
import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PROP_COUNTER;
import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PROP_INCREMENT;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.MIX_ATOMIC_COUNTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class AtomicCounterEditorTest {
    
    @Test
    public void increment() throws CommitFailedException {
        NodeBuilder builder;
        Editor editor;
        PropertyState property;
        
        builder = EMPTY_NODE.builder();
        editor = new AtomicCounterEditor(builder, "0", null);
        property = PropertyStates.createProperty(PROP_INCREMENT, 1L, Type.LONG);
        editor.propertyAdded(property);
        assertNoCounters(builder.getProperties());
        
        builder = EMPTY_NODE.builder();
        builder = setMixin(builder);
        editor = new AtomicCounterEditor(builder, "0", null);
        property = PropertyStates.createProperty(PROP_INCREMENT, 1L, Type.LONG);
        editor.propertyAdded(property);
        assertNull("the oak:increment should never be set", builder.getProperty(PROP_INCREMENT));
        assertTotalCounters(builder.getProperties(), 1);
    }
    
    @Test
    public void consolidate() throws CommitFailedException {
        NodeBuilder builder;
        Editor editor;
        PropertyState property;
        
        builder = EMPTY_NODE.builder();
        builder = setMixin(builder);
        editor = new AtomicCounterEditor(builder, "0", null);
        property = PropertyStates.createProperty(PROP_INCREMENT, 1L, Type.LONG);
        
        editor.propertyAdded(property);
        assertTotalCounters(builder.getProperties(), 1);
        editor.propertyAdded(property);
        assertTotalCounters(builder.getProperties(), 2);
        AtomicCounterEditor.consolidateCount(builder);
        assertNotNull(builder.getProperty(PROP_COUNTER));
        assertEquals(2, builder.getProperty(PROP_COUNTER).getValue(LONG).longValue());
        assertCounterNodeState(builder, ImmutableSet.of(PREFIX_PROP_COUNTER + "0"));
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
    private static void assertTotalCounters(@Nonnull final Iterable<? extends PropertyState> properties,
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
    
    
    private static void assertCounterNodeState(@Nonnull NodeBuilder builder, @Nonnull Set<String> hiddenProps) {
        int totalHiddenProps = 0;
        for (PropertyState p : checkNotNull(builder).getProperties()) {
            String name = p.getName();
            if (name.startsWith(PREFIX_PROP_COUNTER)) {
                totalHiddenProps++;
                assertTrue("unexpected property found: " + name,
                    checkNotNull(hiddenProps.contains(name)));
            }
        }
        assertEquals("The amount of hidden properties does not match", hiddenProps.size(),
            totalHiddenProps);
        
        fail("finish the implemention by checking the total value of counter and the total value of the hidden properties");
    }
    
    /**
     * simulates the update from multiple oak instances
     * @throws CommitFailedException 
     */
    @Test
    public void multipleNodeUpdates() throws CommitFailedException {
        final PropertyState incrementBy1 = PropertyStates.createProperty(PROP_INCREMENT, 1L);
        final PropertyState incrementBy2 = PropertyStates.createProperty(PROP_INCREMENT, 2L);
        NodeBuilder builder;
        Editor e1, e2;
        
        builder = setMixin(EMPTY_NODE.builder());
        e1 = new AtomicCounterEditor(builder, "1", null);
        e2 = new AtomicCounterEditor(builder, "2", null);
        
        fail("instead of calling propertyAdded we should call the whole CommitHook");
        
        e1.propertyAdded(incrementBy1);
        assertCounterNodeState(builder,
            ImmutableSet.of(PREFIX_PROP_COUNTER + "1"));
    }
}
