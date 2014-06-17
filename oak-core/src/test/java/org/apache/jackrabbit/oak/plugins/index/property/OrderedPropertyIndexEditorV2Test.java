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
package org.apache.jackrabbit.oak.plugins.index.property;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedPropertyIndexEditorV2.SplitRules;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.memory.StringBasedBlob;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class OrderedPropertyIndexEditorV2Test {
    /**
     * mocking class to ease the testing
     */
    private class MockRules extends OrderedPropertyIndexEditorV2.SplitRules {
        private final List<Long> split;
        
        MockRules(@Nonnull final List<Long> split) {
            // initialise a dull non-used super configuration
            super(EmptyNodeState.EMPTY_NODE.builder());
            this.split = split;
        }

        @Override
        public List<Long> getSplit() {
            return split;
        }
    }

    /**
     * checks the correct settings of the {@code propertyNames}
     */
    @Test
    public void propertyNamesSettings() {
        final Set<String> expected = Sets.newHashSet("a");
        OrderedPropertyIndexEditorV2 editor;
        NodeBuilder indexDefinition;
        PropertyState propertyNames;
        
        propertyNames = PropertyStates.createProperty(IndexConstants.PROPERTY_NAMES, expected, Type.NAMES);
        indexDefinition = Mockito.mock(NodeBuilder.class);
        Mockito.when(indexDefinition.getProperty(IndexConstants.PROPERTY_NAMES)).thenReturn(propertyNames);
        editor = new OrderedPropertyIndexEditorV2(indexDefinition, null, null);
        assertEquals(expected, editor.getPropertyNames());

        propertyNames = PropertyStates.createProperty(IndexConstants.PROPERTY_NAMES,
            Iterables.concat(expected, ImmutableList.of("b", "c")), Type.NAMES);
        indexDefinition = Mockito.mock(NodeBuilder.class);
        Mockito.when(indexDefinition.getProperty(IndexConstants.PROPERTY_NAMES)).thenReturn(propertyNames);
        editor = new OrderedPropertyIndexEditorV2(indexDefinition, null, null);
        assertEquals(expected, editor.getPropertyNames());
    }
    
    /**
     * cover whether the current property has to be processed or not
     */
    @Test
    public void isToProcess() {
        final String indexed = "a";
        NodeBuilder indexDefinition;
        OrderedPropertyIndexEditorV2 editor;
        PropertyState property;

        indexDefinition = EmptyNodeState.EMPTY_NODE.builder();
        indexDefinition.setProperty(IndexConstants.PROPERTY_NAMES, Sets.newHashSet(indexed),
            Type.NAMES);
        editor = new OrderedPropertyIndexEditorV2(indexDefinition, null, null);

        property = PropertyStates.createProperty(indexed, "justavalue", Type.STRING);
        assertTrue(editor.isToProcess(property));
        property = PropertyStates.createProperty("foobar", "justavalue", Type.STRING);
        assertFalse(editor.isToProcess(property));
        property = PropertyStates.createProperty(indexed, Collections.EMPTY_LIST, Type.STRINGS);
        assertFalse(editor.isToProcess(property));
        property = PropertyStates.createProperty(indexed, new StringBasedBlob("justavalue"),
            Type.BINARY);
        assertFalse(editor.isToProcess(property));
    }
    
    /**
     * test the encoding of a property value
     * @throws UnsupportedEncodingException 
     */
    @Test
    public void encode() throws UnsupportedEncodingException {
        PropertyValue pv;
        SplitRules rules;

        pv = null;
        rules = null;
        assertNull(OrderedPropertyIndexEditorV2.encode(pv, rules));

        pv = PropertyValues.newString("foobar");
        rules = new MockRules(ImmutableList.of(3L, 3L));
        assertEquals(ImmutableSet.of("foo,bar"), OrderedPropertyIndexEditorV2.encode(pv, rules));

        pv = PropertyValues.newString("foobar bazbaz");
        rules = new MockRules(ImmutableList.of(3L, 3L, 4L, 3L));
        assertEquals(ImmutableSet.of("foo,bar,+baz,baz"),
            OrderedPropertyIndexEditorV2.encode(pv, rules));

        pv = PropertyValues.newString("foobar*bazbaz");
        rules = new MockRules(ImmutableList.of(3L, 3L, 4L, 3L));
        assertEquals(
            ImmutableSet.of("foo,bar,%2Abaz,baz"), OrderedPropertyIndexEditorV2.encode(pv, rules));

        pv = PropertyValues.newDate("2014-06-06T10:58:39.165Z");
        rules = new MockRules(ImmutableList.of(4L, 6L, 6L, 8L));
        assertEquals(ImmutableSet.of("2014,-06-06,T10%3A58,%3A39.165Z"),
            OrderedPropertyIndexEditorV2.encode(pv, rules));
    }
    
    @Test
    public void tokenise() {
        OrderedPropertyIndexEditorV2.SplitRules rules;

        rules = new MockRules(ImmutableList.of(3L));
        assertEquals(ImmutableList.of("a::"),
            OrderedPropertyIndexEditorV2.tokeniseAndEncode("a", rules));
        assertEquals(ImmutableList.of("ap:"),
            OrderedPropertyIndexEditorV2.tokeniseAndEncode("ap", rules));
        assertEquals(ImmutableList.of("app"),
            OrderedPropertyIndexEditorV2.tokeniseAndEncode("app", rules));
        assertEquals(ImmutableList.of("app"),
            OrderedPropertyIndexEditorV2.tokeniseAndEncode("apple", rules));
        assertEquals(ImmutableList.of("app"),
            OrderedPropertyIndexEditorV2.tokeniseAndEncode("apples", rules));
        assertEquals(ImmutableList.of("app"),
            OrderedPropertyIndexEditorV2.tokeniseAndEncode("applets", rules));
        assertEquals(ImmutableList.of("app"),
            OrderedPropertyIndexEditorV2.tokeniseAndEncode("apps", rules));

        rules = new MockRules(ImmutableList.of(3L, 2L));
        assertEquals(
            ImmutableList.of("a" + OrderedIndex.FILLER + OrderedIndex.FILLER, OrderedIndex.FILLER),
            OrderedPropertyIndexEditorV2.tokeniseAndEncode("a", rules));
        assertEquals(ImmutableList.of("ap" + OrderedIndex.FILLER, OrderedIndex.FILLER),
            OrderedPropertyIndexEditorV2.tokeniseAndEncode("ap", rules));
        assertEquals(ImmutableList.of("app", OrderedIndex.FILLER),
            OrderedPropertyIndexEditorV2.tokeniseAndEncode("app", rules));
        assertEquals(ImmutableList.of("app", "le"),
            OrderedPropertyIndexEditorV2.tokeniseAndEncode("apple", rules));
        assertEquals(ImmutableList.of("app", "le"),
            OrderedPropertyIndexEditorV2.tokeniseAndEncode("apples", rules));
        assertEquals(ImmutableList.of("app", "le"),
            OrderedPropertyIndexEditorV2.tokeniseAndEncode("applets", rules));
        assertEquals(ImmutableList.of("app", "s" + OrderedIndex.FILLER),
            OrderedPropertyIndexEditorV2.tokeniseAndEncode("apps", rules));

        rules = new MockRules(ImmutableList.of(3L, 2L, 2L));
        assertEquals(ImmutableList.of("a" + OrderedIndex.FILLER + OrderedIndex.FILLER,
            OrderedIndex.FILLER, OrderedIndex.FILLER),
            OrderedPropertyIndexEditorV2.tokeniseAndEncode("a", rules));
        assertEquals(
            ImmutableList.of("ap" + OrderedIndex.FILLER, OrderedIndex.FILLER, OrderedIndex.FILLER),
            OrderedPropertyIndexEditorV2.tokeniseAndEncode("ap", rules));
        assertEquals(ImmutableList.of("app", OrderedIndex.FILLER, OrderedIndex.FILLER),
            OrderedPropertyIndexEditorV2.tokeniseAndEncode("app", rules));
        assertEquals(ImmutableList.of("app", "le", OrderedIndex.FILLER),
            OrderedPropertyIndexEditorV2.tokeniseAndEncode("apple", rules));
        assertEquals(ImmutableList.of("app", "le", "s" + OrderedIndex.FILLER),
            OrderedPropertyIndexEditorV2.tokeniseAndEncode("apples", rules));
        assertEquals(ImmutableList.of("app", "le", "ts"),
            OrderedPropertyIndexEditorV2.tokeniseAndEncode("applets", rules));
        assertEquals(ImmutableList.of("app", "s" + OrderedIndex.FILLER, OrderedIndex.FILLER),
            OrderedPropertyIndexEditorV2.tokeniseAndEncode("apps", rules));

        rules = new MockRules(ImmutableList.of(4L, 6L, 6L, 8L));
        assertEquals(ImmutableList.of("2014", "-06-06", "T10%3A58", "%3A39.165Z"),
            OrderedPropertyIndexEditorV2.tokeniseAndEncode("2014-06-06T10:58:39.165Z", rules));
    }
}