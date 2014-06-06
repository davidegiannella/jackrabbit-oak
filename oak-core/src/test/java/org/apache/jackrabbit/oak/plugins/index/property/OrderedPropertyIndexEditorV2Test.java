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
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.memory.StringBasedBlob;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import static org.apache.jackrabbit.oak.plugins.index.property.OrderedPropertyIndexEditorV2.FILLER;

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
        PropertyState propertyNames = PropertyStates.createProperty(IndexConstants.PROPERTY_NAMES,
            Sets.newHashSet(indexed), Type.NAMES);
        NodeBuilder indexDefinition = Mockito.mock(NodeBuilder.class);
        Mockito.when(indexDefinition.getProperty(IndexConstants.PROPERTY_NAMES)).thenReturn(
            propertyNames);
        OrderedPropertyIndexEditorV2 editor = new OrderedPropertyIndexEditorV2(indexDefinition,
            null, null);

        assertTrue(editor.isToProcess(PropertyStates.createProperty(indexed, "justavalue",
            Type.STRING)));
        assertFalse(editor.isToProcess(PropertyStates.createProperty("foobar", "justavalue",
            Type.STRING)));
        assertFalse(editor.isToProcess(PropertyStates.createProperty(indexed,
            Collections.EMPTY_LIST, Type.STRINGS)));
        assertFalse(editor.isToProcess(PropertyStates.createProperty(indexed, new StringBasedBlob(
            "justavalue"), Type.BINARY)));
    }
    
    /**
     * test the encoding of a property value
     * @throws UnsupportedEncodingException 
     */
    @Test
    public void encode() throws UnsupportedEncodingException {
        PropertyValue pv;
        
        pv = null;
        assertNull(OrderedPropertyIndexEditorV2.encode(pv));
        
        pv = PropertyValues.newString("foobar");
        assertEquals(ImmutableSet.of("foobar"), OrderedPropertyIndexEditorV2.encode(pv));

        pv = PropertyValues.newString("foobar bazbaz");
        assertEquals(ImmutableSet.of(URLEncoder.encode("foobar bazbaz", Charsets.UTF_8.name())), OrderedPropertyIndexEditorV2.encode(pv));

        pv = PropertyValues.newString("foobar*bazbaz");
        assertEquals(ImmutableSet.of(URLEncoder.encode("foobar*bazbaz", Charsets.UTF_8.name()).replaceAll("\\*", "%2A")), OrderedPropertyIndexEditorV2.encode(pv));

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        Calendar c = Calendar.getInstance();
        String d = sdf.format(c.getTime());
        pv = PropertyValues.newDate(d);
        assertEquals(ImmutableSet.of(URLEncoder.encode(d, Charsets.UTF_8.name())), OrderedPropertyIndexEditorV2.encode(pv));
    }
    
    @Test
    public void tokenise() {
        OrderedPropertyIndexEditorV2.SplitRules rules;

        rules = new MockRules(ImmutableList.of(3L));
        assertEquals(ImmutableList.of("a::"), OrderedPropertyIndexEditorV2.tokenise("a", rules));
        assertEquals(ImmutableList.of("ap:"), OrderedPropertyIndexEditorV2.tokenise("ap", rules));
        assertEquals(ImmutableList.of("app"), OrderedPropertyIndexEditorV2.tokenise("app", rules));
        assertEquals(ImmutableList.of("app"), OrderedPropertyIndexEditorV2.tokenise("apple", rules));
        assertEquals(ImmutableList.of("app"), OrderedPropertyIndexEditorV2.tokenise("apples", rules));
        assertEquals(ImmutableList.of("app"), OrderedPropertyIndexEditorV2.tokenise("applets", rules));
        assertEquals(ImmutableList.of("app"), OrderedPropertyIndexEditorV2.tokenise("apps", rules));

        rules = new MockRules(ImmutableList.of(3L, 2L));
        assertEquals(ImmutableList.of("a" + FILLER + FILLER, FILLER),
            OrderedPropertyIndexEditorV2.tokenise("a", rules));
        assertEquals(ImmutableList.of("ap" + FILLER, FILLER), OrderedPropertyIndexEditorV2.tokenise("ap", rules));
        assertEquals(ImmutableList.of("app", FILLER), OrderedPropertyIndexEditorV2.tokenise("app", rules));
        assertEquals(ImmutableList.of("app", "le"), OrderedPropertyIndexEditorV2.tokenise("apple", rules));
        assertEquals(ImmutableList.of("app", "le"), OrderedPropertyIndexEditorV2.tokenise("apples", rules));
        assertEquals(ImmutableList.of("app", "le"), OrderedPropertyIndexEditorV2.tokenise("applets", rules));
        assertEquals(ImmutableList.of("app", "s" + FILLER), OrderedPropertyIndexEditorV2.tokenise("apps", rules));

        rules = new MockRules(ImmutableList.of(3L, 2L, 2L));
        assertEquals(ImmutableList.of("a" + FILLER + FILLER, FILLER, FILLER),
            OrderedPropertyIndexEditorV2.tokenise("a", rules));
        assertEquals(ImmutableList.of("ap" + FILLER, FILLER, FILLER),
            OrderedPropertyIndexEditorV2.tokenise("ap", rules));
        assertEquals(ImmutableList.of("app", FILLER, FILLER), OrderedPropertyIndexEditorV2.tokenise("app", rules));
        assertEquals(ImmutableList.of("app", "le", FILLER), OrderedPropertyIndexEditorV2.tokenise("apple", rules));
        assertEquals(ImmutableList.of("app", "le", "s" + FILLER),
            OrderedPropertyIndexEditorV2.tokenise("apples", rules));
        assertEquals(ImmutableList.of("app", "le", "ts"), OrderedPropertyIndexEditorV2.tokenise("applets", rules));
        assertEquals(ImmutableList.of("app", "s" + FILLER, FILLER),
            OrderedPropertyIndexEditorV2.tokenise("apps", rules));
        
//        rules = new MockRules(ImmutableList.of(4L, 6L));
//        assertEquals(ImmutableList.of("2013", "-02-01"), SplitStrategy.tokenise(
//            OrderedPropertyIndexEditorV2.encode(
//                PropertyValues.newString("2013-02-01T12:13:17.580Z")).toArray(new String[0])[0],
//            rules));
//        assertEquals(ImmutableList.of("2013", "-03-01"), SplitStrategy.tokenise(
//            OrderedPropertyIndexEditorV2.encode(
//                PropertyValues.newString("2013-03-01T12:13:17.580Z")).toArray(new String[0])[0],
//            rules));
//        assertEquals(ImmutableList.of("2013", "-04-01"), SplitStrategy.tokenise(
//            OrderedPropertyIndexEditorV2.encode(
//                PropertyValues.newString("2013-04-01T12:13:17.580Z")).toArray(new String[0])[0],
//            rules));
//        assertEquals(ImmutableList.of("2014", "-02-01"), SplitStrategy.tokenise(
//            OrderedPropertyIndexEditorV2.encode(
//                PropertyValues.newString("2014-02-01T12:13:17.580Z")).toArray(new String[0])[0],
//            rules));
//
//        rules = new MockRules(ImmutableList.of(4L, 6L, 6L, 3L));
//        assertEquals(ImmutableList.of("2013", "-02-01"), SplitStrategy.tokenise(
//            OrderedPropertyIndexEditorV2.encode(
//                PropertyValues.newString("2013-02-01T12:13:17.580Z")).toArray(new String[0])[0],
//            rules));
//        assertEquals(ImmutableList.of("2013", "-03-01"), SplitStrategy.tokenise(
//            OrderedPropertyIndexEditorV2.encode(
//                PropertyValues.newString("2013-03-01T12:13:17.580Z")).toArray(new String[0])[0],
//            rules));
//        assertEquals(ImmutableList.of("2013", "-04-01"), SplitStrategy.tokenise(
//            OrderedPropertyIndexEditorV2.encode(
//                PropertyValues.newString("2013-04-01T12:13:17.580Z")).toArray(new String[0])[0],
//            rules));
//        assertEquals(ImmutableList.of("2014", "-02-01"), SplitStrategy.tokenise(
//            OrderedPropertyIndexEditorV2.encode(
//                PropertyValues.newString("2014-02-01T12:13:17.580Z")).toArray(new String[0])[0],
//            rules));
    }

}