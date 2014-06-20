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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedPropertyIndexEditorV2.SplitRules;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.memory.StringBasedBlob;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class OrderedPropertyIndexEditorV2Test {
    private static final EditorHook HOOK = new EditorHook(new IndexUpdateProvider(
        new OrderedPropertyIndexEditorProvider()));

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
        assertFalse("not indexed property", editor.isToProcess(property));
        property = PropertyStates.createProperty(indexed, Collections.EMPTY_LIST, Type.STRINGS);
        assertFalse("empty lists are not processed", editor.isToProcess(property));
        property = PropertyStates.createProperty(indexed, new StringBasedBlob("justavalue"),
            Type.BINARY);
        assertFalse("binaries are not processed", editor.isToProcess(property));
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

    /**
     * tests the declaringNodeType filtering
     * @throws CommitFailedException 
     */
    @Test
    public void declaringNodeType() throws CommitFailedException {
        final String propertyNames = "indexed";
        final String nodeType = NodeTypeConstants.NT_OAK_UNSTRUCTURED;
        final String indexDefName = "TheIndex";
        final String okNode = "oknode";
        final String okSha1 = "2fad94fc15205634812cec7737ebc11d8e9db5c6";
        final String wrongNode = "WRONGnode";
        final String wrongSha1 = "f7dc04a5f3b8db1fd6dc16422ffda6a3c4448685";
                
        NodeState root = InitialContent.INITIAL_CONTENT;
        NodeBuilder builder = root.builder();
        NodeBuilder node;
        NodeBuilder indexDef = EmptyNodeState.EMPTY_NODE.builder()
            .setProperty(JcrConstants.JCR_PRIMARYTYPE, IndexConstants.INDEX_DEFINITIONS_NODE_TYPE,
                Type.NAME)
            .setProperty(IndexConstants.PROPERTY_NAMES, ImmutableList.of(propertyNames), Type.NAMES)
            .setProperty(IndexConstants.DECLARING_NODE_TYPES, ImmutableList.of(nodeType),
                Type.NAMES)
            .setProperty(IndexConstants.TYPE_PROPERTY_NAME, OrderedIndex.TYPE_2)
            .setProperty(OrderedIndex.PROPERTY_SPLIT, ImmutableList.of(3L, 3L), Type.LONGS)
            .setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true, Type.BOOLEAN);
        NodeState before, after, indexed, ns, bookmark;
        
        //creating index definition
        node = builder.child(IndexConstants.INDEX_DEFINITIONS_NAME);
        node.setChildNode(indexDefName, indexDef.getNodeState());
        
        // adding a node with the proper nodetype should be indexed
        
        before = builder.getNodeState();
        
        // adding a node that should be indexed
        builder.child(okNode)
            .setProperty(JcrConstants.JCR_PRIMARYTYPE, nodeType, Type.NAME)
            .setProperty(propertyNames, "apple");
        
        after = builder.getNodeState();
        
        // processing the commits
        indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        
        ns = indexed.getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME);
        assertTrue(ns.exists());
        ns = ns.getChildNode(indexDefName);
        assertTrue(ns.exists());
        ns = ns.getChildNode(IndexConstants.INDEX_CONTENT_NODE_NAME);
        assertTrue(ns.exists());
        ns = ns.getChildNode("app");
        assertTrue(ns.exists());
        ns = ns.getChildNode("le:");
        assertTrue(ns.exists());
        ns = ns.getChildNode(OrderedIndex.FILLER);
        assertTrue(ns.exists());
        ns = ns.getChildNode(okSha1);
        assertTrue(ns.exists());
        assertEquals("/" + okNode, ns.getString(OrderedIndex.PROPERTY_PATH));
        
        // adding a node with the wrong nodetype should result in no indexing.
        builder = indexed.builder();
        before = indexed;

        builder.child(wrongNode)
        .setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME)
        .setProperty(propertyNames, "apple");
        
        after = builder.getNodeState();
        
        indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        
        // the old node should still be there
        ns = indexed.getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME);
        assertTrue(ns.exists());
        ns = ns.getChildNode(indexDefName);
        assertTrue(ns.exists());
        ns = ns.getChildNode(IndexConstants.INDEX_CONTENT_NODE_NAME);
        assertTrue(ns.exists());
        ns = ns.getChildNode("app");
        assertTrue(ns.exists());
        ns = ns.getChildNode("le:");
        assertTrue(ns.exists());
        ns = ns.getChildNode(OrderedIndex.FILLER);
        assertTrue(ns.exists());
        bookmark = ns; // keeping a bookmark for not rewalking from scratch
        ns = ns.getChildNode(okSha1);
        assertTrue(ns.exists());
        assertEquals("/" + okNode, ns.getString(OrderedIndex.PROPERTY_PATH));
        ns = bookmark.getChildNode(wrongSha1);
        assertFalse("a node with the wrong nodeType should not exists", ns.exists());
        
        // by changing the node type to an existing node it should be indexed if all conditions
        // match
        builder = indexed.builder();
        before = indexed;
        builder.getChildNode(wrongNode)
            .setProperty(JcrConstants.JCR_PRIMARYTYPE, nodeType, Type.NAME);
        after = builder.getNodeState();
        indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY); 
        ns = indexed.getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME);
        assertTrue(ns.exists());
        ns = ns.getChildNode(indexDefName);
        assertTrue(ns.exists());
        ns = ns.getChildNode(IndexConstants.INDEX_CONTENT_NODE_NAME);
        assertTrue(ns.exists());
        ns = ns.getChildNode("app");
        assertTrue(ns.exists());
        ns = ns.getChildNode("le:");
        assertTrue(ns.exists());
        ns = ns.getChildNode(OrderedIndex.FILLER);
        assertTrue(ns.exists());
        bookmark = ns; // keeping a bookmark for not rewalking from scratch
        ns = ns.getChildNode(okSha1);
        assertTrue(ns.exists());
        assertEquals("/" + okNode, ns.getString(OrderedIndex.PROPERTY_PATH));
        ns = bookmark.getChildNode(wrongSha1);
        assertTrue("as the nodetype changed we expect the node to be indexed", ns.exists());
        
        // changing againt he nodetype to something not allowed should remove the node from the
        // index
        builder = indexed.builder();
        before = indexed;
        builder.getChildNode(wrongNode)
            .setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
        after = builder.getNodeState();
        indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY); 
        ns = indexed.getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME);
        assertTrue(ns.exists());
        ns = ns.getChildNode(indexDefName);
        assertTrue(ns.exists());
        ns = ns.getChildNode(IndexConstants.INDEX_CONTENT_NODE_NAME);
        assertTrue(ns.exists());
        ns = ns.getChildNode("app");
        assertTrue(ns.exists());
        ns = ns.getChildNode("le:");
        assertTrue(ns.exists());
        ns = ns.getChildNode(OrderedIndex.FILLER);
        assertTrue(ns.exists());
        bookmark = ns; // keeping a bookmark for not rewalking from scratch
        ns = ns.getChildNode(okSha1);
        assertTrue(ns.exists());
        assertEquals("/" + okNode, ns.getString(OrderedIndex.PROPERTY_PATH));
        ns = bookmark.getChildNode(wrongSha1);
        assertFalse("a node with the wrong nodeType should not exists", ns.exists());
    }
    
    /**
     * checks that if we define an index under a specific path it will index only that path of
     * content.
     * 
     * @throws CommitFailedException 
     */
    @Test
    public void pathSpecifcIndexes() throws CommitFailedException {
        final String content = "content";
        final String contentNode = "nodeUnderContent";
        final String repoWideContentNodeSha1 = "1fd86c2368aacab0b025615ad82dfc5a6767cc95";
        final String contentWideContentNodeSha1 = "10e8c9ccec2e58611b3521fec924a7fda70487cd";
        final String rootNode = "nodeUnderRoot";
        final String rootNodeSha1 = "e931bbb6f91b0554a57c0dc8282c895c33b9a0fc";
        final String indexedProperty = "indexedProperty";
        final String repoWideIndex = "repoWideIndex";
        final String pathSpecificIndex = "pathSpecificIndex";
        NodeBuilder root = InitialContent.INITIAL_CONTENT.builder();
        NodeState indexDef = EmptyNodeState.EMPTY_NODE.builder()
            .setProperty(JcrConstants.JCR_PRIMARYTYPE, IndexConstants.INDEX_DEFINITIONS_NODE_TYPE,
                Type.NAME)
            .setProperty(IndexConstants.PROPERTY_NAMES, ImmutableList.of(indexedProperty), Type.NAMES)
            .setProperty(IndexConstants.TYPE_PROPERTY_NAME, OrderedIndex.TYPE_2)
            .setProperty(OrderedIndex.PROPERTY_SPLIT, ImmutableList.of(3L, 3L), Type.LONGS)
            .setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true, Type.BOOLEAN).getNodeState();
        NodeBuilder node;
        NodeState before, after, indexed, state, bookmark;
        
        // defining the indexes
        node = root.child(IndexConstants.INDEX_DEFINITIONS_NAME);
        node.setChildNode(repoWideIndex, indexDef);
        node = root.child(content);
        node = node.child(IndexConstants.INDEX_DEFINITIONS_NAME);
        node.setChildNode(pathSpecificIndex, indexDef);
        
        before = root.getNodeState();
        
        // adding the nodes
        node = root.getChildNode(content);
        node = node.child(contentNode);
        node.setProperty(indexedProperty, "apple");
        node = root.child(rootNode);
        node.setProperty(indexedProperty, "pear");
        
        after = root.getNodeState();
        
        indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        
        // do we really have the nodes. Paranoia to the rescue!
        state = indexed.getChildNode(content);
        assertTrue(state.exists());
        state = state.getChildNode(contentNode);
        assertTrue(state.exists());
        
        state = indexed.getChildNode(rootNode);
        assertTrue(state.exists());
        
        // the repo-wide index should have both nodes
        state = indexed.getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME);
        assertTrue(state.exists());
        state = state.getChildNode(repoWideIndex);
        assertTrue(state.exists());
        state = state.getChildNode(IndexConstants.INDEX_CONTENT_NODE_NAME);
        bookmark = state;
        assertTrue(state.exists());
        state = state.getChildNode("app");
        assertTrue(state.exists());
        state = state.getChildNode("le:");
        assertTrue(state.exists());
        state = state.getChildNode(OrderedIndex.FILLER);
        assertTrue(state.exists());
        state = state.getChildNode(repoWideContentNodeSha1);
        assertTrue("repo-wide index should have both the added nodes", state.exists());
        assertEquals("/" + content + "/" + contentNode, state.getString(OrderedIndex.PROPERTY_PATH));
        state = bookmark.getChildNode("pea");
        assertTrue(state.exists());
        state = state.getChildNode("r::");
        assertTrue(state.exists());
        state = state.getChildNode(OrderedIndex.FILLER);
        assertTrue(state.exists());
        state = state.getChildNode(rootNodeSha1);
        assertTrue("repo-wide index should have both the added nodes", state.exists());
        assertEquals("/" + rootNode, state.getString(OrderedIndex.PROPERTY_PATH));
        
        // the path-specific index should have only 1 node
        state = indexed.getChildNode(content);
        state = state.getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME);
        assertTrue(state.exists());
        state = state.getChildNode(pathSpecificIndex);
        assertTrue(state.exists());
        state = state.getChildNode(IndexConstants.INDEX_CONTENT_NODE_NAME);
        assertTrue(state.exists());
        bookmark = state;
        state = state.getChildNode("app");
        assertTrue(state.exists());
        state = state.getChildNode("le:");
        assertTrue(state.exists());
        state = state.getChildNode(OrderedIndex.FILLER);
        assertTrue(state.exists());
        state = state.getChildNode(contentWideContentNodeSha1);
        assertTrue(state.exists());
        state = bookmark.getChildNode("pea");
        assertFalse("as it's path-specific index we should not have this", state.exists());
    }
}