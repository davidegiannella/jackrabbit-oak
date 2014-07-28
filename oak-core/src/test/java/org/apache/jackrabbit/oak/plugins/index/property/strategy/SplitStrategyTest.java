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
package org.apache.jackrabbit.oak.plugins.index.property.strategy;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ENTRY_COUNT_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.property.AbstractPropertyIndexLookup;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public class SplitStrategyTest {
    private static final Logger LOG = LoggerFactory.getLogger(SplitStrategyTest.class);
    
    private static final String INDEXED_PROPERTY = "foo";
    private static final Set<String> EMPTY_SET = Sets.newHashSet();
    
    private static final SplitStrategy STRATEGY = new SplitStrategy();

    private static final NodeState INDEX_DEF_V2 = EmptyNodeState.EMPTY_NODE.builder()
        .setProperty(JcrConstants.JCR_PRIMARYTYPE, IndexConstants.INDEX_DEFINITIONS_NODE_TYPE,
            Type.NAME)
        .setProperty(IndexConstants.PROPERTY_NAMES, ImmutableList.of(INDEXED_PROPERTY), Type.NAMES)
        .setProperty(IndexConstants.TYPE_PROPERTY_NAME, OrderedIndex.TYPE_2)
        .setProperty(OrderedIndex.PROPERTY_SPLIT, ImmutableList.of(3L, 3L), Type.LONGS)
        .setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true, Type.BOOLEAN).getNodeState();

    /**
     * adding of a new document
     */
    @Test
    public void insert() {
        SplitStrategy strategy = new SplitStrategy();
        NodeBuilder index;
        NodeBuilder node;
        final String path = "/content/foo/bar";
        final String sha1Path = "cfb-acdd763534a786e0d21adb9d6c6b1565d5bd5211";
        Set<String> before = Sets.newHashSet();
        Set<String> after;
        
        index = EmptyNodeState.EMPTY_NODE.builder();
        after = Sets.newHashSet("app");
        node = index;
        strategy.update(index, path, before, after);
        node = node.getChildNode("app");
        assertTrue(node.exists());
        node = node.getChildNode(OrderedIndex.FILLER);
        assertTrue("a leaf should always have a FILLER if there'are paths under it", node.exists());
        node = node.getChildNode(sha1Path);
        assertTrue("SHA1 node doesn't exists", node.exists());
        assertEquals(path, node.getString(OrderedIndex.PROPERTY_PATH));
        
        index = EmptyNodeState.EMPTY_NODE.builder();
        after = Sets.newHashSet("app,les");
        node = index;
        strategy.update(index, path, before, after);
        node = node.getChildNode("app");
        assertTrue(node.exists());
        node = node.getChildNode("les");
        assertTrue(node.exists());
        node = node.getChildNode(OrderedIndex.FILLER);
        assertTrue("a leaf should always have a FILLER if there'are paths under it", node.exists());
        node = node.getChildNode(sha1Path);
        assertTrue("SHA1 node doesn't exists", node.exists());
        assertEquals(path, node.getString(OrderedIndex.PROPERTY_PATH));

        index = EmptyNodeState.EMPTY_NODE.builder();
        after = Sets.newHashSet("app,le,ts,:");
        node = index;
        strategy.update(index, path, before, after);
        node = node.getChildNode("app");
        assertTrue(node.exists());
        node = node.getChildNode("le");
        assertTrue(node.exists());
        node = node.getChildNode("ts");
        assertTrue(node.exists());
        node = node.getChildNode(":");
        assertTrue(node.exists());
        node = node.getChildNode(OrderedIndex.FILLER);
        assertTrue("a leaf should always have a FILLER if there'are paths under it", node.exists());
        node = node.getChildNode(sha1Path);
        assertTrue("SHA1 node doesn't exists", node.exists());
        assertEquals(path, node.getString(OrderedIndex.PROPERTY_PATH));
    }
    
    /**
     * deleting an existing document
     */
    @Test
    public void remove() {
        Map<String, String> shapaths = ImmutableMap.of(
            "cfb-acdd763534a786e0d21adb9d6c6b1565d5bd5211", "/content/foo/bar",
            "cbb-7ae9bbeab35f5e42b195f309acf0b53f8a14383f", "/content/baz/baz"
            );
        String sha, path;
        SplitStrategy strategy = new SplitStrategy();
        NodeBuilder index, node, doc;
        Set<String> before;
        Set<String> after = Sets.newHashSet();
        
        // let's test that by removing a property a the path is removed
        // initialising the structure
        index = EmptyNodeState.EMPTY_NODE.builder();
        node = index.child("app");
        node = node.child(OrderedIndex.FILLER);
        for (String s : shapaths.keySet()) {
            doc = node.child(s);
            doc.setProperty(OrderedIndex.PROPERTY_PATH, shapaths.get(s));
        }
        // for paranoia's sake let's assert the index structure. Just once :)
        node = index.getChildNode("app");
        assertTrue(node.exists());
        node = node.getChildNode(OrderedIndex.FILLER);
        assertTrue(node.exists());
        for (String k : shapaths.keySet()) {
            assertTrue(node.getChildNode(k).exists());
        }
        
        // removing the first item
        sha = shapaths.keySet().iterator().next();
        path = shapaths.get(sha);
        before = Sets.newHashSet("app");
        strategy.update(index, path, before, after);
        node = index.getChildNode("app");
        assertTrue(node.exists());
        node = node.getChildNode(OrderedIndex.FILLER);
        assertTrue(node.exists());
        assertFalse(
            "As we removed the property we don't exepct the path exists anylonger in the index",
            node.getChildNode(sha).exists());
        
        // if we remove the only path and nothing is left we should clear the tree
        sha = shapaths.keySet().iterator().next();
        path = shapaths.get(sha);
        index = EmptyNodeState.EMPTY_NODE.builder();
        node = index.child("app");
        node = node.child(OrderedIndex.FILLER);
        node = node.child(sha);
        before = Sets.newHashSet("app");
        strategy.update(index, path, before, after);
        assertFalse(index.getChildNode("app").exists());
       
        /*
         * if there are some other children left we should keep part of the tree
         * let's create something like
         * 
         *  :index : {
         *      app : {
         *          : {
         *              someshapath : {}
         *          },
         *          lets : {
         *              someothersha : {}
         *          }
         *      }
         *  }
         * 
         */
        String[] shas = shapaths.keySet().toArray(new String[0]);
        index = EmptyNodeState.EMPTY_NODE.builder();
        node = index.child("app");
        node.child("lets").child(OrderedIndex.FILLER).child(shas[0]);
        node.child(OrderedIndex.FILLER).child(shas[1]);
        before = Sets.newHashSet("app");
        path = shapaths.get(shas[1]);
        strategy.update(index, path, before, after);
        node = index.getChildNode("app");
        assertTrue(node.exists());
        assertFalse(node.getChildNode(OrderedIndex.FILLER).exists());
        node = node.getChildNode("lets");
        assertTrue(node.exists());
        node = node.getChildNode(OrderedIndex.FILLER);
        assertTrue(node.exists());
        node = node.getChildNode(shas[0]);
        assertTrue(node.exists());
    }
    
    /**
     * change the property value of an existing document
     */
    @Test
    public void change() {
        SplitStrategy strategy = new SplitStrategy();
        NodeBuilder index, node;
        Set<String> before, after;
        String path, sha1;
        
        sha1 = "cfb-acdd763534a786e0d21adb9d6c6b1565d5bd5211";
        path = "/content/foo/bar";

        index = EmptyNodeState.EMPTY_NODE.builder();
        node = index.child("app");
        node = node.child("le:");
        node = node.child(OrderedIndex.FILLER);
        node = node.child(sha1);
        
        before = Sets.newHashSet("app,le:");
        after = Sets.newHashSet("app,les");
        strategy.update(index, path, before, after);
        node = index.getChildNode("app");
        assertTrue(node.exists());
        assertFalse("the document was the only child it should have been cleared", 
            node.getChildNode("le:").exists());
        node = node.getChildNode("les");
        assertTrue(node.exists());
        node = node.getChildNode(OrderedIndex.FILLER);
        assertTrue(node.exists());
        node = node.getChildNode(sha1);
        assertTrue(node.exists());
    }
    
    /**
     * assert a count for conditions like {@code property IS NOT NULL}
     */
    @Test
    public void countNotNull() {
        final String indexDefName = "theIndex";
        Random rnd;
        NodeBuilder root, index, indexContent, builder;
        NodeState indexMeta;
        PropertyRestriction pr;
        long count;
        
        
        // -------- creating a structure with entryCount specified but no content in there actually
        root = InitialContent.INITIAL_CONTENT.builder();
        root.child(INDEX_DEFINITIONS_NAME).setChildNode(indexDefName, INDEX_DEF_V2)
            .setProperty(ENTRY_COUNT_PROPERTY_NAME, 1L, Type.LONG);
        indexMeta = root.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(indexDefName)
            .getNodeState();
        pr = new PropertyRestriction();
        pr.propertyName = INDEXED_PROPERTY;
        assertEquals("if we don't have the :index node no query", Long.MAX_VALUE,
            STRATEGY.count(indexMeta, pr, 10));
        
        // ---------------------- saving 2000 nodes should still return 1 as of entryCount property
        root = InitialContent.INITIAL_CONTENT.builder();
        index = root.child(INDEX_DEFINITIONS_NAME).setChildNode(indexDefName, INDEX_DEF_V2)
            .setProperty(ENTRY_COUNT_PROPERTY_NAME, 1L, Type.LONG);
        indexContent = index.child(INDEX_CONTENT_NODE_NAME);
        for (int i = 0; i < 2000; i++) {
            STRATEGY.update(indexContent, "/content/foobar" + i, EMPTY_SET,
                Sets.newHashSet("app,les"));
        }
        // asserting the index status
        builder = indexContent;
        builder = builder.getChildNode("app");
        assertTrue(builder.exists());
        builder = builder.getChildNode("les");
        assertTrue(builder.exists());
        builder = builder.getChildNode(OrderedIndex.FILLER);
        assertTrue(builder.exists());
        assertEquals(2000, builder.getChildNodeCount(2100));
        // checking the count
        indexMeta = index.getNodeState();
        pr = new PropertyRestriction();
        pr.propertyName = INDEXED_PROPERTY;
        assertEquals(1, STRATEGY.count(indexMeta, pr, AbstractPropertyIndexLookup.NO_LIMITS_COST));

        // ------------------------------------------------------------ count with one key add only
        rnd = new Random(1);
        root = InitialContent.INITIAL_CONTENT.builder();
        index = root.child(INDEX_DEFINITIONS_NAME).setChildNode(indexDefName, INDEX_DEF_V2);
        indexContent = index.child(INDEX_CONTENT_NODE_NAME);
        for (int i = 0; i < 2000; i++) {
            STRATEGY.update(indexContent, "/content/foobar" + i, EMPTY_SET,
                Sets.newHashSet("app,les"), rnd);
        }
        // asserting the index status
        builder = indexContent;
        builder = builder.getChildNode("app");
        assertTrue(builder.exists());
        builder = builder.getChildNode("les");
        assertTrue(builder.exists());
        builder = builder.getChildNode(OrderedIndex.FILLER);
        assertTrue(builder.exists());
        assertEquals(2000, builder.getChildNodeCount(2100));
        printProperties(builder);
        // checking the count
        indexMeta = index.getNodeState();
        pr = new PropertyRestriction();
        pr.propertyName = INDEXED_PROPERTY;
        // as it is an estimation based on randomisation we can assess a range of allowed values
        count = STRATEGY.count(indexMeta, pr, AbstractPropertyIndexLookup.NO_LIMITS_COST);
        assertEquals(1600, count);
        
        // ----------------------------------------------------------- count with two keys add only
        rnd = new Random(1);
        root = InitialContent.INITIAL_CONTENT.builder();
        index = root.child(INDEX_DEFINITIONS_NAME).setChildNode(indexDefName, INDEX_DEF_V2);
        indexContent = index.child(INDEX_CONTENT_NODE_NAME);
        for (int i = 0; i < 2000; i++) {
            STRATEGY.update(indexContent, "/content/foobar" + i, EMPTY_SET,
                Sets.newHashSet("app,les"), rnd);
        }
        for (int i = 0; i < 2000; i++) {
            STRATEGY.update(indexContent, "/content/bazbaz" + i, EMPTY_SET,
                Sets.newHashSet("app,le:"), rnd);
        }
        builder = indexContent;
        builder = builder.getChildNode("app");
        assertTrue(builder.exists());
        builder = builder.getChildNode("les");
        assertTrue(builder.exists());
        builder = builder.getChildNode(OrderedIndex.FILLER);
        assertTrue(builder.exists());
        assertEquals(2000, builder.getChildNodeCount(2100));
        printProperties(builder);
        builder = indexContent;
        builder = builder.getChildNode("app");
        assertTrue(builder.exists());
        builder = builder.getChildNode("le:");
        assertTrue(builder.exists());
        builder = builder.getChildNode(OrderedIndex.FILLER);
        assertTrue(builder.exists());
        assertEquals(2000, builder.getChildNodeCount(2100));
        printProperties(builder);
        indexMeta = index.getNodeState();
        pr = new PropertyRestriction();
        pr.propertyName = INDEXED_PROPERTY;
        count = STRATEGY.count(indexMeta, pr, AbstractPropertyIndexLookup.NO_LIMITS_COST);
        assertEquals(4800, count);
        
        // ---------------------------------- no properties for estimations and less than 100 nodes
        root = InitialContent.INITIAL_CONTENT.builder();
        index = root.child(INDEX_DEFINITIONS_NAME).setChildNode(indexDefName, INDEX_DEF_V2);
        indexContent = index.child(INDEX_CONTENT_NODE_NAME);
        for (int i = 0; i < 90; i++) {
            STRATEGY.update(indexContent, "/content/foobar" + i, EMPTY_SET,
                Sets.newHashSet("app,les"), rnd);
        }
        builder = indexContent;
        builder = builder.getChildNode("app");
        assertTrue(builder.exists());
        builder = builder.getChildNode("les");
        assertTrue(builder.exists());
        builder = builder.getChildNode(OrderedIndex.FILLER);
        assertTrue(builder.exists());
        assertEquals(90, builder.getChildNodeCount(2100));
        // cleaning up the properties to force the use case
        for (PropertyState p : builder.getProperties()) {
            builder.removeProperty(p.getName());
        }
        indexMeta = index.getNodeState();
        pr = new PropertyRestriction();
        pr.propertyName = INDEXED_PROPERTY;
        count = STRATEGY.count(indexMeta, pr, AbstractPropertyIndexLookup.NO_LIMITS_COST);
        assertEquals(90, count);        
    }
    
    private static void printProperties(@Nonnull final NodeBuilder builder) {
        checkNotNull(builder);
        if (LOG.isDebugEnabled()) {
            LOG.debug("------");
            for (PropertyState p : builder.getProperties()) {
                LOG.debug("{}", p);
            }
        }
    }
}
