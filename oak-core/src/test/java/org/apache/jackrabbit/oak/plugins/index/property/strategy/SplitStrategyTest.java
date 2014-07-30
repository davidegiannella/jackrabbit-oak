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
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.property.AbstractPropertyIndexLookup;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedPropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.apache.jackrabbit.oak.util.NodeCounter;
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

    private static final EditorHook HOOK = new EditorHook(
        new IndexUpdateProvider(new OrderedPropertyIndexEditorProvider()));

    /**
     * adding of a new document
     */
    @Test
    public void insert() {
        NodeBuilder index;
        NodeBuilder node;
        final String path = "/content/foo/bar";
        final String sha1Path = "cfb-acdd763534a786e0d21adb9d6c6b1565d5bd5211";
        final Set<String> before = Sets.newHashSet();
        Set<String> after;
        
        index = EmptyNodeState.EMPTY_NODE.builder();
        after = Sets.newHashSet("app");
        node = index;
        STRATEGY.update(index, path, before, after);
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
        STRATEGY.update(index, path, before, after);
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
        STRATEGY.update(index, path, before, after);
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

        // -------------------------------- checking we have the counting properties on :index node
        Random rnd = new Random(1);
        index = EmptyNodeState.EMPTY_NODE.builder();
        after = Sets.newHashSet("app");
        for (int i = 0; i < 2000; i++) {
            STRATEGY.update(index, "/content/foo/bar" + i, before, after, rnd);
        }
        // with this random seeds we expect to have 5 :count-* properties
        assertEquals("with the provided seed we got a wrong number of properties", 5,
            countCountProperties(index, null));
        node = index.getChildNode("app");
        assertTrue(node.exists());
        node = node.getChildNode(OrderedIndex.FILLER);
        assertTrue(node.exists());
        assertEquals("No :count properties is expected at the ':' node", 0,
            countCountProperties(node, null));
    }
    
    /**
     * counts the number of the {@code :count-} properties within the provided node. See
     * {@link NodeCounter} for details.
     * 
     * @param node the node to inspect
     * @param properties if not null it will push in all the count property names
     * @return
     */
    private static int countCountProperties(@Nonnull final NodeBuilder node, 
                                            @Nullable List<String> properties) {
        checkNotNull(node);
        int counts = 0;
        for (PropertyState p : node.getProperties()) {
            String name = p.getName(); 
            if (name.startsWith(NodeCounter.PREFIX)) {
                counts++;
                if (properties != null) {
                    properties.add(name);
                }
            }
        }
        return counts;
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
        indexMeta = index.getNodeState();
        assertEquals(1600,
            STRATEGY.count(indexMeta, pr, AbstractPropertyIndexLookup.NO_LIMITS_COST));
    }
    
    @SuppressWarnings("unused")
    private static void printProperties(@Nonnull final NodeBuilder builder) {
        checkNotNull(builder);
        if (LOG.isDebugEnabled()) {
            LOG.debug("------");
            for (PropertyState p : builder.getProperties()) {
                LOG.debug("{}", p);
            }
        }
    }
    
    /**
     * cover the use case for reindex of the index
     * @throws CommitFailedException 
     */
    @Test
    public void reindex() throws CommitFailedException {
        final String indexDefName = "theIndex";
        NodeBuilder root, builder;
        NodeState before, after, indexed, state;
        List<String> countsBefore, countsAfter;
        
        // ------------------------- when re-index the :count properties must differs from previous        
        root = InitialContent.INITIAL_CONTENT.builder();
        builder = root.child(INDEX_DEFINITIONS_NAME);
        builder = builder.setChildNode(indexDefName, INDEX_DEF_V2);
        // pushing nodes
        before = root.getNodeState();
        for (int i = 0; i < 2000; i++) {
            builder = root.child("foo"+i);
            builder.setProperty(INDEXED_PROPERTY, "app");
        }
        after = root.getNodeState();
        indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        
        // asserting index state
        state = indexed.getChildNode(INDEX_DEFINITIONS_NAME);
        assertTrue(state.exists());
        state = state.getChildNode(indexDefName);
        assertTrue(state.exists());
        assertFalse(state.getBoolean(REINDEX_PROPERTY_NAME));
        state = state.getChildNode(INDEX_CONTENT_NODE_NAME);
        assertTrue(state.exists());
        countsBefore = new ArrayList<String>();
        countCountProperties(new ReadOnlyBuilder(state), countsBefore);
        assertFalse(countsBefore.isEmpty());
        
        // triggering a re-index
        root = indexed.builder();
        before = root.getNodeState();
        root.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(indexDefName)
            .setProperty(REINDEX_PROPERTY_NAME, true);
        after = root.getNodeState();
        indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        state = indexed.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(indexDefName);
        assertTrue(state.exists());
        assertFalse(state.getBoolean(REINDEX_PROPERTY_NAME));
        state = state.getChildNode(INDEX_CONTENT_NODE_NAME);
        assertTrue(state.exists());
        countsAfter = new ArrayList<String>();
        countCountProperties(new ReadOnlyBuilder(state), countsAfter);
        assertFalse(countsAfter.isEmpty());
        for (String name: countsAfter) {
            // when re-indexing the :count properties should be reset and recounted.
            assertFalse("Found a previously existing :count property", countsBefore.contains(name));
        }
    }
}
