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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public class SplitStrategyTest {
    @Test
    public void insert() {
        SplitStrategy strategy = new SplitStrategy();
        NodeBuilder index;
        NodeBuilder node;
        final String path = "/content/foo/bar";
        final String sha1Path = "acdd763534a786e0d21adb9d6c6b1565d5bd5211";
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
    
    @Test
    public void remove() {
        Map<String, String> shapaths = ImmutableMap.of(
            "acdd763534a786e0d21adb9d6c6b1565d5bd5211", "/content/foo/bar",
            "7ae9bbeab35f5e42b195f309acf0b53f8a14383f", "/content/baz/baz"
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
        assertTrue(node.getChildNode("acdd763534a786e0d21adb9d6c6b1565d5bd5211").exists());
        assertTrue(node.getChildNode("7ae9bbeab35f5e42b195f309acf0b53f8a14383f").exists());
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
         * let's recreate
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
}
