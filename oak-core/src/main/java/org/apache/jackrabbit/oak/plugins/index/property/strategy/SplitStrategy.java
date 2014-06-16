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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.hash.Hashing;

/**
 * Implements the split strategy for the ordered indexes
 */
//TODO improve javadoc
public class SplitStrategy implements IndexStoreStrategy {
    public static final Logger LOG = LoggerFactory.getLogger(SplitStrategy.class);

    /**
     * simple pojo for encoding the path.
     */
    private static class Sha1Path {
        private final String path;
        private final String sha1;
        
        public Sha1Path(@Nonnull final String path) {
            this.path = path;
            this.sha1 = Hashing.sha1().hashBytes(path.getBytes(Charsets.UTF_8)).toString();
        }
        
        public String getPath() {
            return path;
        }
        
        public String getSha1() {
            return sha1;
        }
    }
    
    /**
     * the actual add into the index of the provided attributes
     * 
     * @param index
     * @param key
     * @param path
     */
    private static void insert(final NodeBuilder index, final String key, final String path) {
        LOG.debug("insert()");
        Iterable<String> tokens;
        NodeBuilder node;
        
        // 1. tokenise
        tokens = Splitter.on(OrderedIndex.SPLITTER).split(key);
        // 2. create tree
        node = index;
        for (String token : tokens) {
            node = node.child(token);
        }
        
        // 'node' should now be our leaf where we should add the path
        // let's add a special node that will be treated specially for the sortin
        // putting it as first or last depending on the order by.
        // here we'll put our paths
        node = node.child(OrderedIndex.FILLER);
        
        // 3. hash and store the path
        Sha1Path sp = new Sha1Path(path);
        node = node.child(sp.getSha1());
        node.setProperty(OrderedIndex.PROPERTY_PATH, sp.getPath());
    }

    /**
     * the actual delete from the index of the provided attributes
     * 
     * @param index
     * @param key
     * @param path
     */
    private static void remove(final NodeBuilder index, final String key, final String path) {
        LOG.debug("remove()");
        Deque<NodeBuilder> nodes;
        Iterable<String> tokens;
        NodeBuilder node;

        // each node will have a ':' as leaf for keeping the paths. Let's add it
        String k = key + OrderedIndex.SPLITTER + OrderedIndex.FILLER;

        // 1. walking down the tree and keeping track of the path for later cleaning up
        nodes = new ArrayDeque<NodeBuilder>();
        tokens = Splitter.on(OrderedIndex.SPLITTER).split(k);
        node = index;
        for (String s : tokens) {
            node = node.getChildNode(s);
            if (node.exists()) {
                nodes.addFirst(node);
            } else {
                if (!OrderedIndex.FILLER.equals(s)) {
                    LOG.debug(
                        "Something weird here but it could be. '{}' doesn't exits anymore. quitting.",
                        s);
                    node = null;
                    break;
                }
            }
        }
        // 'node' will now contain the leaf of the tree

        if (node != null) {
            // 2. delete the node
            Sha1Path sp = new Sha1Path(path);
            node = node.getChildNode(sp.getSha1());
            if (node.exists()) {
                node.remove();
            }

            // 3. walking up the tree and delete nodes if no children
            while (!nodes.isEmpty()) {
                node = nodes.removeFirst();
                if (node.getChildNodeCount(1) == 0) {
                    node.remove();
                } else {
                    break;
                }
            }
        }
    }

    @Override
    public void update(NodeBuilder index, String path, Set<String> beforeKeys, Set<String> afterKeys) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("update() - path: {}", path);
            LOG.debug("update() - beforeKeys: {}", beforeKeys);
            LOG.debug("update() - afterKeys: {}", afterKeys);
        }
        
        // remove all the beforeKeys
        for (String key : beforeKeys) {
            remove(index, key, path);
        }
        
        // add all the afterKeys
        for (String key : afterKeys) {
            insert(index, key, path);
        }
    }


    @Override
    public Iterable<String> query(Filter filter, String indexName, NodeState indexMeta,
                                  Iterable<String> values) {
        // TODO Auto-generated method stub
        LOG.debug("query()");
        return null;
    }

    @Override
    public long count(NodeState indexMeta, Set<String> values, int max) {
        LOG.debug("count()");
        // TODO Auto-generated method stub
        return 0;
    }
}
