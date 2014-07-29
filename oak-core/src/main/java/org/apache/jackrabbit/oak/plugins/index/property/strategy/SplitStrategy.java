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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Random;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy.AdvancedIndexStoreStrategy;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.NodeCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.hash.Hashing;

/**
 * Implements the split strategy for the ordered indexes
 */
//TODO improve javadoc
public class SplitStrategy implements AdvancedIndexStoreStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(SplitStrategy.class);

    /**
     * limit used for the approximation algorithm as well as limit for the
     * {@link NodeState#getChildNodeCount(long)}
     */
    private static final long COUNT_LIMIT = 100L;
    
    /**
     * simple pojo for encoding the path.
     */
    private static class Sha1Path {
        private final String path;
        private final String sha1;
        
        public Sha1Path(@Nonnull final String path) {
            this.path = path;
            
            // composing a prefix to reduce the cost of a randomly distributed lists. By grepping
            // the first letter of each part of the path we'll group closer similar/siblings nodes.
            StringBuilder sb = new StringBuilder();
            Iterable<String> elements = PathUtils.elements(path);
            for (String e : elements) {
                sb.append(e.substring(0, 1));
            }
            sb.append("-");
            sb.append(Hashing.sha1().hashBytes(path.getBytes(Charsets.UTF_8)).toString());
            this.sha1 = sb.toString();
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
    private static void insert(final NodeBuilder index, final String key, final String path,
                               final Random rnd) {
        
        checkNotNull(index);
        checkNotNull(key);
        checkNotNull(path);
        checkNotNull(rnd);
        
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
        NodeCounter.nodeAdded(rnd, index, NodeCounter.PREFIX, COUNT_LIMIT);
    }

    /**
     * the actual delete from the index of the provided attributes
     * 
     * @param index
     * @param key
     * @param path
     */
    private static void remove(final NodeBuilder index, final String key, final String path,
                               final Random rnd) {
        checkNotNull(index);
        checkNotNull(key);
        checkNotNull(path);
        checkNotNull(rnd);
        
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
                NodeCounter.nodeRemoved(rnd, index, NodeCounter.PREFIX, COUNT_LIMIT);
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
    public void update(final NodeBuilder index, final String path, final Set<String> beforeKeys,
                       final Set<String> afterKeys) {
        update(index, path, beforeKeys, afterKeys, null);
    }
    
    /**
     * is the one doing the behind-the-scene work of {@link #update(NodeBuilder, String, Set, Set)}
     * using the provided Random instance.
     * 
     * @param index
     * @param path
     * @param beforeKeys
     * @param afterKeys
     * @param rnd the Random instance to be used. If nullit will generate a new as
     *            {@code new Random()}
     */
    public void update(NodeBuilder index, String path, Set<String> beforeKeys, Set<String> afterKeys, Random rnd) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("update() - path: {}", path);
            LOG.debug("update() - beforeKeys: {}", beforeKeys);
            LOG.debug("update() - afterKeys: {}", afterKeys);
        }
        
        // remove all the beforeKeys
        for (String key : beforeKeys) {
            remove(index, key, path, rnd == null ? new Random() : rnd);
        }
        
        // add all the afterKeys
        for (String key : afterKeys) {
            insert(index, key, path, rnd == null ? new Random() : rnd);
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
        throw new UnsupportedOperationException(
            "Unsupported as implementing AdvancedIndexStoreStrategy");
    }

    @Override
    public long count(final NodeState indexMeta, final PropertyRestriction pr, final long max) {
        
        checkNotNull(indexMeta, "IndexMeta cannot be null");
        checkNotNull(pr, "PropertyRestriction cannot be null");
        
        long count = Long.MAX_VALUE;
        NodeState content = indexMeta.getChildNode(INDEX_CONTENT_NODE_NAME);
        
        if (content.exists()) {
            // entryCount property
            PropertyState entryCount = indexMeta.getProperty(ENTRY_COUNT_PROPERTY_NAME);
            if (entryCount != null && Type.LONG.equals(entryCount.getType())) {
                count = entryCount.getValue(Type.LONG);
            } else {
                LOG.debug(
                    "count() - entryCount property not set or not valid type. entryCount: {}",
                    entryCount);
                if (pr.first == null && pr.last == null) {
                    // property IS NOT NULL case (open query)
                    LOG.debug("count() - property is not null case");
                    //count = traverseAndCount(content);
                }
            }
        }
        
        LOG.debug("count() -  total count: {}", count);
        return count;
    }    
}
