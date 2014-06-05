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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * Implements the split strategy for the ordered indexes
 */
//TODO improve javadoc
public class SplitStrategy implements IndexStoreStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(SplitStrategy.class);

    /**
     * char used for filling in with nodes.
     */
    public static final String FILLER = ":";

    /**
     * internal use as some libs want a char but Strings are normally better understood.
     */
    private static final char FILLER_C = FILLER.charAt(0);
    
    /**
     * the rules for the index
     */
    private final SplitRules rules;
    private final List<Long> split;
    private final SortLogic logic;

    
    
    /**
     * enum for easing the sort logic management
     */
    public enum SortLogic {
        STRING, DATE, LONG, DOUBLE
    }

    /**
     * used for passing an immutable easy-to-use index definition
     */
    public static class SplitRules {
        private final List<Long> split;
        private final SortLogic logic;
        private int length = -1;
        
        /**
         * Create the class based on the index definition. See the {@link #SplitStrategy}
         * documentation for details.
         * 
         * @param indexDefinition cannot be null
         */
        public SplitRules(@Nonnull final NodeBuilder indexDefinition) {
            PropertyState sp = indexDefinition.getProperty(OrderedIndex.PROPERTY_SPLIT);
            PropertyState lo = indexDefinition.getProperty(OrderedIndex.PROPERTY_LOGIC);
            
            if (sp != null && Type.LONGS.equals(sp.getType())) {
                split = Collections.unmodifiableList(Lists.newArrayList(sp.getValue(Type.LONGS)));
            } else {
                LOG.debug(
                    "Property 'split' is null or not of the correct type. Setting to null. split: {}",
                    sp);
                split = null;
            }
            
            if (lo != null && Type.STRING.equals(lo.getType())) {
                String s = lo.getValue(Type.STRING);
                SortLogic l;
                try {
                    l = SortLogic.valueOf(s.toUpperCase());
                } catch (IllegalArgumentException e) {
                    LOG.debug("Wrong logic specified. Defaulting to String");
                    l = SortLogic.STRING;
                }
                logic = l;
            } else {
                LOG.debug(
                    "Property 'logic' is null or not of the correct type. Setting to null. logic: {}",
                    lo);
                logic = null;
            }
            
        }
        
        /**
         * return the split rules to be applied
         * 
         * @return
         */
        public List<Long> getSplit() {
            return split;
        }
        
        /**
         * return the sort logic to be applied
         * @return
         */
        public SortLogic getLogic() {
            return logic;
        }
        
        /**
         * lazily compute and return the length the key should have for "balancing" the tree. See
         * {@link #SplitStrategy} for details.
         * 
         * @return
         */
        public int getLength() {
            if (length == -1) {
                // not computed yet. Let's do it.
                length = 0;
                for (Long l : getSplit()) {
                    length += l;
                }
            }
            return length;
        }
    }
    
    // ---------------------------------------------------------------------------------- < public >
    
    public SplitStrategy(@Nonnull final SplitRules rules) {
        this.rules = rules;
        this.split = rules.getSplit();
        this.logic = rules.getLogic();
        
    }
    
    @Override
    public void update(NodeBuilder index, String path, Set<String> beforeKeys, Set<String> afterKeys) {
        LOG.debug("update()");
        if (split == null || logic == null) {
            LOG.warn(
                "Index not correctly set. Missing split or logic settings. ABORTING! split: {} - logic: {}",
                split, logic);
        } else {
            // 1. tokenise
            // 2. create tree
            // 3. hash the path
            // 4. store the path
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

    /**
     * convert the input key into the set of nodes for creating the keys-tree. See
     * {@link #SplitStrategy} for details
     * 
     * @param key
     * @param rules
     * @return
     */
    static Iterable<String> tokenise(@Nonnull final String key, @Nonnull final SplitRules rules) {
        String k = Strings.padEnd(key, rules.getLength(), FILLER_C);
        List<String> tokens = new ArrayList<String>();
        Iterator<Long> iter = rules.getSplit().iterator();
        int start = 0;
        while (iter.hasNext()) {
            int l = (int) (long) iter.next();
            String s = k.substring(start, Math.min(k.length(), start+l));
            if (s.startsWith(FILLER)) {
                s = FILLER;
            }
            tokens.add(s);
            start += l;
        }
        return Collections.unmodifiableList(tokens);
    }
}
