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

import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Implements the split strategy for the ordered indexes
 */
//TODO improve javadoc
public class SplitStrategy implements IndexStoreStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(SplitStrategy.class);

    /**
     * enum for easing the sort logic management
     */
    public enum SortLogic {
        STRING, DATE, LONG, DOUBLE
    }

    /**
     * property used for specifying the desired split
     */
    public static final String PROPERTY_SPLIT = "split";
    
    /**
     * property used for specifying the desired sort logic
     */
    public static final String PROPERTY_LOGIC = "logic";
    
    
    /**
     * used for passing an immutable easy-to-use index definition
     */
    public static class SplitRules {
        private final List<Long> split;
        private final SortLogic logic;
        
        /**
         * Create the class based on the index definition. See the {@link #SplitStrategy}
         * documentation for details.
         * 
         * @param indexDefinition cannot be null
         */
        public SplitRules(@Nonnull final NodeBuilder indexDefinition) {
            PropertyState sp = indexDefinition.getProperty(PROPERTY_SPLIT);
            PropertyState lo = indexDefinition.getProperty(PROPERTY_LOGIC);
            
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
    }
    
    public SplitStrategy(SplitRules rules) {
        
    }
    
    @Override
    public void update(NodeBuilder index, String path, Set<String> beforeKeys, Set<String> afterKeys) {
        LOG.debug("update()");
        // TODO Auto-generated method stub
        
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
