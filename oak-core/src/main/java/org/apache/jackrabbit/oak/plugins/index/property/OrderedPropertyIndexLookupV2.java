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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy.AdvancedIndexStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.SplitStrategy;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderedPropertyIndexLookupV2 extends AbstractPropertyIndexLookup {
    private static final Logger LOG = LoggerFactory.getLogger(OrderedPropertyIndexLookupV2.class);
    private static final IndexStoreStrategy SPLIT_STRATEGY = new SplitStrategy();
    
    private final NodeState root;
    
    public OrderedPropertyIndexLookupV2(@Nonnull final NodeState root) {
        this.root = root;
    }

    @Override
    NodeState getRoot() {
        return root;
    }

    @Override
    String getType() {
        return OrderedIndex.TYPE_2;
    }

    @Override
    IndexStoreStrategy getStrategy(final NodeState indexMeta) {
        return SPLIT_STRATEGY;
    }

    @Override
    public long getEstimatedEntryCount(final NodeState indexMeta, final PropertyRestriction pr) {
        if (pr == null) {
            LOG.debug("PropertyRestriction is null. returning MAX_VALUE");
            return Long.MAX_VALUE;
            
        }        
        IndexStoreStrategy strategy = getStrategy(indexMeta);
        
        if (strategy instanceof AdvancedIndexStoreStrategy) {
            return ((AdvancedIndexStoreStrategy) strategy).count(indexMeta, pr, MAX_COST);
        } else {
            LOG.debug("Wrong strategy instance. Returning MAX_VALUE");
            return Long.MAX_VALUE;
        }
    }
}
