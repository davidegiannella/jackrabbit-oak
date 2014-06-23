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

import java.util.Collection;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.AdvancedQueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;

public class OrderedPropertyIndexV2 extends AbstractOrderedIndex implements QueryIndex, AdvancedQueryIndex {
    private static final Logger LOG = LoggerFactory.getLogger(OrderedPropertyIndexV2.class);

    /**
     * all the ORDER BY the current index can provide
     */
    private static final Iterable<OrderEntry.Order> ORDERS = ImmutableList.of(
        OrderEntry.Order.ASCENDING, OrderEntry.Order.DESCENDING);
    
    @Override
    public List<IndexPlan> getPlans(Filter filter, List<OrderEntry> sortOrder, NodeState rootState) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getPlans() - filter: {}", filter);
            LOG.debug("getPlans() - sortOrder: {}", sortOrder);
            LOG.debug("getPlans() - rootState: {}", rootState);
        }
        AbstractPropertyIndexLookup lookup = getLookup(rootState);
        List<IndexPlan> plans = Lists.newArrayList();
        
        if (sortOrder != null) {
            plans.addAll(processSortOrder(sortOrder, lookup, filter, ORDERS));
        }
        
        Collection<PropertyRestriction> restrictions = filter.getPropertyRestrictions();
        if (restrictions != null) {
            plans.addAll(processRestrictions(restrictions, lookup, filter, sortOrder, ORDERS));
        }
        
        return plans; 
    }

    @Override
    public String getPlanDescription(IndexPlan plan, NodeState root) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getPlanDescription() - plan: {}", plan);
            LOG.debug("getPlanDescription() - root: {}", root);
        }
        return null;
    }

    @Override
    public Cursor query(IndexPlan plan, NodeState rootState) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("query() - plan: {}", plan);
            LOG.debug("query() - rootState: {}", rootState);
        }
        // TODO Auto-generated method stub
        return null;
    }

    private OrderedPropertyIndexLookupV2 getLookup(@Nonnull final NodeState root) {
        return new OrderedPropertyIndexLookupV2(root);
    }

    @Override
    public String getIndexName() {
        return OrderedIndex.TYPE_2;
    }

    // ------------------------------------------------------------------------- QueryIndex methods
    @Override
    public double getCost(Filter filter, NodeState rootState) {
        throw new UnsupportedOperationException("Not supported as implementing AdvancedQueryIndex");
    }

    @Override
    public Cursor query(Filter filter, NodeState rootState) {
        throw new UnsupportedOperationException("Not supported as implementing AdvancedQueryIndex");
    }

    @Override
    public String getPlan(Filter filter, NodeState rootState) {
        throw new UnsupportedOperationException("Not supported as implementing AdvancedQueryIndex");
    }
}
