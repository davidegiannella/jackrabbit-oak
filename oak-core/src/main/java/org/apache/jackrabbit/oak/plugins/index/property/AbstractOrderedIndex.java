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
import java.util.PropertyResourceBundle;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry.Order;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;

/**
 * holders for common methos for the OrderedIndexes
 */
public abstract class AbstractOrderedIndex {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractOrderedIndex.class);
    
    /**
     * create a builder with some initial common settings
     * 
     * * {@code costPerExecution: 1}
     * * {@code costPerEntry: 1.1}
     * * {@code fulltextIndex: false}
     * * {@code includeNodesData: false}
     * * {@code filter: as the incoming one}
     * 
     * @param filter
     * @return 
     */
    static IndexPlan.Builder getIndexPlanBuilder(final Filter filter) {
        IndexPlan.Builder b = new IndexPlan.Builder();
        b.setCostPerExecution(1); // we're local. Low-cost
        // we're local but slightly more expensive than a standard PropertyIndex
        b.setCostPerEntry(1.1);
        b.setFulltextIndex(false); // we're never full-text
        b.setIncludesNodeData(false); // we should not include node data
        b.setFilter(filter);
        return b;
    }
    
    /**
     * process the sort conditions and return plans accordingly 
     * 
     * @param sortOrder the requested property to be sorted
     * @param lookup an instance of the lookup to be used
     * @param filter the current query filter
     * @param root the current root state
     * @param availableSorting the available sorting the current index can provide
     * @return
     */
    @Nonnull
    static List<IndexPlan> processSortOrder(@Nullable final List<OrderEntry> sortOrder, 
                                            @Nonnull final AbstractPropertyIndexLookup lookup, 
                                            @Nonnull final Filter filter, 
                                            @Nonnull final Iterable<Order> availableSorting) {
        List<IndexPlan> plans = Lists.newArrayList();
        
        if (sortOrder != null) {
            for (OrderEntry oe : sortOrder) {
                String propertyName = PathUtils.getName(oe.getPropertyName());
                if (lookup.isIndexed(propertyName, "/", filter)) {
                    NodeState indexMeta = lookup.getIndexNode(propertyName, filter);
                    IndexPlan.Builder b = getIndexPlanBuilder(filter);
                    List<OrderEntry> orders = Lists.newArrayList();
                    for (OrderEntry.Order order : availableSorting) {
                        orders.add(
                            new OrderEntry(
                                propertyName,
                                Type.UNDEFINED,
                                order)
                            );
                    }
                    b.setSortOrder(orders);
                    
                    // the cost is estimated as `property IS NOT NULL`
                    PropertyRestriction pr = new PropertyRestriction();
                    pr.propertyName = propertyName;                    
                    b.setEstimatedEntryCount(lookup.getEstimatedEntryCount(indexMeta, pr));
                    
                    IndexPlan plan = b.build();
                    LOG.debug("plan: {}", plan);
                    plans.add(plan);
                }
            }
        }
        return plans;
    }
    
    /**
     * process the {@code PropertyRestriction}s according to generic OrderedIndex rules such as:
     * 
     * we process 
     * * {@code property is not null}
     * * {@code property = value}
     * * {@code range queries}
     * * {@code sorting}
     * 
     * @param restrictions
     * @param lookup
     * @param filter
     * @param availableSorting
     * @return
     */
    static List<IndexPlan> processRestrictions(
                    @Nonnull final Collection<PropertyRestriction> restrictions,
                    @Nonnull final AbstractPropertyIndexLookup lookup,
                    @Nonnull final Filter filter,
                    @Nullable final List<OrderEntry> sortOrder,
                    @Nonnull final Iterable<Order> availableSorting) {
        List<IndexPlan> plans = Lists.newArrayList();
        for (Filter.PropertyRestriction pr : restrictions) {
            String propertyName = PathUtils.getName(pr.propertyName);
            if (lookup.isIndexed(propertyName, "/", filter)) {
                NodeState indexMeta = lookup.getIndexNode(propertyName, filter);
                PropertyValue value = null;
                boolean createPlan = false;
                if (pr.first == null && pr.last == null) {
                    // open query: [property] is not null
                    value = null;
                    createPlan = true;
                } else if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding
                           && pr.lastIncluding) {
                    // [property]=[value]
                    value = pr.first;
                    createPlan = true;
                } else if (pr.first != null && !pr.first.equals(pr.last)) {
                    // '>' & '>=' use cases
                    value = pr.first;
                    createPlan = true;
                } else if (pr.last != null && !pr.last.equals(pr.first)) {
                    // '<' & '<='
                    value = pr.last;
                    createPlan = true;
                }
                if (createPlan) {
                    // we can return a sorted as well as unsorted set
                    IndexPlan.Builder b = getIndexPlanBuilder(filter);
                    List<OrderEntry> o;
                    
                    // we have to reprocess the sort condition as we could apply on it as well
                    // with different costs
                    List<IndexPlan> orders = processSortOrder(sortOrder, lookup, filter,
                        availableSorting);
                    if (orders.isEmpty()) {
                        o = null;
                    } else {
                        o = Lists.newArrayList();
                        for (IndexPlan order : orders) {
                            o.addAll(order.getSortOrder());
                        }
                        
                    }
                    b.setSortOrder(o);
                    
                    // TODO implement the proper PropertyRestriction encoding
                    long count = lookup.getEstimatedEntryCount(indexMeta, null);
                    b.setEstimatedEntryCount(count);
                    LOG.debug("estimatedCount: {}", count);

                    IndexPlan plan = b.build();
                    LOG.debug("plan: {}", plan);
                    plans.add(plan);
                }
            }
        }

        return plans;
    }
}
