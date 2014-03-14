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

import static org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.TYPE;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Cursors;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.AdvancedQueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A property index that supports ordering keys.
 */
public class OrderedPropertyIndex extends PropertyIndex implements AdvancedQueryIndex {
    private static final Logger LOG = LoggerFactory.getLogger(OrderedPropertyIndex.class);
    
    @Override
    public String getIndexName() {
        return TYPE;
    }

    @Override
    PropertyIndexLookup getLookup(NodeState root) {
        return new OrderedPropertyIndexLookup(root);
    }

    /**
     * retrieve the cost for the query.
     * 
     * !!! for now we want to skip the use-case of NON range-queries !!!
     */
    @Override
    public double getCost(Filter filter, NodeState root) {
        double cost = Double.POSITIVE_INFINITY;

        if (filter.getFullTextConstraint() == null && !filter.containsNativeConstraint()) {
            PropertyIndexLookup pil = getLookup(root);
            if (pil instanceof OrderedPropertyIndexLookup) {
                OrderedPropertyIndexLookup lookup = (OrderedPropertyIndexLookup) pil;
                for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
                    String propertyName = PathUtils.getName(pr.propertyName);
                    if (lookup.isIndexed(propertyName, "/", filter)) {
                        // '>' && '>=' case
                        if (pr.first != null && !pr.first.equals(pr.last)
                            && lookup.isAscending(root, propertyName, filter)) {
                            cost = lookup.getCost(filter, propertyName, pr.first);
                        }
                        // '<'  && '<=' case
                        else if (pr.last != null && !pr.last.equals(pr.first)
                                   && !lookup.isAscending(root, propertyName, filter)) {
                            cost = lookup.getCost(filter, propertyName, pr.last);
                        }
                    }
                }
            }
        }

        return cost;
    }
    
    @Override
    public Cursor query(Filter filter, NodeState root) {
        LOG.debug("query(Filter, NodeState)");
        Iterable<String> paths = null;
        Cursor cursor = null;
        PropertyIndexLookup pil = getLookup(root);
        if (pil instanceof OrderedPropertyIndexLookup) {
            OrderedPropertyIndexLookup lookup = (OrderedPropertyIndexLookup) pil;
            int depth = 1;
            for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
                String propertyName = PathUtils.getName(pr.propertyName);
                depth = PathUtils.getDepth(pr.propertyName);
                if (lookup.isIndexed(propertyName, "/", filter)) {
//                    if (pr.first != null && !pr.first.equals(pr.last)) {
//                        // '>' & '>=' case
//                        paths = lookup.query(filter, propertyName, pr);
//                    } else {
                        // processed as "[property] is not null"
                        paths = lookup.query(filter, propertyName, pr);
//                        break;
//                    }
                } 
            }
            if (paths == null) {
                throw new IllegalStateException(
                    "OrderedPropertyIndex index is used even when no index is available for filter "
                        + filter);
            }
            cursor = Cursors.newPathCursor(paths);
            if (depth > 1) {
                cursor = Cursors.newAncestorCursor(cursor, depth - 1);
            }
        } else {
            // if for some reasons it's not an Ordered Lookup we delegate up the chain
            cursor = super.query(filter, root);
        }
        return cursor;
    }

    @Override
    public List<IndexPlan> getPlans(Filter filter, List<OrderEntry> sortOrder, NodeState root) {
        LOG.debug("getPlans() - filter: {} - ", filter);
        LOG.debug("getPlans() - sortOrder: {} - ", sortOrder);
        LOG.debug("getPlans() - rootState: {} - ", root);
        List<IndexPlan> plans = new ArrayList<IndexPlan>();

        PropertyIndexLookup pil = getLookup(root);
        if (pil instanceof OrderedPropertyIndexLookup) {
            OrderedPropertyIndexLookup lookup = (OrderedPropertyIndexLookup) pil;
            for (Filter.PropertyRestriction pr : filter.getPropertyRestrictions()) {
                String propertyName = PathUtils.getName(pr.propertyName);
                if (lookup.isIndexed(propertyName, "/", filter)) {
                    PropertyValue value = null;
                    boolean createPlan = true;
                    if (pr.first == null && pr.last == null) {
                        // open query: [property] is not null
                        value = null;
                    } else if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding
                               && pr.lastIncluding) {
                        // [property]=[value]
                        value = pr.first;
                    } else if (pr.first != null && !pr.first.equals(pr.last)) {
                        // '>' & '>=' use cases
                        if (lookup.isAscending(root, propertyName, filter)) {
                            value = pr.first;
                        } else {
                            createPlan = false;
                        }
                    } else if (pr.last != null && !pr.last.equals(pr.first)){
                        // '<' & '<='
                        if (!lookup.isAscending(root, propertyName, filter)) {
                            value = pr.last;
                        } else {
                            createPlan = false;
                        }
                    }
                    if (createPlan) {
                        IndexPlan.Builder b = new IndexPlan.Builder();
                        b.setCostPerExecution(1); // we're local. Low-cost
                        // we're local but slightly more expensive than a standard PropertyIndex
                        b.setCostPerEntry(1.3);
                        b.setFulltextIndex(false); // we're never full-text
                        b.setIncludesNodeData(false); // we should not include node data
                        b.setFilter(filter);
                        // TODO it's synch for now but we should investigate the indexMeta
                        b.setDelayed(false);
                        // TODO for now but we should get this information from somewhere
                        b.setSortOrder(null);
                        // something to be delegated to IndexStoreStrategy
                        // TODO we will have to consider different use-cases of PropertyRestriction
                        // and
                        // values
                        long count = lookup.getEstimatedEntryCount(propertyName, value, filter, pr);
                        b.setEstimatedEntryCount(count);
                        LOG.debug("estimatedCount: {}", count);

                        IndexPlan plan = b.build();
                        LOG.debug("plan: {}", plan);
                        plans.add(plan);
                    }
                }
            }
        } else {
            LOG.error("Without an OrderedPropertyIndexLookup you should not end here");
        }

        return plans;
    }

    @Override
    public String getPlanDescription(IndexPlan plan) {
        LOG.debug("getPlanDescription() - plan: {}", plan);
        LOG.error("Not implemented yet");
        return null;
    }

    @Override
    public Cursor query(IndexPlan plan, NodeState rootState) {
        LOG.debug("query() - plan: {}", plan);
        LOG.debug("query() - rootState: {}", rootState);
        LOG.error("Not implemented yet");
        return null;
    }
}
