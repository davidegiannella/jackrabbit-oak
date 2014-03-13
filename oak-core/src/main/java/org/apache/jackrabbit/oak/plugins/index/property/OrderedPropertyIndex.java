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

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Cursors;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A property index that supports ordering keys.
 */
public class OrderedPropertyIndex extends PropertyIndex {
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
}
