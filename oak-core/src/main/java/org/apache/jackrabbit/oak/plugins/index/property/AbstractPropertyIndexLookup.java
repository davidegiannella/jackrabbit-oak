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

import static com.google.common.collect.Iterables.contains;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;

import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public abstract class AbstractPropertyIndexLookup {
    /**
     * retrieve the {@code root} from the instantiated class
     * 
     * @return
     */
    abstract NodeState getRoot();
    
    /**
     * retrieve the type of the index
     * 
     * @return the type
     */
    abstract String getType();
    
    /**
     * retrieve the estimated entry count for the current filter
     * 
     * @param propertyName
     * @param value
     * @param filter
     * @param pr
     * @return
     */
    public abstract long getEstimatedEntryCount(@Nonnull final String propertyName, 
                                                @Nonnull final PropertyValue value,
                                                @Nonnull final Filter filter, 
                                                @Nonnull final PropertyRestriction pr);
    
    static Set<String> getSuperTypes(Filter filter) {
        if (filter != null && !filter.matchesAllTypes()) {
            return filter.getSupertypes();
        }
        return null;
    }

    /**
     * Checks whether the named property is indexed somewhere along the given
     * path. Lookup starts at the current path (at the root of this object) and
     * traverses down the path.
     * 
     * @param propertyName property name
     * @param path lookup path
     * @param filter for the node type restriction (null if no node type restriction)
     * @return true if the property is indexed
     */
    public boolean isIndexed(String propertyName, String path, Filter filter) {
        NodeState root = getRoot();
        if (PathUtils.denotesRoot(path)) {
            return getIndexNode(root, propertyName, filter) != null;
        }
    
        NodeState node = root;
        for (String s : PathUtils.elements(path)) {
            if (getIndexNode(node, propertyName, filter) != null) {
                return true;
            }
            node = node.getChildNode(s);
        }
        return false;
    }

    /**
     * Get the node with the index definition for the given property, if there
     * is an applicable index with data.
     * 
     * @param propertyName the property name
     * @param filter the filter (which contains information of all supertypes,
     *            unless the filter matches all types)
     * @return the node where the index definition (metadata) is stored (the
     *         parent of ":index"), or null if no index definition or index data
     *         node was found
     */
    @Nullable
    protected NodeState getIndexNode(NodeState node, String propertyName, Filter filter) {
        // keep a fallback to a matching index def that has *no* node type constraints
        // (initially, there is no fallback)
        NodeState fallback = null;
    
        NodeState state = node.getChildNode(INDEX_DEFINITIONS_NAME);
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            NodeState index = entry.getNodeState();
            PropertyState type = index.getProperty(TYPE_PROPERTY_NAME);
            if (type == null || type.isArray() || !getType().equals(type.getValue(Type.STRING))) {
                continue;
            }
            if (contains(index.getNames(PROPERTY_NAMES), propertyName)) {
                NodeState indexContent = index.getChildNode(INDEX_CONTENT_NODE_NAME);
                if (!indexContent.exists()) {
                    continue;
                }
                Set<String> supertypes = getSuperTypes(filter);
                if (index.hasProperty(DECLARING_NODE_TYPES)) {
                    if (supertypes != null) {
                        for (String typeName : index.getNames(DECLARING_NODE_TYPES)) {
                            if (supertypes.contains(typeName)) {
                                // TODO: prefer the most specific type restriction
                                return index;
                            }
                        }
                    }
                } else if (supertypes == null) {
                    return index;
                } else if (fallback == null) {
                    // update the fallback
                    fallback = index;
                }
            }
        }
        return fallback;
    }
}
