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

import static org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider.TYPE;
import static org.apache.jackrabbit.oak.plugins.index.property.PropertyIndex.encode;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.ContentMirrorStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.UniqueEntryStoreStrategy;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Is responsible for querying the property index content.
 * <br>
 * This class can be used directly on a subtree where there is an index defined
 * by supplying a {@link NodeState} root.
 * 
 * <pre>
 * <code>
 * {
 *     NodeState state = ... // get a node state
 *     PropertyIndexLookup lookup = new PropertyIndexLookup(state);
 *     Set<String> hits = lookup.find("foo", PropertyValues.newString("xyz"));
 * }
 * </code>
 * </pre>
 */
public class PropertyIndexLookup extends AbstractPropertyIndexLookup {

    /**
     * The cost overhead to use the index in number of read operations.
     */
    private static final double COST_OVERHEAD = 2;
    
    /** Index storage strategy */
    private static final IndexStoreStrategy MIRROR =
            new ContentMirrorStoreStrategy();

    /** Index storage strategy */
    private static final IndexStoreStrategy UNIQUE =
            new UniqueEntryStoreStrategy();

    final NodeState root;

    public PropertyIndexLookup(NodeState root) {
        this.root = root;
    }

    public Iterable<String> query(Filter filter, String propertyName, PropertyValue value) {
        NodeState indexMeta = getIndexNode(root, propertyName, filter);
        if (indexMeta == null) {
            throw new IllegalArgumentException("No index for " + propertyName);
        }
        return getStrategy(indexMeta).query(filter, propertyName, indexMeta, encode(value));
    }

    IndexStoreStrategy getStrategy(NodeState indexMeta) {
        if (indexMeta.getBoolean(IndexConstants.UNIQUE_PROPERTY_NAME)) {
            return UNIQUE;
        }
        return MIRROR;
    }

    public double getCost(Filter filter, String propertyName, PropertyValue value) {
        NodeState indexMeta = getIndexNode(root, propertyName, filter);
        if (indexMeta == null) {
            return Double.POSITIVE_INFINITY;
        }
        return COST_OVERHEAD +
                getStrategy(indexMeta).count(indexMeta, encode(value), MAX_COST);
    }

    @Override
    String getType() {
        return TYPE;
    }

    @Override
    NodeState getRoot() {
        return root;
    }

    @Override
    public long getEstimatedEntryCount(NodeState indexMeta, PropertyRestriction pr) {
        throw new UnsupportedOperationException(); 
    }
}