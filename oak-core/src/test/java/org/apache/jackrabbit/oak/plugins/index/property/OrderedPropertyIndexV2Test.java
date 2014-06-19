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

import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.AdvancedQueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry.Order;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class OrderedPropertyIndexV2Test {
    
    /**
     * test the returned plans. <b>Does not cover the {@code estimatedEntryCount}</b>
     */
    @Test @Ignore("Still WIP")
    public void getPlans() {
        final String indexedProperty = "foo";
        final AdvancedQueryIndex index = new OrderedPropertyIndexV2();
        final NodeState indexDefV1 = EmptyNodeState.EMPTY_NODE.builder()
            .setProperty(JcrConstants.JCR_PRIMARYTYPE, IndexConstants.INDEX_DEFINITIONS_NODE_TYPE,
                Type.NAME)
            .setProperty(IndexConstants.PROPERTY_NAMES, ImmutableList.of(indexedProperty), Type.NAMES)
            .setProperty(IndexConstants.TYPE_PROPERTY_NAME, OrderedIndex.TYPE)
            .setProperty(OrderedIndex.PROPERTY_SPLIT, ImmutableList.of(3L, 3L), Type.LONGS)
            .setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true, Type.BOOLEAN).getNodeState();
        final NodeState indexDefV2 = EmptyNodeState.EMPTY_NODE.builder()
            .setProperty(JcrConstants.JCR_PRIMARYTYPE, IndexConstants.INDEX_DEFINITIONS_NODE_TYPE,
                Type.NAME)
            .setProperty(IndexConstants.PROPERTY_NAMES, ImmutableList.of(indexedProperty), Type.NAMES)
            .setProperty(IndexConstants.TYPE_PROPERTY_NAME, OrderedIndex.TYPE)
            .setProperty(OrderedIndex.PROPERTY_VERSION, OrderedIndex.Version.V2.toString())
            .setProperty(OrderedIndex.PROPERTY_SPLIT, ImmutableList.of(3L, 3L), Type.LONGS)
            .setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true, Type.BOOLEAN).getNodeState();
        NodeBuilder root;
        List<QueryIndex.OrderEntry> sortOrder;
        FilterImpl filter;
        List<IndexPlan> plans;
        IndexPlan plan;
        String statement;

        plans = index.getPlans(new FilterImpl(), null, EmptyNodeState.EMPTY_NODE);
        assertNotNull("it doesn't matter we should always return something", plans);

        // defining the index V1
        root = InitialContent.INITIAL_CONTENT.builder();
        root.child(IndexConstants.INDEX_DEFINITIONS_NAME).setChildNode("theIndex", indexDefV1);

        statement = "SELECT * FROM [nt:base] WHERE bazbaz IS NOT NULL ORDER BY " + indexedProperty;
        sortOrder = createOrderEntry(indexedProperty, Order.ASCENDING);
        filter = createFilter(root.getNodeState(), JcrConstants.NT_BASE, statement);
        filter.restrictProperty("bazbaz", Operator.EQUAL, null);
        plans = index.getPlans(filter, sortOrder, root.getNodeState());
        assertNotNull(plans);
        assertTrue("The V1 version of the index should not apply to us", plans.isEmpty());

        // defining the index V2
        root = InitialContent.INITIAL_CONTENT.builder();
        root.child(IndexConstants.INDEX_DEFINITIONS_NAME).setChildNode("theIndex", indexDefV2);
        
        statement = "SELECT * FROM [nt:base] WHERE bazbaz IS NOT NULL";
        filter = createFilter(root.getNodeState(), JcrConstants.NT_BASE, statement);
        filter.restrictProperty("bazbaz", Operator.EQUAL, null);
        plans = index.getPlans(filter, null, root.getNodeState());
        assertNotNull(plans);
        assertTrue("if we don't index WHERE or ORDER then empty is expected", plans.isEmpty());

        statement = "SELECT * FROM [nt:base] WHERE bazbaz IS NOT NULL ORDER BY bazbaz";
        sortOrder = createOrderEntry("bazbaz", Order.ASCENDING);
        filter = createFilter(root.getNodeState(), JcrConstants.NT_BASE, statement);
        filter.restrictProperty("bazbaz", Operator.EQUAL, null);
        plans = index.getPlans(filter, sortOrder, root.getNodeState());
        assertNotNull(plans);
        assertTrue("if we don't index WHERE or ORDER then empty is expected", plans.isEmpty());

        statement = "SELECT * FROM [nt:base] WHERE bazbaz IS NOT NULL ORDER BY " + indexedProperty;
        sortOrder = createOrderEntry(indexedProperty, Order.ASCENDING);
        filter = createFilter(root.getNodeState(), JcrConstants.NT_BASE, statement);
        filter.restrictProperty("bazbaz", Operator.EQUAL, null);
        plans = index.getPlans(filter, sortOrder, root.getNodeState());
        assertNotNull(plans);
        assertEquals(1, plans.size());
        plan = plans.get(0);
        assertNotNull(plan);
    }
    
    private static FilterImpl createFilter(NodeState indexed, String nodeTypeName, String statement) {
        NodeState system = indexed.getChildNode(JCR_SYSTEM);
        NodeState types = system.getChildNode(JCR_NODE_TYPES);
        NodeState type = types.getChildNode(nodeTypeName);
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        String st = (statement == null) ? "SELECT * FROM [" + nodeTypeName + "]" : statement;
        return new FilterImpl(selector, st, new QueryEngineSettings());
    }

    private static List<QueryIndex.OrderEntry> createOrderEntry(String property,
                                                          QueryIndex.OrderEntry.Order order) {
        return ImmutableList.of(new QueryIndex.OrderEntry(property, Type.UNDEFINED, order));
    }

    /**
     * keep tracks of what is not supported
     */
    @Test
    public void unsupportedOperations() {
        QueryIndex qi = new OrderedPropertyIndexV2();
        
        try {
            qi.getCost(null, null);
            fail("It should have risen an exception as it's not supported");
        } catch (UnsupportedOperationException e) {
            // all good if here
        }
        try {
            qi.getPlan(null, null);
            fail("It should have risen an exception as it's not supported");
        } catch (UnsupportedOperationException e) {
            // all good if here
        }
        try {
            qi.query(null, null);
            fail("It should have risen an exception as it's not supported");
        } catch (UnsupportedOperationException e) {
            // all good if here
        }
    }
}
