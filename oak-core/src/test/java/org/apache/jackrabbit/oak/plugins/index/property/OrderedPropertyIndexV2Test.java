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
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.AdvancedQueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry.Order;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class OrderedPropertyIndexV2Test {
    private static final EditorHook HOOK = new EditorHook(new IndexUpdateProvider(
        new OrderedPropertyIndexEditorProvider()));

    private static final String INDEXED_PROPERTY = "foo";

    private static final AdvancedQueryIndex INDEX = new OrderedPropertyIndexV2();

    private static final NodeState INDEX_DEF_V2 = EmptyNodeState.EMPTY_NODE.builder()
        .setProperty(JcrConstants.JCR_PRIMARYTYPE, IndexConstants.INDEX_DEFINITIONS_NODE_TYPE,
            Type.NAME)
        .setProperty(IndexConstants.PROPERTY_NAMES, ImmutableList.of(INDEXED_PROPERTY), Type.NAMES)
        .setProperty(IndexConstants.TYPE_PROPERTY_NAME, OrderedIndex.TYPE_2)
        .setProperty(OrderedIndex.PROPERTY_SPLIT, ImmutableList.of(3L, 3L), Type.LONGS)
        .setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true, Type.BOOLEAN).getNodeState();

    /**
     * test the returned plans. <b>Does not cover the {@code estimatedEntryCount}</b>
     * @throws CommitFailedException 
     */
    @Test
    public void getPlans() throws CommitFailedException {
        final String indexDefName = "theIndex";
        final NodeState indexDefV1 = EmptyNodeState.EMPTY_NODE.builder()
            .setProperty(JcrConstants.JCR_PRIMARYTYPE, IndexConstants.INDEX_DEFINITIONS_NODE_TYPE,
                Type.NAME)
            .setProperty(IndexConstants.PROPERTY_NAMES, ImmutableList.of(INDEXED_PROPERTY), Type.NAMES)
            .setProperty(IndexConstants.TYPE_PROPERTY_NAME, OrderedIndex.TYPE)
            .setProperty(OrderedIndex.PROPERTY_SPLIT, ImmutableList.of(3L, 3L), Type.LONGS)
            .setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true, Type.BOOLEAN).getNodeState();
        NodeBuilder root;
        NodeState indexed, before, after;
        List<QueryIndex.OrderEntry> sortOrder;
        FilterImpl filter;
        List<IndexPlan> plans;
        IndexPlan plan;
        String statement;

        plans = INDEX.getPlans(new FilterImpl(), null, EmptyNodeState.EMPTY_NODE);
        assertNotNull("it doesn't matter we should always return something", plans);

        // defining the index V1
        root = InitialContent.INITIAL_CONTENT.builder();
        root.child(INDEX_DEFINITIONS_NAME).setChildNode("theIndex", indexDefV1);

        // we have to index at least one node for having the isIndexed to work
        before = root.getNodeState();
        root.child("anode").setProperty(INDEXED_PROPERTY, "avalue");
        after = root.getNodeState();
        indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
        
        statement = "SELECT * FROM [nt:base] WHERE bazbaz IS NOT NULL ORDER BY " + INDEXED_PROPERTY;
        sortOrder = createOrderEntry(INDEXED_PROPERTY, Order.ASCENDING);
        filter = createFilter(root.getNodeState(), JcrConstants.NT_BASE, statement);
        filter.restrictProperty("bazbaz", Operator.EQUAL, null);
        plans = INDEX.getPlans(filter, sortOrder, indexed);
        assertNotNull(plans);
        assertTrue("The V1 version of the index should not apply to us", plans.isEmpty());

        // defining the index V2
        root = InitialContent.INITIAL_CONTENT.builder();
        root.child(IndexConstants.INDEX_DEFINITIONS_NAME).setChildNode(indexDefName, INDEX_DEF_V2);
        before = root.getNodeState();
        root.child("anode").setProperty(INDEXED_PROPERTY, "avalue");
        after = root.getNodeState();
        indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
                
        statement = "SELECT * FROM [nt:base] WHERE bazbaz IS NOT NULL";
        filter = createFilter(root.getNodeState(), JcrConstants.NT_BASE, statement);
        filter.restrictProperty("bazbaz", Operator.EQUAL, null);
        plans = INDEX.getPlans(filter, null, indexed);
        assertNotNull(plans);
        assertTrue("if we don't index WHERE or ORDER then empty is expected", plans.isEmpty());

        statement = "SELECT * FROM [nt:base] WHERE bazbaz IS NOT NULL ORDER BY bazbaz";
        sortOrder = createOrderEntry("bazbaz", Order.ASCENDING);
        filter = createFilter(root.getNodeState(), JcrConstants.NT_BASE, statement);
        filter.restrictProperty("bazbaz", Operator.EQUAL, null);
        plans = INDEX.getPlans(filter, sortOrder, indexed);
        assertNotNull(plans);
        assertTrue("if we don't index WHERE or ORDER then empty is expected", plans.isEmpty());

        statement = "SELECT * FROM [nt:base] WHERE bazbaz IS NOT NULL ORDER BY " + INDEXED_PROPERTY;
        sortOrder = createOrderEntry(INDEXED_PROPERTY, Order.ASCENDING);
        filter = createFilter(root.getNodeState(), JcrConstants.NT_BASE, statement);
        filter.restrictProperty("bazbaz", Operator.EQUAL, null);
        plans = INDEX.getPlans(filter, sortOrder, indexed);
        assertNotNull(plans);
        assertEquals(1, plans.size());
        plan = plans.get(0);
        assertNotNull(plan);
        sortOrder = plan.getSortOrder(); 
        assertNotNull(sortOrder);
        assertEquals(2, plan.getSortOrder().size());
        assertTrue(sortOrder.containsAll(createOrderEntry(INDEXED_PROPERTY, Order.ASCENDING)));
        assertTrue(sortOrder.containsAll(createOrderEntry(INDEXED_PROPERTY, Order.DESCENDING)));

        statement = String.format(
             "SELECT * FROM [nt:base] WHERE %s IS NOT NULL",
             INDEXED_PROPERTY);
        sortOrder = null;
        filter = createFilter(root.getNodeState(), JcrConstants.NT_BASE, statement);
        filter.restrictProperty(INDEXED_PROPERTY, Operator.EQUAL, null);
        plans = INDEX.getPlans(filter, sortOrder, indexed);
        assertNotNull(plans);
        assertEquals(1, plans.size());
        plan = plans.get(0);
        assertNotNull(plan);
        sortOrder = plan.getSortOrder(); 
        assertNull(sortOrder);

        statement = String.format("SELECT * FROM [nt:base] WHERE %s IS NOT NULL ORDER BY %s",
            INDEXED_PROPERTY, INDEXED_PROPERTY);
        sortOrder = createOrderEntry(INDEXED_PROPERTY, Order.ASCENDING);
        filter = createFilter(root.getNodeState(), JcrConstants.NT_BASE, statement);
        filter.restrictProperty(INDEXED_PROPERTY, Operator.EQUAL, null);
        plans = INDEX.getPlans(filter, sortOrder, indexed);
        assertNotNull(plans);
        assertEquals(2, plans.size());
        // we have two plans. One with for the sole order and another one for the filte and the
        // order combined. They will differentiate (if any) in costs
        plan = plans.get(0);
        assertNotNull(plan);
        sortOrder = plan.getSortOrder();
        assertNotNull(sortOrder);
        assertTrue(sortOrder.containsAll(createOrderEntry(INDEXED_PROPERTY, Order.ASCENDING)));
        assertTrue(sortOrder.containsAll(createOrderEntry(INDEXED_PROPERTY, Order.DESCENDING)));
        plan = plans.get(1);
        assertNotNull(plan);
        sortOrder = plan.getSortOrder();
        assertNotNull(sortOrder);
        assertTrue(sortOrder.containsAll(createOrderEntry(INDEXED_PROPERTY, Order.ASCENDING)));
        assertTrue(sortOrder.containsAll(createOrderEntry(INDEXED_PROPERTY, Order.DESCENDING)));
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
