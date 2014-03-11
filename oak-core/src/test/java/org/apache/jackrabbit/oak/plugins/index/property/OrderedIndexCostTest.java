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

import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.easymock.EasyMock;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * tests the Cost-related part of the provider/strategy
 */
public class OrderedIndexCostTest extends BasicOrderedPropertyIndexQueryTest {
    /**
     * convenience class that return an always indexed strategy
     */
    private static class AlwaysIndexedOrderedPropertyIndex extends OrderedPropertyIndex {

        @Override
        PropertyIndexLookup getLookup(NodeState root) {
            return new AlwaysIndexedLookup(root);
        }

        /**
         * convenience class that always return true at the isIndexed test
         */
        private static class AlwaysIndexedLookup extends OrderedPropertyIndexLookup {
            public AlwaysIndexedLookup(NodeState root) {
                super(root);
            }

            @Override
            public boolean isIndexed(String propertyName, String path, Filter filter) {
                return true;
            }
        }
      }

    @Test 
    public void costFullTextConstraint() {
        OrderedPropertyIndex index = new OrderedPropertyIndex();
        NodeState root = InitialContent.INITIAL_CONTENT;
        Filter filter = EasyMock.createNiceMock(Filter.class);
        FullTextExpression fte = EasyMock.createNiceMock(FullTextExpression.class);
        EasyMock.expect(filter.getFullTextConstraint()).andReturn(fte).anyTimes();
        EasyMock.replay(fte);
        EasyMock.replay(filter);

        assertEquals("if it contains FullText we don't serve", Double.POSITIVE_INFINITY,
            index.getCost(filter, root), 0);
    }
  
    @Test
    public void costContainsNativeConstraints(){
        OrderedPropertyIndex index = new OrderedPropertyIndex();
        NodeState root = InitialContent.INITIAL_CONTENT;
        Filter filter = EasyMock.createNiceMock(Filter.class);
        EasyMock.expect(filter.containsNativeConstraint()).andReturn(true).anyTimes();
        EasyMock.replay(filter);

        assertEquals("If it contains Natives we don't serve", Double.POSITIVE_INFINITY,
            index.getCost(filter, root), 0);
    }

    /**
     * tests the use-case where we ask for '>' of a date.
     * 
     * As we're not testing the actual algorithm, part of {@code IndexLookup} we want to make sure
     * the Index doesn't reply with "dont' serve" in special cases
     */
    @Test
    public void costGreaterThanAscendingDirection() throws Exception {
        OrderedPropertyIndex index = new AlwaysIndexedOrderedPropertyIndex();
        NodeState root = InitialContent.INITIAL_CONTENT;
        Filter filter = createNiceMock(Filter.class);
        Filter.PropertyRestriction restriction = new Filter.PropertyRestriction();
        restriction.first = PropertyValues.newDate("2013-01-01");
        expect(filter.getPropertyRestrictions()).andReturn(ImmutableList.of(restriction))
            .anyTimes();
        expect(filter.containsNativeConstraint()).andReturn(false).anyTimes();
        replay(filter);

        assertFalse("In ascending order we're expeting to serve this kind of queries",
            Double.POSITIVE_INFINITY == index.getCost(filter, root));
    }

 // =================================================
 // =================================================
 //  SOME RUBBISH TO BE REUSED MAYBE LATER ON
 // =================================================
 // =================================================
//    /**
//     * used for easing access internal aspects of the index
//     */
//    private static class MockOrderedPropertyIndexProvider extends OrderedPropertyIndexProvider {
//        private static final OrderedPropertyIndex OPI = new OrderedPropertyIndex();
//
//        @Override
//        public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
//            return ImmutableList.<QueryIndex> of(OPI);
//        }
//        
//        public OrderedPropertyIndex getOpi(){
//            return OPI;
//        }
//    }
    
    
//    /**
//     * used for accessing internal aspects of the implementation while running tests
//     */
//    private static final MockOrderedPropertyIndexProvider PROVIDER = new MockOrderedPropertyIndexProvider();
    
//    @Override
//    protected ContentRepository createRepository() {
//        return new Oak().with(new InitialContent())
//            .with(new OpenSecurityProvider())
//            .with(PROVIDER)
//            .with(new OrderedPropertyIndexEditorProvider())
//            .createContentRepository();
//    }

//    private static final EditorHook HOOK = new EditorHook(
//        new IndexUpdateProvider(new OrderedPropertyIndexEditorProvider()));
    
//    @Override
//    protected void createTestIndexNode() throws Exception {
//        // intentionally left blank. Each test will have to define its own index configuration
//    }
    
//    private void defineAscendingIndex(NodeBuilder oakIndexNode) throws Exception {
//        Tree index = root.getTree("/");
//        IndexUtils.createIndexDefinition(
//            new NodeUtil(index.getChild(IndexConstants.INDEX_DEFINITIONS_NAME)),
//            TEST_INDEX_NAME, 
//            false, 
//            new String[] { ORDERED_PROPERTY }, 
//            null, 
//            OrderedIndex.TYPE,
//            ImmutableMap.of(
//                OrderedIndex.DIRECTION, OrderedIndex.OrderDirection.ASC.getDirection()
//            )
//        );
//        root.commit();
//    }

//    @Test
//    public void costGreaterThanAscendingIndex() {
//        OrderedPropertyIndex index = new OrderedPropertyIndex();
//        NodeState root = InitialContent.INITIAL_CONTENT;
//        Filter filter = EasyMock.createNiceMock(Filter.class);
//
//        assertFalse("Ascending index and '>' queries should be considered for serving the query",
//            index.getCost(filter, root) == Double.POSITIVE_INFINITY);
//    }
    
    
//    /**
//     * tests the use-case where we ask for '>' of a date
//     */
//    @Test
//    public void costGreaterThanAscendingDirection() throws Exception {
//        NodeState root = InitialContent.INITIAL_CONTENT;
//        NodeBuilder builder = root.builder();
//        NodeBuilder test = builder.child("test");
//        
//        defineAscendingIndex(builder.child(IndexConstants.INDEX_DEFINITIONS_NAME));
//        
//        // generating initial content
//        NodeState before = builder.getNodeState();
//        Calendar start = Calendar.getInstance();
//        start.set(2013, Calendar.JANUARY, 1);
//        addChildNodes(
//            generateOrderedDates(NUMBER_OF_NODES, OrderDirection.ASC, start), 
//            test,
//            Type.DATE);
//        
//        NodeState after = builder.getNodeState();
//        
//        //processing commit through the CommitHook
//        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);
//
//    }
//    
//    private static FilterImpl createFilter(NodeState root, String nodeTypeName) {
//        NodeState system = root.getChildNode(JCR_SYSTEM);
//        NodeState types = system.getChildNode(JCR_NODE_TYPES);
//        NodeState type = types.getChildNode(nodeTypeName);
//        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
//        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]");
//    }

}
