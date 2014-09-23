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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection.ASC;
import static org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection.DESC;
import static org.apache.jackrabbit.oak.plugins.index.property.strategy.OrderedContentMirrorStoreStrategy.START;
import static org.apache.jackrabbit.oak.plugins.index.property.strategy.OrderedContentMirrorStoreStrategy.getPropertyNext;
import static org.apache.jackrabbit.oak.plugins.index.property.strategy.OrderedContentMirrorStoreStrategy.setPropertyNext;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import javax.annotation.Nonnull;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.RepositoryException;
import javax.jcr.query.Row;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.OrderedContentMirrorStoreStrategy;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

public class Oak2077QueriesTest extends BasicOrderedPropertyIndexQueryTest {
    private static final Logger LOG = LoggerFactory.getLogger(Oak2077QueriesTest.class);
    private NodeStore nodestore;
    private ContentRepository repository;
    
    // ------------------------------------------------------------------------ < utility classes >
    /**
     * used to return an instance of IndexEditor with a defined Random for a better reproducible
     * unit testing
     */
    private class SeededOrderedPropertyIndexEditorProvider extends OrderedPropertyIndexEditorProvider {
        private Random rnd = new Random(1);
        
        @Override
        public Editor getIndexEditor(String type, NodeBuilder definition, NodeState root,
                                     IndexUpdateCallback callback) throws CommitFailedException {
            Editor editor = (TYPE.equals(type)) ? new SeededPropertyIndexEditor(definition, root,
                callback, rnd) : null;
            return editor; 
        }
    }

    /**
     * index editor that will return a content strategy with 
     */
    private class SeededPropertyIndexEditor extends OrderedPropertyIndexEditor {
        private Random rnd;
        
        public SeededPropertyIndexEditor(NodeBuilder definition, NodeState root,
                                         IndexUpdateCallback callback, Random rnd) {
            super(definition, root, callback);
            this.rnd = rnd;
        }

        public SeededPropertyIndexEditor(SeededPropertyIndexEditor parent, String name) {
            super(parent, name);
            this.rnd = parent.rnd;
        }

        @Override
        IndexStoreStrategy getStrategy(boolean unique) {
            SeededOrderedMirrorStore store = new SeededOrderedMirrorStore();
            if (!OrderedIndex.DEFAULT_DIRECTION.equals(getDirection())) {
                store = new SeededOrderedMirrorStore(DESC);
            }
            store.setRandom(rnd);
            return store;
        }

        @Override
        PropertyIndexEditor getChildIndexEditor(PropertyIndexEditor parent, String name) {
            return new SeededPropertyIndexEditor(this, name);
        }
    }
    
    /**
     * mocking class that makes use of the provided {@link Random} instance for generating the lanes
     */
    private class SeededOrderedMirrorStore extends OrderedContentMirrorStoreStrategy {
        private Random rnd = new Random();
        
        public SeededOrderedMirrorStore() {
            super();
        }

        public SeededOrderedMirrorStore(OrderDirection direction) {
            super(direction);
        }

        @Override
        public int getLane() {
            return getLane(rnd);
        }
        
        public void setRandom(Random rnd) {
            this.rnd = rnd;
        }
    }

    // ---------------------------------------------------------------------------------- < tests >
    @Override
    protected ContentRepository createRepository() {
        nodestore = new MemoryNodeStore();
        repository = new Oak(nodestore).with(new InitialContent())
            .with(new OpenSecurityProvider())
            .with(new SeededOrderedPropertyIndexEditorProvider())
            .with(new OrderedPropertyIndexProvider())
            .createContentRepository(); 
        return repository;
    }

    @Override
    protected void createTestIndexNode() throws Exception {
        // leaving it empty. Prefer to create the index definition in each method
    }
    
    private void defineIndex(@Nonnull final OrderDirection direction) 
                            throws IllegalArgumentException, RepositoryException, CommitFailedException {
        checkNotNull(direction);
        
        Tree index = root.getTree("/");
        IndexUtils.createIndexDefinition(
            new NodeUtil(index.getChild(INDEX_DEFINITIONS_NAME)),
            TEST_INDEX_NAME,
            false,
            new String[] { ORDERED_PROPERTY },
            null,
            OrderedIndex.TYPE,
            ImmutableMap.of(
                OrderedIndex.DIRECTION, direction.getDirection()
            )
        );
        root.commit();
    }

    /**
     * <p>
     * reset the environment variables to be sure to use the latest root. {@code session, root, qe}
     * <p>
     * 
     * @throws IOException
     * @throws LoginException
     * @throws NoSuchWorkspaceException
     */
    private void resetEnvVariables() throws IOException, LoginException, NoSuchWorkspaceException {
        session.close();
        session = repository.login(null, null);
        root = session.getLatestRoot();
        qe = root.getQueryEngine();
    }
    
    @Test
    public void queryNotNullAscending() throws Exception {
        setTraversalEnabled(false);
        // with 1k nodes we're sure to have all the 4 lanes due to probability
        final int numberOfNodes = 10;
        final OrderDirection direction = ASC;
        final String unexistent  = formatNumber(numberOfNodes + 1);
        final String statement = "SELECT * FROM [nt:base] WHERE " + ORDERED_PROPERTY
                                 + " IS NOT NULL";
        defineIndex(direction);
        
        Tree content = root.getTree("/").addChild("content").addChild("nodes");
        List<String> values = generateOrderedValues(numberOfNodes, direction);
        List<ValuePathTuple> nodes = addChildNodes(values, content, direction, STRING);
        root.commit();
        
//        // truncating the list on lane 0
//        NodeBuilder rootBuilder = nodestore.getRoot().builder();
//        NodeBuilder builder = rootBuilder.getChildNode(INDEX_DEFINITIONS_NAME);
//        builder = builder.getChildNode(TEST_INDEX_NAME);
//        builder = builder.getChildNode(INDEX_CONTENT_NODE_NAME);
//        
//        NodeBuilder truncated = builder.getChildNode(START);
//        String truncatedName;
//        
//        for (int i = 0; i < 4; i++) {
//            // changing the 4th element. No particular reasons on why the 4th.
//            truncatedName = getPropertyNext(truncated);
//            truncated = builder.getChildNode(truncatedName);
//        }
//        
//        setPropertyNext(truncated, unexistent, 0);
//        
//        // re-basing should be fine as we're the only one changing the data
//        nodestore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
//        
//        resetEnvVariables();
        
        // pointing to a non-existent node in lane 0 we expect the result to be truncated
        Result result = executeQuery(statement, SQL2, null);
        assertRightOrder(nodes, result.getRows().iterator());
//        for (ResultRow row : result.getRows()) {
//            LOG.debug("{}", row.getTree(null));
//        }
        
        // TODO perform a query
        // TODO ensure result ends at the right point
//        fail("complete the test");
        setTraversalEnabled(true);
    }
    
    @Test @Ignore
    public void queryNotNullDescending() {
        fail();
    }
    
    @Test @Ignore
    public void queryEqualsAscending() {
        fail();
    }

    @Test @Ignore
    public void queryEqualsDescending() {
        fail();
    }

    @Test @Ignore
    public void queryGreaterThanAscending() {
        fail();
    }
    
    @Test @Ignore
    public void queryGreaterThenDescending() {
        fail();
    }
    
    @Test @Ignore
    public void queryGreaterThanEqualAscending() {
        fail();
    }
    
    @Test @Ignore
    public void queryGreaterThanEqualDescending() {
        fail();
    }
    
    @Test @Ignore
    public void queryLessThanAscending() {
        fail();
    }
    
    @Test @Ignore
    public void queryLessThanDescending() {
        fail();
    }
    
    @Test @Ignore
    public void queryLessThanEqualAscending() {
        fail();
    }
    
    @Test @Ignore
    public void queryLessThanEqualDescending() {
        fail();
    }
    
    @Test @Ignore
    public void queryBetweenAscending() {
        fail();
    }
    
    @Test @Ignore
    public void queryBetweenDescending() {
        fail();
    }
    
    @Test @Ignore
    public void queryBetweenWithBoundariesAscending() {
        fail();
    }

    @Test @Ignore
    public void queryBetweenWithBoundariesDescending() {
        fail();
    }
}
