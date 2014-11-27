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
import static org.apache.jackrabbit.oak.spi.query.PropertyValues.newString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.annotation.Nonnull;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.RepositoryException;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.OrderedContentMirrorStorageStrategyTest;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.OrderedContentMirrorStoreStrategy;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.OutputStreamAppender;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class Oak2077QueriesTest extends BasicOrderedPropertyIndexQueryTest {
    private static final LoggingTracker<ILoggingEvent> LOGGING_TRACKER;
    private static final Logger LOG = LoggerFactory.getLogger(Oak2077QueriesTest.class);
    private NodeStore nodestore;
    private ContentRepository repository;

    static {

        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        
        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(lc);
        encoder.setPattern("%msg%n");
        encoder.start();
        
        LOGGING_TRACKER = new LoggingTracker<ILoggingEvent>();
        LOGGING_TRACKER.setContext(lc);
        LOGGING_TRACKER.setEncoder(encoder);
        LOGGING_TRACKER.start();

        // adding the new appender to the root logger
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME))
            .addAppender(LOGGING_TRACKER);

        //configuring the logging level to desired value
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(LOGGING_TRACKER.getName()))
            .setLevel(Level.WARN);
    }
    
    // ------------------------------------------------------------------------ < utility classes >
    private static class LoggingTracker<E> extends OutputStreamAppender<E> {
        private ByteArrayOutputStream baos = new ByteArrayOutputStream();
        
        @Override
        public void start() {
            setOutputStream(baos);
            super.start();
        }
        
        /**
         * reset the inner OutputStream. 
         */
        public void reset() {
            baos.reset();
        }
        
        public BufferedReader toBufferedReader() {
            return new BufferedReader(new StringReader(baos.toString()));
        }
        
        public int countLinesTracked() throws IOException {
            int lines = 0;
            BufferedReader br = toBufferedReader();
            while (br.readLine() != null) {
                lines++;
            }
            return lines;
        }

        @Override
        public String getName() {
            return LoggingTracker.class.getName();
        }
    }
    
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
        
        // removing any previously defined index definition for a complete reset
        index = index.getChild(INDEX_DEFINITIONS_NAME);
        if (index.exists()) {
            index = index.getChild(TEST_INDEX_NAME);
            if (index.exists()) {
                index.remove();
            }
        }
        index = root.getTree("/");
        
        // ensuring we have a clear reset of the environment
        assertFalse("the index definition should not be here yet",
            index.getChild(INDEX_DEFINITIONS_NAME).getChild(TEST_INDEX_NAME).exists());
        
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
        final int numberOfNodes = 20;
        final OrderDirection direction = ASC;
        final String unexistent  = formatNumber(numberOfNodes + 1);
        final String statement = "SELECT * FROM [nt:base] WHERE " + ORDERED_PROPERTY
                                 + " IS NOT NULL";
        defineIndex(direction);
        
        Tree content = root.getTree("/").addChild("content").addChild("nodes");
        List<String> values = generateOrderedValues(numberOfNodes, direction);
        List<ValuePathTuple> nodes = addChildNodes(values, content, direction, STRING);
        root.commit();
        
        // truncating the list on lane 0
        NodeBuilder rootBuilder = nodestore.getRoot().builder();
        NodeBuilder builder = rootBuilder.getChildNode(INDEX_DEFINITIONS_NAME);
        builder = builder.getChildNode(TEST_INDEX_NAME);
        builder = builder.getChildNode(INDEX_CONTENT_NODE_NAME);
        
        NodeBuilder truncated = builder.getChildNode(START);
        String truncatedName;
        
        for (int i = 0; i < 4; i++) {
            // changing the 4th element. No particular reasons on why the 4th.
            truncatedName = getPropertyNext(truncated);
            truncated = builder.getChildNode(truncatedName);
        }
        setPropertyNext(truncated, unexistent, 0);
        
        nodestore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        resetEnvVariables();
        
        //filtering out the part that should not be returned by the resultset.
        List<ValuePathTuple> expected = Lists.newArrayList(Iterables.filter(nodes,
            new Predicate<ValuePathTuple>() {
                boolean stopHere;

                @Override
                public boolean apply(ValuePathTuple input) {
                    if (!stopHere) {
                        stopHere = unexistent.equals(input.getValue());
                    }
                    return !stopHere;
                }
            }));
        
        // pointing to a non-existent node in lane 0 we expect the result to be truncated
        LOGGING_TRACKER.reset();
        Result result = executeQuery(statement, SQL2, null);
        assertRightOrder(expected, result.getRows().iterator());
        assertEquals("We expect 1 warning message to be tracked", 1, LOGGING_TRACKER.countLinesTracked());
        // as the full iterable used in `property IS NOT NULL` cases walk the index on lane 0, any
        // other lanes doesn't matter.
        
        setTraversalEnabled(true);
    }
    
    @Test
    public void queryNotNullDescending() throws Exception {
        setTraversalEnabled(false);
        final int numberOfNodes = 20;
        final OrderDirection direction = DESC; //changed
        final String unexistent  = formatNumber(0); //changed
        final String statement = "SELECT * FROM [nt:base] WHERE " + ORDERED_PROPERTY
                                 + " IS NOT NULL";
        defineIndex(direction);
        
        Tree content = root.getTree("/").addChild("content").addChild("nodes");
        List<String> values = generateOrderedValues(numberOfNodes, 1, direction); //changed by offset
        List<ValuePathTuple> nodes = addChildNodes(values, content, direction, STRING);
        root.commit();
        
        // truncating the list on lane 0
        NodeBuilder rootBuilder = nodestore.getRoot().builder();
        NodeBuilder builder = rootBuilder.getChildNode(INDEX_DEFINITIONS_NAME);
        builder = builder.getChildNode(TEST_INDEX_NAME);
        builder = builder.getChildNode(INDEX_CONTENT_NODE_NAME);
        
        NodeBuilder truncated = builder.getChildNode(START);
        String truncatedName;
        
        for (int i = 0; i < 4; i++) {
            // changing the 4th element. No particular reasons on why the 4th.
            truncatedName = getPropertyNext(truncated);
            truncated = builder.getChildNode(truncatedName);
        }
        setPropertyNext(truncated, unexistent, 0);
        
        nodestore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        resetEnvVariables();
        
        //filtering out the part that should not be returned by the resultset.
        List<ValuePathTuple> expected = Lists.newArrayList(Iterables.filter(nodes,
            new Predicate<ValuePathTuple>() {
                boolean stopHere;

                @Override
                public boolean apply(ValuePathTuple input) {
                    if (!stopHere) {
                        stopHere = unexistent.equals(input.getValue());
                    }
                    return !stopHere;
                }
            }));
        
        // pointing to a non-existent node in lane 0 we expect the result to be truncated
        LOGGING_TRACKER.reset();
        Result result = executeQuery(statement, SQL2, null);
        assertRightOrder(expected, result.getRows().iterator());
        assertEquals("We expect 1 warning message to be tracked", 1, LOGGING_TRACKER.countLinesTracked());

        // as the full iterable used in `property IS NOT NULL` cases walk the index on lane 0, any
        // other lanes doesn't matter.
        
        setTraversalEnabled(true);
    }

    // As of OAK-2202 we don't use the skip list for returning a specific key item, so we're not
    // affected by OAK-2077
    // public void queryEqualsAscending() throws Exception {
    // }
    // public void queryEqualsDescending() {
    // }

    @Test
    public void queryGreaterThanAscending() throws Exception {
        setTraversalEnabled(false);
        final int numberOfNodes = 20;
        final OrderDirection direction = ASC;
        final String unexistent  = formatNumber(numberOfNodes + 1);
        // as 'values' will start from 0, we're excluding first entry(ies)
        String whereCondition = formatNumber(1);
        final String statement = "SELECT * FROM [nt:base] WHERE " + ORDERED_PROPERTY
                                 + " > '%s'";
        defineIndex(direction);
        
        Tree content = root.getTree("/").addChild("content").addChild("nodes");
        List<String> values = generateOrderedValues(numberOfNodes, direction);
        List<ValuePathTuple> nodes = addChildNodes(values, content, direction, STRING);
        root.commit();
        
        // truncating the list on lane 0
        NodeBuilder rootBuilder = nodestore.getRoot().builder();
        NodeBuilder builder = rootBuilder.getChildNode(INDEX_DEFINITIONS_NAME);
        builder = builder.getChildNode(TEST_INDEX_NAME);
        builder = builder.getChildNode(INDEX_CONTENT_NODE_NAME);
        
        NodeBuilder truncated = builder.getChildNode(START);
        String truncatedName;
        
        OrderedContentMirrorStorageStrategyTest.printSkipList(builder.getNodeState());
        
        for (int i = 0; i < 4; i++) {
            // changing the 4th element. No particular reasons on why the 4th.
            truncatedName = getPropertyNext(truncated);
            truncated = builder.getChildNode(truncatedName);
        }
        setPropertyNext(truncated, unexistent, 0);
        
        nodestore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        resetEnvVariables();
        
        //filtering out the part that should not be returned by the resultset.
        final String wc = whereCondition;
        List<ValuePathTuple> expected = Lists.newArrayList(Iterables.filter(nodes,
            new Predicate<ValuePathTuple>() {
                boolean stopHere;

                @Override
                public boolean apply(ValuePathTuple input) {
                    if (!stopHere) {
                        stopHere = unexistent.equals(input.getValue());
                    }
                    return !stopHere && input.getValue().compareTo(wc) > 0;
                }
            }));
        
        // pointing to a non-existent node in lane 0 we expect the result to be truncated
        LOGGING_TRACKER.reset();
        Result result = executeQuery(String.format(statement, whereCondition), SQL2, null);
        assertRightOrder(expected, result.getRows().iterator());
        assertTrue("We expect at least 1 warning message to be tracked",
            LOGGING_TRACKER.countLinesTracked() >= 1);
        
        setTraversalEnabled(true);
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
