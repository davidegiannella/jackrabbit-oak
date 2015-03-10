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
package org.apache.jackrabbit.oak.plugins.async;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.jackrabbit.oak.api.CommitFailedException.CONSTRAINT;
import static org.apache.jackrabbit.oak.plugins.async.AsyncProcessor.ASYNC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.apache.jackrabbit.oak.NodeStoreFixture;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.FixturesHelper;
import org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture;
import org.apache.jackrabbit.oak.spi.commit.AsyncEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class AsyncEditorProcessorTest {
    private static final Set<Fixture> FIXTURES = FixturesHelper.getFixtures();
    
    private static final AsyncEditorProvider DUMMY_EDITOR_PROVIDER = new AsyncEditorProvider() {
        @Override
        public Editor getRootEditor(NodeState before, NodeState after, NodeBuilder builder,
                                    CommitInfo info) throws CommitFailedException {
            return null;
        }
    };
    
    private static final AsyncEditorProvider ERRORING_EDITOR_PROVIDER = new AsyncEditorProvider() {
        @Override
        public Editor getRootEditor(NodeState before, NodeState after, NodeBuilder builder,
                                    CommitInfo info) throws CommitFailedException {
            return new DefaultEditor() {
                @Override
                public void leave(NodeState before, NodeState after) throws CommitFailedException {
                    throw new CommitFailedException(CONSTRAINT, 1, "We always fail. It's intentional");
                }
            };
        }
    };
    
    @BeforeClass
    public static void assumptions() {
        Assume.assumeTrue(FIXTURES.contains(Fixture.SEGMENT_MK));
    }
    
    @Test
    public void checkpointsSuccessfull() throws CommitFailedException {
        final String processName = "foobar";
        NodeStore store = NodeStoreFixture.SEGMENT_MK.createNodeStore();
        AsyncEditorProcessor processor = new AsyncEditorProcessor(processName, store,
            of(DUMMY_EDITOR_PROVIDER));
        NodeState root, async;
        NodeBuilder builder;
        String cp1, cp2;
        
        try {
            // asserting initial state
            root = store.getRoot();                
            assertFalse(root.getChildNode(ASYNC).exists());
            
            processor.run();
            
            root = store.getRoot();
            async = root.getChildNode(ASYNC);
            assertTrue("we should have the 'async' node by now", async.exists());
            cp1 = async.getString(processName);
            assertNotNull("the chekpoint reference should have been stored",
                cp1);
            
            builder = root.builder();
            builder.child("mickeymouse");
            store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            processor.run();
            
            root = store.getRoot();
            async = root.getChildNode(ASYNC);
            assertTrue("we should have the 'async' node by now", async.exists());
            cp2 = async.getString(processName);
            assertNotNull("the chekpoint reference should have been stored",
                cp2);
            assertFalse("We should have had a checkpoint rolling by now", cp1.equals(cp2));
        } finally {
            NodeStoreFixture.SEGMENT_MK.dispose(store);
        }
    }
    
    @Test
    public void checkpointsOnError() throws CommitFailedException {
        final String processName = "foobar";
        NodeStore store = NodeStoreFixture.SEGMENT_MK.createNodeStore();
        AsyncEditorProcessor processor;
        NodeState root, async;
        NodeBuilder builder;
        String cp1, cp2;
        
        try {
            // asserting initial state
            root = store.getRoot(); 
            processor = new AsyncEditorProcessor(processName, store, of(DUMMY_EDITOR_PROVIDER));
            assertFalse(root.getChildNode(ASYNC).exists());

            // let's process a good run for having a checkpoint.
            processor.run();

            root = store.getRoot();
            async = root.getChildNode(ASYNC);
            assertTrue("we should have the 'async' node by now", async.exists());
            cp1 = async.getString(processName);
            assertNotNull("the chekpoint reference should have been stored", cp1);

            // let's modify the store so we have something to process
            builder = root.builder();
            builder.child("mickeymouse");
            store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            // and now we have an error
            processor = new AsyncEditorProcessor(processName, store,
                of(ERRORING_EDITOR_PROVIDER));
            processor.run();
            root = store.getRoot();
            async = root.getChildNode(ASYNC);
            assertTrue("we should have the 'async' node by now", async.exists());
            cp2 = async.getString(processName);
            assertNotNull("the chekpoint reference should have been stored", cp2);
            assertEquals("in case of error the checkpoint should remain the same", cp1, cp2);
        } finally {
            NodeStoreFixture.SEGMENT_MK.dispose(store);
        }
    }
    
    @Test
    public void checkpointsSuccessNoChanges() {
        final String processName = "foobar";
        NodeStore store = NodeStoreFixture.SEGMENT_MK.createNodeStore();
        AsyncEditorProcessor processor;
        NodeState root, async;
        NodeBuilder builder;
        String cp1, cp2;
        
        try {
            // asserting initial state
            root = store.getRoot(); 
            processor = new AsyncEditorProcessor(processName, store, of(DUMMY_EDITOR_PROVIDER));
            assertFalse(root.getChildNode(ASYNC).exists());

            // let's process a good run for having a checkpoint.
            processor.run();

            root = store.getRoot();
            async = root.getChildNode(ASYNC);
            assertTrue("we should have the 'async' node by now", async.exists());
            cp1 = async.getString(processName);
            assertNotNull("the chekpoint reference should have been stored", cp1);

            processor.run();

            root = store.getRoot();
            async = root.getChildNode(ASYNC);
            assertTrue("we should have the 'async' node by now", async.exists());
            cp2 = async.getString(processName);
            assertNotNull("the chekpoint reference should have been stored", cp2);
            assertEquals("if no changes the checkpoint should be the same", cp1, cp2);
        } finally {
            NodeStoreFixture.SEGMENT_MK.dispose(store);
        }
    }
}
