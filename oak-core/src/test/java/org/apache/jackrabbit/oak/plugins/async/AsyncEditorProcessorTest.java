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

import static org.apache.jackrabbit.oak.plugins.async.AsyncProcessor.ASYNC;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Set;

import org.apache.jackrabbit.oak.NodeStoreFixture;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.FixturesHelper;
import org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture;
import org.apache.jackrabbit.oak.spi.commit.AsyncEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class AsyncEditorProcessorTest {
    private static final Set<Fixture> FIXTURES = FixturesHelper.getFixtures();
    
    private static final AsyncEditorProvider DUMMY_EDITOR_PROVIDER = new AsyncEditorProvider() {
        @Override
        public Editor getRootEditor(NodeState before, NodeState after, NodeBuilder builder,
                                    CommitInfo info) throws CommitFailedException {
            return null;
        }
    };
    
    @BeforeClass
    public static void assumptions() {
        Assume.assumeTrue(FIXTURES.contains(Fixture.SEGMENT_MK));
    }
    
    @Test
    public void checkPointChecks() throws CommitFailedException {
        final String processName = "foobar";
        NodeStore store = NodeStoreFixture.SEGMENT_MK.createNodeStore();
        AsyncEditorProcessor processor = new AsyncEditorProcessor(processName, store,
            ImmutableList.of(DUMMY_EDITOR_PROVIDER));
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
    @Ignore
    public void checkpointsOnError() {
        // TODO check the checkpoints in case of errors during processing of Editor
    }
}
