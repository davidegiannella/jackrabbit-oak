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
package org.apache.jackrabbit.oak.spi.commit;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.MIX_ATOMIC_COUNTER;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

public class AtomicCounterEditorTest {
    @Test
    public void childNodeAdded() throws CommitFailedException {
        NodeBuilder builder = EMPTY_NODE.builder();
        
        Editor editor = new AtomicCounterEditor(EMPTY_NODE.builder());
        
        assertNull("without the mixin we should not process",
            editor.childNodeAdded("foo", builder.getNodeState()));
        
        builder = EMPTY_NODE.builder();
        builder.setProperty(JCR_MIXINTYPES, of(MIX_ATOMIC_COUNTER), NAMES);
        assertTrue("with the mixin set we should get a proper Editor",
            editor.childNodeAdded("foo", builder.getNodeState()) instanceof AtomicCounterEditor);
    }
}
