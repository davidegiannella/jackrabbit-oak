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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

public class OrderedPropertyIndexEditorProviderTest {
    /**
     * cover the correct editor implementation based on the index definition
     * @throws CommitFailedException 
     */
    @Test
    public void correctEditor() throws CommitFailedException {
        final PropertyState propertyNames = PropertyStates.createProperty(
            IndexConstants.PROPERTY_NAMES, ImmutableList.of("we don't care"), Type.NAMES);
        IndexEditorProvider iep = new OrderedPropertyIndexEditorProvider();
        Editor editor;
        NodeBuilder indexDefinition;
        
        
        indexDefinition = Mockito.mock(NodeBuilder.class);
        Mockito.when(indexDefinition.getProperty(IndexConstants.PROPERTY_NAMES)).thenReturn(
            propertyNames);

        editor = iep.getIndexEditor("foobar", indexDefinition, null, null);
        assertNull("With wrong index type a null is expected", editor);
        
        editor = iep.getIndexEditor("foobar", indexDefinition, null, null);
        assertNull(editor);

        editor = iep.getIndexEditor(OrderedIndex.TYPE, indexDefinition, null, null);
        assertTrue(editor instanceof OrderedPropertyIndexEditor);

        editor = iep.getIndexEditor(OrderedIndex.TYPE_2, indexDefinition, null, null);
        assertTrue(editor instanceof OrderedPropertyIndexEditorV2);
    }
}
