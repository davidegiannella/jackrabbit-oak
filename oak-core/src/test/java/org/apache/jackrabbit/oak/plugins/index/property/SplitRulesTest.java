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

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedPropertyIndexEditorV2.SplitRules;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class SplitRulesTest {

    @Test
    public void constructor() {
        NodeBuilder id;
        OrderedPropertyIndexEditorV2.SplitRules rules;
        List<Long> split;
        
        // property presence
        id = EmptyNodeState.EMPTY_NODE.builder();
        rules = new OrderedPropertyIndexEditorV2.SplitRules(id);
        assertEquals(ImmutableList.of(SplitRules.MAX), rules.getSplit());
        assertEquals("If we don't have the property set null is expected", null, rules.getLogic());

        // property type enforcing
        id = EmptyNodeState.EMPTY_NODE.builder();
        id.setProperty(PropertyStates.createProperty(OrderedIndex.PROPERTY_LOGIC, 123L, Type.LONG));
        id.setProperty(PropertyStates.createProperty(OrderedIndex.PROPERTY_SPLIT, "", Type.STRING));
        rules = new OrderedPropertyIndexEditorV2.SplitRules(id);
        assertEquals(ImmutableList.of(SplitRules.MAX), rules.getSplit());
        assertEquals("with wrong property type, null is expected", null, rules.getLogic());
        
        // correct settings
        split = ImmutableList.of(1L, 2L, 3L);
        id = EmptyNodeState.EMPTY_NODE.builder();
        id.setProperty(PropertyStates.createProperty(OrderedIndex.PROPERTY_LOGIC, "long", Type.STRING));
        id.setProperty(PropertyStates.createProperty(OrderedIndex.PROPERTY_SPLIT, split, Type.LONGS));
        rules = new OrderedPropertyIndexEditorV2.SplitRules(id);
        assertEquals(split, rules.getSplit());
        assertEquals(OrderedPropertyIndexEditorV2.SortLogic.LONG, rules.getLogic());
        
        // defaulting on String for sort logic
        id = EmptyNodeState.EMPTY_NODE.builder();
        id.setProperty(PropertyStates.createProperty(OrderedIndex.PROPERTY_LOGIC, "foobar", Type.STRING));
        rules = new OrderedPropertyIndexEditorV2.SplitRules(id);
        assertEquals(OrderedPropertyIndexEditorV2.SortLogic.STRING, rules.getLogic());
        id = EmptyNodeState.EMPTY_NODE.builder();
    }
    
    @Test
    public void length() { 
        NodeBuilder id; 
        
        id = EmptyNodeState.EMPTY_NODE.builder();
        id.setProperty(PropertyStates.createProperty(OrderedIndex.PROPERTY_SPLIT,
            ImmutableList.of(1L, 2L, 3L), Type.LONGS));
        assertEquals(6, new OrderedPropertyIndexEditorV2.SplitRules(id).getLength());

        id = EmptyNodeState.EMPTY_NODE.builder();
        id.setProperty(PropertyStates.createProperty(OrderedIndex.PROPERTY_SPLIT,
            ImmutableList.of(3L), Type.LONGS));
        assertEquals(3, new OrderedPropertyIndexEditorV2.SplitRules(id).getLength());
}
}
