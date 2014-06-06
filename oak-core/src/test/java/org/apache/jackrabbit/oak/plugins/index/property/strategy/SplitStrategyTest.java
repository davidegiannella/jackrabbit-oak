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
package org.apache.jackrabbit.oak.plugins.index.property.strategy;

import static org.junit.Assert.assertEquals;

import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.index.property.OrderedPropertyIndexEditorV2;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.SplitStrategy.SplitRules;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.index.property.strategy.SplitStrategy.FILLER;

import com.google.common.collect.ImmutableList;

public class SplitStrategyTest {
    /**
     * mocking class to ease the testing
     */
    private class MockRules extends SplitRules {
        private final List<Long> split;
        
        MockRules(@Nonnull final List<Long> split) {
            // initialise a dull non-used super configuration
            super(EmptyNodeState.EMPTY_NODE.builder());
            this.split = split;
        }

        @Override
        public List<Long> getSplit() {
            return split;
        }
    }
    
    @Test
    public void tokenise() {
        SplitRules rules;

        rules = new MockRules(ImmutableList.of(3L));
        assertEquals(ImmutableList.of("a::"), SplitStrategy.tokenise("a", rules));
        assertEquals(ImmutableList.of("ap:"), SplitStrategy.tokenise("ap", rules));
        assertEquals(ImmutableList.of("app"), SplitStrategy.tokenise("app", rules));
        assertEquals(ImmutableList.of("app"), SplitStrategy.tokenise("apple", rules));
        assertEquals(ImmutableList.of("app"), SplitStrategy.tokenise("apples", rules));
        assertEquals(ImmutableList.of("app"), SplitStrategy.tokenise("applets", rules));
        assertEquals(ImmutableList.of("app"), SplitStrategy.tokenise("apps", rules));

        rules = new MockRules(ImmutableList.of(3L, 2L));
        assertEquals(ImmutableList.of("a" + FILLER + FILLER, FILLER),
            SplitStrategy.tokenise("a", rules));
        assertEquals(ImmutableList.of("ap" + FILLER, FILLER), SplitStrategy.tokenise("ap", rules));
        assertEquals(ImmutableList.of("app", FILLER), SplitStrategy.tokenise("app", rules));
        assertEquals(ImmutableList.of("app", "le"), SplitStrategy.tokenise("apple", rules));
        assertEquals(ImmutableList.of("app", "le"), SplitStrategy.tokenise("apples", rules));
        assertEquals(ImmutableList.of("app", "le"), SplitStrategy.tokenise("applets", rules));
        assertEquals(ImmutableList.of("app", "s" + FILLER), SplitStrategy.tokenise("apps", rules));

        rules = new MockRules(ImmutableList.of(3L, 2L, 2L));
        assertEquals(ImmutableList.of("a" + FILLER + FILLER, FILLER, FILLER),
            SplitStrategy.tokenise("a", rules));
        assertEquals(ImmutableList.of("ap" + FILLER, FILLER, FILLER),
            SplitStrategy.tokenise("ap", rules));
        assertEquals(ImmutableList.of("app", FILLER, FILLER), SplitStrategy.tokenise("app", rules));
        assertEquals(ImmutableList.of("app", "le", FILLER), SplitStrategy.tokenise("apple", rules));
        assertEquals(ImmutableList.of("app", "le", "s" + FILLER),
            SplitStrategy.tokenise("apples", rules));
        assertEquals(ImmutableList.of("app", "le", "ts"), SplitStrategy.tokenise("applets", rules));
        assertEquals(ImmutableList.of("app", "s" + FILLER, FILLER),
            SplitStrategy.tokenise("apps", rules));
        
//        rules = new MockRules(ImmutableList.of(4L, 6L));
//        assertEquals(ImmutableList.of("2013", "-02-01"), SplitStrategy.tokenise(
//            OrderedPropertyIndexEditorV2.encode(
//                PropertyValues.newString("2013-02-01T12:13:17.580Z")).toArray(new String[0])[0],
//            rules));
//        assertEquals(ImmutableList.of("2013", "-03-01"), SplitStrategy.tokenise(
//            OrderedPropertyIndexEditorV2.encode(
//                PropertyValues.newString("2013-03-01T12:13:17.580Z")).toArray(new String[0])[0],
//            rules));
//        assertEquals(ImmutableList.of("2013", "-04-01"), SplitStrategy.tokenise(
//            OrderedPropertyIndexEditorV2.encode(
//                PropertyValues.newString("2013-04-01T12:13:17.580Z")).toArray(new String[0])[0],
//            rules));
//        assertEquals(ImmutableList.of("2014", "-02-01"), SplitStrategy.tokenise(
//            OrderedPropertyIndexEditorV2.encode(
//                PropertyValues.newString("2014-02-01T12:13:17.580Z")).toArray(new String[0])[0],
//            rules));
//
//        rules = new MockRules(ImmutableList.of(4L, 6L, 6L, 3L));
//        assertEquals(ImmutableList.of("2013", "-02-01"), SplitStrategy.tokenise(
//            OrderedPropertyIndexEditorV2.encode(
//                PropertyValues.newString("2013-02-01T12:13:17.580Z")).toArray(new String[0])[0],
//            rules));
//        assertEquals(ImmutableList.of("2013", "-03-01"), SplitStrategy.tokenise(
//            OrderedPropertyIndexEditorV2.encode(
//                PropertyValues.newString("2013-03-01T12:13:17.580Z")).toArray(new String[0])[0],
//            rules));
//        assertEquals(ImmutableList.of("2013", "-04-01"), SplitStrategy.tokenise(
//            OrderedPropertyIndexEditorV2.encode(
//                PropertyValues.newString("2013-04-01T12:13:17.580Z")).toArray(new String[0])[0],
//            rules));
//        assertEquals(ImmutableList.of("2014", "-02-01"), SplitStrategy.tokenise(
//            OrderedPropertyIndexEditorV2.encode(
//                PropertyValues.newString("2014-02-01T12:13:17.580Z")).toArray(new String[0])[0],
//            rules));
    }
}
