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

import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.OrderedContentMirrorStoreStrategy;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 *
 */
public class OrderedPropertyIndexLookup extends PropertyIndexLookup {

    /**
     * the standard Ascending ordered index
     */
    private static final IndexStoreStrategy STORE = new OrderedContentMirrorStoreStrategy();

    /**
     * the descending ordered index
     */
    private static final IndexStoreStrategy REVERSED_STORE = new OrderedContentMirrorStoreStrategy(OrderDirection.DESC);
    
    public OrderedPropertyIndexLookup(NodeState root) {
        super(root);
    }

    @Override
    IndexStoreStrategy getStrategy(NodeState indexMeta) {
        if (OrderDirection.isAscending(indexMeta)) {
            return STORE;
        } else {
            return REVERSED_STORE;
        }
    }

    @Override
    String getType() {
        return OrderedIndex.TYPE;
    }
}
