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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class OrderedPropertyIndexLookupV2 extends AbstractPropertyIndexLookup {
    private final NodeState root;
    
    public OrderedPropertyIndexLookupV2(@Nonnull final NodeState root) {
        this.root = root;
    }

    @Override
    NodeState getRoot() {
        return root;
    }

    @Override
    String getType() {
        return OrderedIndex.TYPE;
    }

    @Override
    public long getEstimatedEntryCount(final String propertyName, 
                                       final PropertyValue value, 
                                       final Filter filter,
                                       final PropertyRestriction pr) {
        // TODO Auto-generated method stub
        return 0;
    }
}
