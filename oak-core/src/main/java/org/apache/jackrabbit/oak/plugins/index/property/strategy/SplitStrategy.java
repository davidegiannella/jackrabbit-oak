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

import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.index.property.OrderedPropertyIndexEditorV2;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the split strategy for the ordered indexes
 */
//TODO improve javadoc
public class SplitStrategy implements IndexStoreStrategy {
    public static final Logger LOG = LoggerFactory.getLogger(SplitStrategy.class);

    

    
    
    
    @Override
    public void update(NodeBuilder index, String path, Set<String> beforeKeys, Set<String> afterKeys) {
        // TODO Auto-generated method stub
        LOG.debug("update()");
            // 1. tokenise
            // 2. create tree
            // 3. hash the path
            // 4. store the path
    }

    @Override
    public Iterable<String> query(Filter filter, String indexName, NodeState indexMeta,
                                  Iterable<String> values) {
        // TODO Auto-generated method stub
        LOG.debug("query()");
        return null;
    }

    @Override
    public long count(NodeState indexMeta, Set<String> values, int max) {
        LOG.debug("count()");
        // TODO Auto-generated method stub
        return 0;
    }
}
