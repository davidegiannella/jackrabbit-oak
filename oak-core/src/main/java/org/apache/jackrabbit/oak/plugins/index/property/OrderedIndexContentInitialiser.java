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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class OrderedIndexContentInitialiser implements RepositoryInitializer {
    private static final Logger LOG = LoggerFactory.getLogger(OrderedIndexContentInitialiser.class);
    
    @Override
    public void initialize(NodeBuilder builder) {
        final String indexDefNode = "orderedV2";
        NodeBuilder oakIndex = builder.child(IndexConstants.INDEX_DEFINITIONS_NAME);
        NodeBuilder indexDef = oakIndex.getChildNode(indexDefNode);
        if (!indexDef.exists()) {
            LOG.debug("Creating index definition");
            indexDef = oakIndex.child(indexDefNode);
            indexDef.setProperty(JcrConstants.JCR_PRIMARYTYPE, IndexConstants.INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
            indexDef.setProperty(IndexConstants.TYPE_PROPERTY_NAME, OrderedIndex.TYPE, Type.STRING);
            indexDef.setProperty(OrderedIndex.PROP_VERSION, OrderedIndex.Version.V2.toString(), Type.STRING);
            indexDef.setProperty(PropertyStates.createProperty(IndexConstants.PROPERTY_NAMES, ImmutableList.of("theproperty"), Type.NAMES));
            indexDef.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true, Type.BOOLEAN);
        } else {
            LOG.debug("Node already exists. Skipping.");
        }
    }

}
