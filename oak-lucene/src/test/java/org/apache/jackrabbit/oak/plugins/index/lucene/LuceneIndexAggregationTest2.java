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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;
import static org.apache.jackrabbit.JcrConstants.NT_FOLDER;
import static org.apache.jackrabbit.oak.plugins.name.NamespaceConstants.REP_NAMESPACES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.core.SystemRoot;
import org.apache.jackrabbit.oak.plugins.index.aggregate.AggregateIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.aggregate.NodeAggregator;
import org.apache.jackrabbit.oak.plugins.index.aggregate.SimpleNodeAggregator;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.name.NamespaceConstants;
import org.apache.jackrabbit.oak.plugins.name.NamespaceEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class LuceneIndexAggregationTest2 extends AbstractQueryTest {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneIndexAggregationTest2.class);
    
    @Override
    protected ContentRepository createRepository() {
        LowCostLuceneIndexProvider provider = new LowCostLuceneIndexProvider();
        
        return new Oak()
            .with(new InitialContent() {

                @Override
                public void initialize(NodeBuilder builder) {
                    super.initialize(builder);

                    // registering additional node types for wider testing
                    InputStream stream = null;
                    try {
                        stream = LuceneIndexAggregationTest2.class
                            .getResourceAsStream("test_nodetypes.cnd");
                        NodeState base = builder.getNodeState();
                        NodeStore store = new MemoryNodeStore(base);

                        Root root = new SystemRoot(store, new EditorHook(
                            new CompositeEditorProvider(new NamespaceEditorProvider(),
                                new TypeEditorProvider())));

                        NodeTypeRegistry.register(root, stream, "testing node types");

                        NodeState target = store.getRoot();
                        target.compareAgainstBaseState(base, new ApplyDiff(builder));
                    } catch (Exception e) {
                        LOG.error("Error while registering required node types. Failing here", e);
                        fail("Error while registering required node types");
                    } finally {
                        printNodeTypes(builder);
                        if (stream != null) {
                            try {
                                stream.close();
                            } catch (IOException e) {
                                LOG.debug("Ignoring exception on stream closing.", e);
                            }
                        }
                    }

                }

            })
            .with(new OpenSecurityProvider())
            .with(AggregateIndexProvider.wrap(provider.with(getNodeAggregator())))
            .with((Observer) provider).with(new LuceneIndexEditorProvider())
            .createContentRepository();
    }
    
    /**
     * convenience method for printing on logs the currently registered node types.
     * 
     * @param builder
     */
    private static void printNodeTypes(NodeBuilder builder) {
        if (LOG.isDebugEnabled()) {
            NodeBuilder namespace = builder.child(JCR_SYSTEM).child(JCR_NODE_TYPES);
            List<String> nodes = Lists.newArrayList(namespace.getChildNodeNames());
            Collections.sort(nodes);
            for (String node : nodes) {
                LOG.debug(node);
            }        
        }
    }
    
    private static NodeAggregator getNodeAggregator() {
        return new SimpleNodeAggregator()
            .newRuleWithName("nt:file", newArrayList("jcr:content"))
            .newRuleWithName("test:Page", newArrayList("jcr:content"))
            .newRuleWithName("test:PageContent", newArrayList("*", "*/*", "*/*/*", "*/*/*/*"))
            .newRuleWithName("test:Asset", newArrayList("jcr:content"))
            .newRuleWithName(
                "test:AssetContent",
                newArrayList("metadata", "renditions", "renditions/original", "comments",
                    "renditions/original/jcr:content"))
            .newRuleWithName("rep:User", newArrayList("profile"));
    }
    
    @Test
    public void dummy() {
        // just for running the initialiser. Part of an investigation
    }
}
