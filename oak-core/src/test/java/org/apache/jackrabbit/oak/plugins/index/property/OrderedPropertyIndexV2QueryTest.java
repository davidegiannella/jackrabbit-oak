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

import static com.google.common.collect.ImmutableList.of;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.api.Type.LONGS;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.PROPERTY_SPLIT;
import static org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection.ASC;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

public class OrderedPropertyIndexV2QueryTest extends BasicOrderedPropertyIndexQueryTest {
    @Override
    protected void createTestIndexNode() throws Exception {
        Tree t = root.getTree("/");
        IndexUtils.createIndexDefinition(
            new NodeUtil(t.getChild(INDEX_DEFINITIONS_NAME)),
            TEST_INDEX_NAME,
            false,
            new String[] { ORDERED_PROPERTY },
            null,
            OrderedIndex.TYPE_2
        );
        t = root.getTree("/").getChild(INDEX_DEFINITIONS_NAME).getChild(TEST_INDEX_NAME);
        assertTrue("index definition not found.", t.exists());
        
        // we push something in the format of 'value000', 'value001'. Splitting as 'value0-00'
        t.setProperty(PROPERTY_SPLIT, of(6L, 2L), LONGS);
        
        root.commit();
    }

    @Override
    protected ContentRepository createRepository() {
        return new Oak().with(new InitialContent())
            .with(new OpenSecurityProvider())
            .with(new OrderedPropertyIndexEditorProvider())
            .with(new OrderedPropertyIndexProvider())
            .createContentRepository();
    }

    @Test
    public void queryPropertyNotNull() throws CommitFailedException, ParseException {
        setTraversalEnabled(false);
        
        final int numberOfNodes = 1000;
        final String query = String.format("SELECT * FROM [%s] WHERE %s IS NOT NULL",
            NT_UNSTRUCTURED, ORDERED_PROPERTY);
        
        // index automatically created by the framework:
        // {@code createTestIndexNode()}

        Tree rTree = root.getTree("/");
        Tree test = rTree.addChild("test");
        List<ValuePathTuple> nodes = addChildNodes(generateOrderedValues(numberOfNodes), test,
            ASC, STRING);
        root.commit();

        Iterator<? extends ResultRow> results;
        results = executeQuery(query, SQL2, null).getRows().iterator();
        assertContains(nodes, results);
        
        setTraversalEnabled(true);
    }
}
