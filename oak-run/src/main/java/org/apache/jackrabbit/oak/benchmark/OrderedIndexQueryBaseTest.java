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
package org.apache.jackrabbit.oak.benchmark;

import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;

/**
 * Benchmark the query performance of an ORDER BY clause when No index are involved
 */
public abstract class OrderedIndexQueryBaseTest extends OrderedIndexBaseTest {
    /**
     * query to execute with the ORDER BY statement
     */
    public final static String QUERY_WITH_ORDER = String.format(
        "SELECT * FROM [%s] WHERE %s IS NOT NULL ORDER BY %s", NODE_TYPE, INDEXED_PROPERTY, INDEXED_PROPERTY);

    @Override
    protected void beforeSuite() throws Exception {
        session = loginWriter();
        dump = session.getRootNode().addNode(DUMP_NODE, NODE_TYPE);
        session.save();
        defineIndex();
        insertRandomNodes(PRE_ADDED_NODES);
    }

    @Override
    protected void afterSuite() throws Exception {
        dump.remove();
        session.save();
        session.logout();
    }

    @Override
    protected void runTest() throws Exception {
        QueryManager qm = session.getWorkspace().getQueryManager();
        Query q = qm.createQuery(QUERY_WITH_ORDER, Query.JCR_SQL2);
        QueryResult r = q.execute();
        r.getNodes();
    }
    
    abstract String getQuery();
}
