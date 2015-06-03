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
package org.apache.jackrabbit.oak.jcr;

import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.junit.Assume.assumeTrue;

import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.commons.FixturesHelper;
import org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.junit.BeforeClass;
import org.junit.Test;

public class AsyncConflictsIT extends DocumentClusterIT {
    private static final Set<Fixture> FIXTURES = FixturesHelper.getFixtures();

    @BeforeClass
    public static void assumptions() {
        assumeTrue(FIXTURES.contains(Fixture.DOCUMENT_NS));
        assumeTrue(OakMongoNSRepositoryStub.isMongoDBAvailable());
    }
    
    @Test
    public void updates() throws Exception {
        setUpCluster(this.getClass(), mks, repos, 0);
        defineIndex(repos.get(0));
        
        alignCluster(mks);
    }
    
    private void defineIndex(@Nonnull final Repository repo) throws RepositoryException {
        Session session = repo.login(ADMIN);
        Node n = session.getRootNode().getNode("oak:index");
        n = n.addNode("asyncconflict");
        n.setPrimaryType(IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);
        n.setProperty("compatVersion", 2);
        n.setProperty(TYPE_PROPERTY_NAME, "lucene");
        n.setProperty(ASYNC_PROPERTY_NAME, "async");
        n = n.addNode("indexRules");
        n.setPrimaryType(NT_UNSTRUCTURED);
        
        session.save();
    }
}
