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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest.dispose;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.oak.commons.FixturesHelper;
import org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AtomicCounterClusterIT {
    private static final Set<Fixture> FIXTURES = FixturesHelper.getFixtures();
    
    @BeforeClass
    public static void assumtions() {
        assumeTrue(FIXTURES.contains(Fixture.DOCUMENT_MK));
        assumeTrue(OakMongoNSRepositoryStub.isMongoDBAvailable());
    }
    
    @Test
    public void increments() throws Exception {
        setUpCluster(this.getClass(), mks, repos);
        
        
    }
    
    // ------------------------------------------------ < common with ConcurrentAddNodesClusterIT >
    
    List<Repository> repos = new ArrayList<Repository>();
    List<DocumentMK> mks = new ArrayList<DocumentMK>();

    /**
     * the number of nodes we'd like to run against
     */
    static final int NUM_CLUSTER_NODES = Integer.getInteger("it.documentmk.cluster.nodes", 5);

    @Before
    public void before() throws Exception {
        dropDB(this.getClass());
        initRepository(this.getClass());
    }

    @After
    public void after() throws Exception {
        for (Repository repo : repos) {
            dispose(repo);
        }
        for (DocumentMK mk : mks) {
            mk.dispose();
        }
        dropDB(this.getClass());
    }

    /**
     * set up the cluster connections
     * 
     * @param clazz class used for logging into Mongo itself
     * @param mks the list of mks to work on.
     * @param repos list of {@link Repository} created on each {@code mks}
     * @throws Exception
     */
    static void setUpCluster(@Nonnull final Class<?> clazz, 
                             @Nonnull final List<DocumentMK> mks,
                             @Nonnull final List<Repository> repos) throws Exception {
        for (int i = 0; i < NUM_CLUSTER_NODES; i++) {
            DocumentMK mk = new DocumentMK.Builder()
                    .setMongoDB(createConnection(checkNotNull(clazz)).getDB())
                    .setClusterId(i + 1).open();
            
            Repository repo = new Jcr(mk.getNodeStore()).createRepository();
            
            mks.add(mk);
            repos.add(repo);
        }        
    }
    
    static MongoConnection createConnection(@Nonnull final Class<?> clazz) throws Exception {
        return OakMongoNSRepositoryStub.createConnection(
                checkNotNull(clazz).getSimpleName());
    }

    static void dropDB(@Nonnull final Class<?> clazz) throws Exception {
        MongoConnection con = createConnection(checkNotNull(clazz));
        try {
            con.getDB().dropDatabase();
        } finally {
            con.close();
        }
    }

    static void initRepository(@Nonnull final Class<?> clazz) throws Exception {
        MongoConnection con = createConnection(checkNotNull(clazz));
        DocumentMK mk = new DocumentMK.Builder()
                .setMongoDB(con.getDB())
                .setClusterId(1).open();
        Repository repository = new Jcr(mk.getNodeStore()).createRepository();
        Session session = repository.login(
                new SimpleCredentials("admin", "admin".toCharArray()));
        session.logout();
        dispose(repository);
        mk.dispose(); // closes connection as well
    }

    /**
     * extends this class to implement your task. Will provide you with a class instance of
     * {@link Repository} and {@code Map<String, Exception>}
     */
    abstract class Worker implements Runnable {
        final Repository repo;
        final Map<String, Exception> exceptions;

        Worker(@Nonnull final Repository repo, @Nonnull final Map<String, Exception> exceptions) {
            this.repo = checkNotNull(repo);
            this.exceptions = checkNotNull(exceptions);
        }
    }
}
