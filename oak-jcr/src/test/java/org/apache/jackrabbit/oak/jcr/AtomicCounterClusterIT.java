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
import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PROP_COUNTER;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.MIX_ATOMIC_COUNTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.oak.commons.FixturesHelper;
import org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture;
import org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFutureTask;

public class AtomicCounterClusterIT {
    private static final Set<Fixture> FIXTURES = FixturesHelper.getFixtures();
    private static final Logger LOG = LoggerFactory.getLogger(AtomicCounterClusterIT.class);
    
    @BeforeClass
    public static void assumtions() {
        assumeTrue(FIXTURES.contains(Fixture.DOCUMENT_MK));
        assumeTrue(OakMongoNSRepositoryStub.isMongoDBAvailable());
    }
    
    @Test
    public void increments() throws Exception {
        setUpCluster(this.getClass(), mks, repos);

        final String counterPath;
        final Random rnd = new Random(14);
        final AtomicLong expected = new AtomicLong(0);
        final Map<String, Exception> exceptions = Collections.synchronizedMap(
            new HashMap<String, Exception>());

        // setting-up the repo state
        Repository repo = repos.get(0);
        Session session = repo.login(ADMIN);
        Node counter;
        
        try {
            counter = session.getRootNode().addNode("counter");
            counter.addMixin(MIX_ATOMIC_COUNTER);
            session.save();
            
            counterPath = counter.getPath();
        } finally {
            session.logout();
        }
        
        alignCluster(mks);
                
        // asserting the initial state
        assertFalse("Path to the counter node should be set", Strings.isNullOrEmpty(counterPath));
        for (Repository r : repos) {
            
            try {
                session = r.login(ADMIN);
                counter = session.getNode(counterPath);
                assertEquals("Nothing should have touched the `expected`", 0, expected.get());
                assertEquals(
                    "Wrong initial counter", 
                    expected.get(), 
                    counter.getProperty(PROP_COUNTER).getLong());
            } finally {
                session.logout();
            }
            
        }
        
        // for each cluster node, 100 sessions pushing random increments
        List<ListenableFutureTask<Void>> tasks = Lists.newArrayList();
        for (Repository rep : repos) {
            final Repository r = rep;
            for (int i = 0; i < 100; i++) {
                ListenableFutureTask<Void> task = ListenableFutureTask.create(new Callable<Void>() {

                        @Override
                        public Void call() throws Exception {
                            Session s = r.login(ADMIN);
                            try {
                                try {
                                    Node n = s.getNode(counterPath);
                                    int increment = rnd.nextInt(10) + 1;
                                    n.setProperty(PROP_COUNTER, increment);
                                    expected.addAndGet(increment);
                                    s.save();
                                } finally {
                                    s.logout();
                                }                                
                            } catch (Exception e) {
                                exceptions.put(Thread.currentThread().getName(), e);
                            }
                            return null;
                        }
                });
                new Thread(task).start();
                tasks.add(task);
            }
        }
        Futures.allAsList(tasks).get();
        
        alignCluster(mks);

        raiseExceptions(exceptions);
        
        // assert the final situation
        try {
            session = repos.get(0).login(ADMIN);
            counter = session.getNode(counterPath);
            assertEquals(
                "Wrong counter", 
                expected.get(), 
                counter.getProperty(PROP_COUNTER).getLong());
        } finally {
            session.logout();
        }
    }
    
    // ------------------------------------------------ < common with ConcurrentAddNodesClusterIT >
    
    List<Repository> repos = new ArrayList<Repository>();
    List<DocumentMK> mks = new ArrayList<DocumentMK>();

    /**
     * the number of nodes we'd like to run against
     */
    static final int NUM_CLUSTER_NODES = Integer.getInteger("it.documentmk.cluster.nodes", 5);
    
    /**
     * credentials for logging in as {@code admin}
     */
    static final Credentials ADMIN = new SimpleCredentials("admin", "admin".toCharArray());
    
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

    static void raiseExceptions(final Map<String, Exception> exceptions) throws Exception {
        if (exceptions != null) {
            for (Map.Entry<String, Exception> entry : exceptions.entrySet()) {
                LOG.error("Exception in thread {}", entry.getKey(), entry.getValue());
                throw entry.getValue();
            }
        }
    }

    /**
     * ensures that the cluster is aligned by running all the background operations
     * 
     * @param mks the list of {@link DocumentMK} composing the cluster. Cannot be null.
     */
    static void alignCluster(@Nonnull final List<DocumentMK> mks) {
        for (DocumentMK mk : mks) {
            mk.getNodeStore().runBackgroundOperations();
        }
        
        try {
            // FIXME we should remove this. Leave it for unlocking myself and proceeding with actual code.
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            LOG.error("Sleeping badly.", e);
        }
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
}
