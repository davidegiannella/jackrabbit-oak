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

import static org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest.dispose;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jcr.Credentials;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.PathNotFoundException;
import javax.jcr.PropertyType;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderedIndexConcurrentClusterIT {
    private static final Logger LOG = LoggerFactory.getLogger(OrderedIndexConcurrentClusterIT.class);
    
    private static final long CACHE_SIZE = 32 * 1024 * 1024;
    private static final int NUM_CLUSTER_NODES = 5;
    private static final int LOOP = 2800;
    private static final int COUNT = 5;
    private static final Credentials ADMIN = new SimpleCredentials("admin", "admin".toCharArray());
    private static final String INDEX_NODE_NAME = "lastModified";
    private static final String INDEX_PROPERTY = "lastModified";
    private static final CreateNodesWorker.IndexDefinition INDEX_DEFINITION = new CreateNodesWorker.IndexDefinition(
        INDEX_NODE_NAME, OrderedIndex.TYPE, new String[] { INDEX_PROPERTY });

    private List<Repository> repos = new ArrayList<Repository>();
    private List<DocumentMK> mks = new ArrayList<DocumentMK>();
    private List<Thread> workers = new ArrayList<Thread>();

    // ----- SHARED WITH ConcurrentAddNodesClusterIT (later refactoring) -----
    // vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
    
    @BeforeClass
    public static void mongoDBAvailable() {
        Assume.assumeTrue(OakMongoMKRepositoryStub.isMongoDBAvailable());
    }
    
    private static MongoConnection createConnection() throws Exception {
        return OakMongoMKRepositoryStub.createConnection(
            OrderedIndexConcurrentClusterIT.class.getSimpleName());
    }

    @Before
    public void before() throws Exception {
        dropDB();
        initRepository();
    }

    private static void dropDB() throws Exception {
        MongoConnection con = createConnection();
        try {
            con.getDB().dropDatabase();
        } finally {
            con.close();
        }
    }

    private static void initRepository() throws Exception {
        MongoConnection con = createConnection();
        DocumentMK mk = new DocumentMK.Builder()
                .memoryCacheSize(CACHE_SIZE)
                .setMongoDB(con.getDB())
                .setClusterId(1).open();
        Repository repository = new Jcr(mk.getNodeStore()).createRepository();
        Session session = repository.login(ADMIN);
        ensureIndex(session);
        session.logout();
        dispose(repository);
        mk.dispose(); // closes connection as well
    }

    @After
    public void after() throws Exception {
        for (Repository repo : repos) {
            dispose(repo);
        }
        for (DocumentMK mk : mks) {
            mk.dispose();
        }
        dropDB();
    }

    // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    // ----- SHARED WITH ConcurrentAddNodesClusterIT (later refactoring) -----

    private static Repository createMongoRepo(final DocumentMK mk) {
        return new Jcr(mk.getNodeStore())
            .createRepository();
    }

    private static void raiseExceptions(final Map<String, Exception> exceptions) throws Exception {
        if (exceptions != null) {
            for (Map.Entry<String, Exception> entry : exceptions.entrySet()) {
                LOG.error("Exception in thread {}", entry.getKey(), entry.getValue());
                throw entry.getValue();
            }
        }
    }

    @Test
    public void deleteConcurrently() throws Exception {
        final int loop = LOOP;
        final int count = COUNT;
        final int clusters = NUM_CLUSTER_NODES;
        final List<Thread> creationWorkers = new ArrayList<Thread>();

        LOG.debug(
            "Adding a total of {} nodes evely spread across cluster. Loop: {}, Count: {}, Cluster nodes: {}",
            new Object[] { loop * count * clusters, loop, count, clusters });

        // creating instances
        for (int i = 1; i <= clusters; i++) {
            DocumentMK mk = new DocumentMK.Builder()
                    .memoryCacheSize(CACHE_SIZE)
                    .setMongoDB(createConnection().getDB())
                    .setClusterId(i).open();
            mks.add(mk);
        }

        Map<String, Exception> exceptions = Collections.synchronizedMap(
            new HashMap<String, Exception>());

        // initialising repositories and creating workers
        for (int i = 0; i < mks.size(); i++) {
            DocumentMK mk = mks.get(i);
            Repository repo = createMongoRepo(mk);
            Session session = repo.login(ADMIN);
            NodesWorker.ensureIndex(session, INDEX_DEFINITION);
            session.logout();
            repos.add(repo);
            workers.add(new Thread(new DeleteNodesWorker(repo, exceptions, ADMIN, INDEX_DEFINITION,
                loop, count), "Worker-" + (i + 1)));
            creationWorkers.add(new Thread(new CreateNodesWorker(repo, exceptions, ADMIN,
                INDEX_DEFINITION, loop, count), "Worker-" + (i + 1)));
        }

        // we know we have at least repos[0]
        Repository repo = repos.get(0);
        Session session = repo.login(ADMIN);
        ensureIndex(session);
        
        // initialising the repository sequentially to avoid any possible
        // concurrency errors during inserts
        for (Thread w : creationWorkers) {
            w.start();
            w.join();
        }
        
        // extra save for being sure.
        session.save();
        
        if (exceptions.isEmpty()) {
            // ensuring the cluster is aligned before triggering in order to avoid any
            // PathNotFoundException
            for (DocumentMK mk : mks) {
                mk.getNodeStore().runBackgroundOperations();
            }
            for (Thread t : workers) {
                t.start();
            }
            for (Thread t : workers) {
                t.join();
            }
        } else {
            // something where wrong during the insert. halting
            LOG.error("Something went wrong during insert");
        }
        
        raiseExceptions(exceptions);
    }
    
    /**
     * creates the index in the provided session
     * 
     * @param session
     */
    private static void ensureIndex(Session session) throws RepositoryException {
        Node root = session.getRootNode();
        Node indexDef = root.getNode(IndexConstants.INDEX_DEFINITIONS_NAME);
        Node index;

        if (!indexDef.hasNode(INDEX_NODE_NAME)) {
            index = indexDef.addNode(INDEX_NODE_NAME, IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);

            index.setProperty(IndexConstants.TYPE_PROPERTY_NAME, OrderedIndex.TYPE);
            index.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
            index.setProperty(IndexConstants.PROPERTY_NAMES, new String[] { INDEX_PROPERTY },
                PropertyType.NAME);
            try {
                root.getSession().save();
            } catch (RepositoryException e) {
                // created by other thread -> ignore
                root.getSession().refresh(false);
            }
        }
    }
}
