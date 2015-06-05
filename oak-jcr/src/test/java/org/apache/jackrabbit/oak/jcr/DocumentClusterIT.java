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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;

/**
 * abstract class that can be inherited by an IT who has to run tests against a cluster of
 * DocumentMKs for having some utility methods available.
 */
public abstract class DocumentClusterIT {
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
    
    static final int NOT_PROVIDED = Integer.MIN_VALUE;
    
    @Before
    public void before() throws Exception {
        dropDB(this.getClass());
        
        List<Repository> rs = new ArrayList<Repository>();
        List<DocumentMK> ds = new ArrayList<DocumentMK>();
        
        initRepository(this.getClass(), rs, ds);
        
        Repository repository = rs.get(0);
        DocumentMK mk = ds.get(0);
        
        Session session = repository.login(ADMIN);
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
        dropDB(this.getClass());
    }

    /**
     * raise the exception passed into the provided Map
     * 
     * @param exceptions
     * @param log may be null. If valid Logger it will be logged
     * @throws Exception
     */
    static void raiseExceptions(@Nonnull final Map<String, Exception> exceptions, 
                                @Nullable final Logger log) throws Exception {
        if (exceptions != null) {
            for (Map.Entry<String, Exception> entry : exceptions.entrySet()) {
                if (log != null) {
                    log.error("Exception in thread {}", entry.getKey(), entry.getValue());
                }
                throw entry.getValue();
            }
        }
    }

    /**
     * <p> 
     * ensures that the cluster is aligned by running all the background operations
     * </p>
     * 
     * <p>
     * In order to use this you have to initialise the cluster with {@code setAsyncDelay(0)}.
     * </p>
     * 
     * @param mks the list of {@link DocumentMK} composing the cluster. Cannot be null.
     */
    static void alignCluster(@Nonnull final List<DocumentMK> mks) {
        for (int i = 0; i < 2; i++) {
            for (DocumentMK mk : mks) {
                mk.getNodeStore().runBackgroundOperations();
            }            
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
        setUpCluster(clazz, mks, repos, NOT_PROVIDED);
    }

    static void setUpCluster(@Nonnull final Class<?> clazz, 
                             @Nonnull final List<DocumentMK> mks,
                             @Nonnull final List<Repository> repos,
                             final int asyncDelay) throws Exception {
        for (int i = 0; i < NUM_CLUSTER_NODES; i++) {
            DocumentMK.Builder builder = new DocumentMK.Builder(); 
            
            builder.setMongoDB(createConnection(checkNotNull(clazz)).getDB())
                   .setClusterId(i + 1);
            
            if (asyncDelay != NOT_PROVIDED) {
                builder.setAsyncDelay(asyncDelay);
            }
                    
            DocumentMK mk = builder.open();
            
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

    protected void initRepository(@Nonnull final Class<?> clazz, 
                                  @Nonnull final List<Repository> repos, 
                                  @Nonnull final List<DocumentMK> mks) throws Exception {
        MongoConnection con = createConnection(checkNotNull(clazz));
        DocumentMK mk = new DocumentMK.Builder()
                .setMongoDB(con.getDB())
                .setClusterId(1).open();
        Repository repository = new Jcr(mk.getNodeStore()).createRepository();
        
        repos.add(repository);
        mks.add(mk);
    }
}
