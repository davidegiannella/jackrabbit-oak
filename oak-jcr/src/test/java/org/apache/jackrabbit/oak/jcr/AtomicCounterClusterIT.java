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

import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PROP_COUNTER;
import static org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditor.PROP_INCREMENT;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.MIX_ATOMIC_COUNTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.commons.FixturesHelper;
import org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFutureTask;

public class AtomicCounterClusterIT  extends DocumentClusterIT {
    private static final Set<Fixture> FIXTURES = FixturesHelper.getFixtures();
    private static final Logger LOG = LoggerFactory.getLogger(AtomicCounterClusterIT.class);
    
    @BeforeClass
    public static void assumtions() {
        assumeTrue(FIXTURES.contains(Fixture.DOCUMENT_NS));
        assumeTrue(OakMongoNSRepositoryStub.isMongoDBAvailable());
    }
    
    @Test
    public void increments() throws Exception {
        setUpCluster(this.getClass(), mks, repos, 0);

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
        
        final int numIncrements = 100;
        
        // for each cluster node, 100 sessions pushing random increments
        List<ListenableFutureTask<Void>> tasks = Lists.newArrayList();
        for (Repository rep : repos) {
            final Repository r = rep;
            for (int i = 0; i < numIncrements; i++) {
                ListenableFutureTask<Void> task = ListenableFutureTask.create(new Callable<Void>() {

                        @Override
                        public Void call() throws Exception {
                            Session s = r.login(ADMIN);
                            try {
                                try {
                                    Node n = s.getNode(counterPath);
                                    int increment = rnd.nextInt(10) + 1;
                                    n.setProperty(PROP_INCREMENT, increment);
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

        // let the time for the async process to kick in and run.
        alignCluster(mks);
        Thread.sleep(10000);
        alignCluster(mks);
        
        raiseExceptions(exceptions, LOG);
        
        // assert the final situation
        for (int i = 0; i < repos.size(); i++) {
            Repository r = repos.get(i);
            try {
                session = r.login(ADMIN);
                counter = session.getNode(counterPath);
                assertEquals(
                    "Wrong counter on node " + (i + 1), 
                    expected.get(), 
                    counter.getProperty(PROP_COUNTER).getLong());
            } finally {
                session.logout();
            }
            
        }
    }
    
    @Override
    protected Jcr getJcr(NodeStore store) {
        return super.getJcr(store).withAtomicCounter();
    }

    @Test @Ignore("louncher to inspect logging. Don't enable")
    public void longRunning() throws Exception {
        setUpCluster(getClass(), mks, repos);
        Thread.sleep(20000);
    }
}
