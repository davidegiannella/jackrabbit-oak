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

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import javax.jcr.Credentials;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

public class CreateNodesWorker extends NodesWorker {
    public CreateNodesWorker(Repository repo, Map<String, Exception> exceptions, Credentials user,
                             IndexDefinition indexDefinition, int loopCount, int nodeCount) {
        super(repo, exceptions, user, indexDefinition, loopCount, nodeCount);
    }
    
    @Override
    public void run() {
        try {
            Session session = repo.login(user);
            CreateNodesWorker.ensureIndex(session, indexDefinition);

            String nodeName = getNodeName(Thread.currentThread());
            createNodes(session, nodeName, loopCount, nodeCount, exceptions);
        } catch (Exception e) {
            exceptions.put(Thread.currentThread().getName(), e);
        }
    }

    // Slightly modified by the ConcurrentAddNodesClusterIT making the indexes property a Date.
    private void createNodes(Session session,
                             String nodeName,
                             int loopCount,
                             int nodeCount,
                             Map<String, Exception> exceptions)
            throws RepositoryException {
        Node root = session.getRootNode().addNode(nodeName, "nt:unstructured");
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.S");
                
        for (int i = 0; i < loopCount; i++) {
            Node node = root.addNode("testnode" + i, "nt:unstructured");
            for (int j = 0; j < nodeCount; j++) {
                Node child = node.addNode("node" + j, "nt:unstructured");
                calendar = Calendar.getInstance();
                child.setProperty(indexDefinition.getPropertyNames()[0], calendar);
            }
            if (!exceptions.isEmpty()) {
                break;
            }
            if (LOG.isDebugEnabled() && i % 10 == 0) {
                LOG.debug("{} looped {}. Last calendar: {}",
                    new Object[] { nodeName, i, sdf.format(calendar.getTime()) });
            }
            session.save();
        }
    }

}
