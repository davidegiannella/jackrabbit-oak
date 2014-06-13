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

import java.util.Map;

import javax.jcr.Credentials;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

public class DeleteNodesWorker extends NodesWorker {
    public DeleteNodesWorker(Repository repo, Map<String, Exception> exceptions, Credentials user,
                             IndexDefinition indexDefinition, int loopCount, int nodeCount) {
        super(repo, exceptions, user, indexDefinition, loopCount, nodeCount);
    }

    @Override
    public void run() {
        try {
            Session session = repo.login(user);
            CreateNodesWorker.ensureIndex(session, indexDefinition);

            String nodeName = getNodeName(Thread.currentThread());
            deleteNodes(session, nodeName, exceptions);
        } catch (Exception e) {
            exceptions.put(Thread.currentThread().getName(), e);
        }
    }

    private void deleteNodes(final Session session, final String nodeName,
                             final Map<String, Exception> exceptions) throws RepositoryException {

        Node root = session.getRootNode().getNode(nodeName);
        NodeIterator children = root.getNodes();

        while (children.hasNext()) {
            Node node = children.nextNode();
            NodeIterator children2 = node.getNodes();
            while (children2.hasNext()) {
                children2.nextNode().remove();
            }
            LOG.debug("deleting {}", node.getName());
            node.remove();
            session.save();
        }
    }
}
