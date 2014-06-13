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
import javax.jcr.PropertyType;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class NodesWorker implements Runnable {
    static final Logger LOG = LoggerFactory.getLogger(NodesWorker.class);

    /**
     * convenience class for storing index definition attributes and passing around. POJO.
     */
    public static class IndexDefinition {
        private final String indexNodeName;
        private final String type;
        private final String[] propertyNames;
        
        public IndexDefinition(final String nodeName, final String type,
                               final String[] propertyNames) {
            this.indexNodeName = nodeName;
            this.type = type;
            this.propertyNames = propertyNames;
        }
        
        public String getIndexNodeName() {
            return indexNodeName;
        }
    
        public String getType() {
            return type;
        }
    
        public String[] getPropertyNames() {
            return propertyNames;
        }
    }

    final Repository repo;
    final Map<String, Exception> exceptions;
    final Credentials user;
    final IndexDefinition indexDefinition;
    final int loopCount, nodeCount;
    
    public NodesWorker(final Repository repo, final Map<String, Exception> exceptions,
                             final Credentials user, final IndexDefinition indexDefinition,
                             final int loopCount, final int nodeCount) {
        this.repo = repo;
        this.exceptions = exceptions;
        this.user = user;
        this.indexDefinition = indexDefinition;
        this.loopCount = loopCount;
        this.nodeCount = nodeCount;
    }

    /**
     * creates the index in the provided session
     * 
     * @param session
     * @param indexDefinition
     * @throws RepositoryException
     */
    public static void ensureIndex(final Session session, final IndexDefinition indexDefinition) throws RepositoryException {
        Node root = session.getRootNode();
        Node indexDef = root.getNode(IndexConstants.INDEX_DEFINITIONS_NAME);
        Node index;
    
        if (!indexDef.hasNode(indexDefinition.getIndexNodeName())) {
            index = indexDef.addNode(indexDefinition.getIndexNodeName(),
                IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);
    
            index.setProperty(IndexConstants.TYPE_PROPERTY_NAME, indexDefinition.getType());
            index.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
            index.setProperty(IndexConstants.PROPERTY_NAMES, indexDefinition.getPropertyNames(),
                PropertyType.NAME);
            try {
                root.getSession().save();
            } catch (RepositoryException e) {
                // created by other thread -> ignore
                root.getSession().refresh(false);
            }
        }
    }

    protected static String getNodeName(final Thread t) {
        return "testroot-" + t.getName();
    }
}
