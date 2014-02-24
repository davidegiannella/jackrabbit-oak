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

import java.util.UUID;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;

/**
 *
 */
public abstract class OrderedIndexBaseTest extends AbstractTest {
    /**
     * the number of nodes created per iteration
     */
    final static int NODES_PER_ITERATION = Integer.parseInt(System.getProperty("nodesPerIteration", "100"));
    
    /**
     * number of nodes that has to be added before performing the actual test
     */
    final static int PRE_ADDED_NODES = Integer.parseInt(System.getProperty("preAddedNodes", "0"));

    /**
    * type of the created node
    */
   final static String NODE_TYPE = NodeTypeConstants.NT_OAK_UNSTRUCTURED;
      
   /**
    * property that will be indexed
    */
   final static String INDEXED_PROPERTY = "indexedProperty";
   
   /**
    * node name below which creating the test data
    */
   final String DUMP_NODE = this.getClass().getSimpleName() + TEST_ID;

   /**
    * session used for operations throughout the test
    */
   Session session;
   
   /**
    * node under which all the test data will be filled in
    */
   Node dump;
      
   void insertRandomNodes(int numberOfNodes){
      try{
         for(int i=0; i<numberOfNodes; i++){
            String uuid = UUID.randomUUID().toString();
            dump.addNode(uuid, NODE_TYPE).setProperty(INDEXED_PROPERTY, uuid);
            session.save();            
         }
      } catch (RepositoryException e){
         throw new RuntimeException(e);
      }      
   }

   /**
    * override when needed to define an index
    */
   void defineIndex() throws Exception{
   }
}
