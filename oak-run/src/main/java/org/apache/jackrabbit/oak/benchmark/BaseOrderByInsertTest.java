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

/**
 *
 */
public abstract class BaseOrderByInsertTest extends AbstractTest {
   /**
    * type of the created node
    */
   private final static String NODE_TYPE = "oak:Unstructured";
      
   /**
    * property that will be indexed
    */
   final static String INDEXED_PROPERTY = "indexedProperty";
   
   /**
    * the number of nodes created per iteration
    */
   private final static int NODES_PER_ITERATION = Integer.parseInt(System.getProperty("nodesPerIteration", "100"));

   /**
    * node name below which creating the test data
    */
   private final String DUMP_NODE = this.getClass().getSimpleName() + TEST_ID;

   /**
    * session used for operations throughout the test
    */
   Session session;
   
   /**
    * node under which all the test data will be filled in
    */
   private Node dump;
   
   @Override
   protected void beforeTest() throws Exception {
      session = loginWriter();

      //initiate the place for writing child nodes
      dump = session.getRootNode().addNode(DUMP_NODE,NODE_TYPE);
      session.save();
   }

   @Override
   protected void afterTest() throws Exception {
      //clean-up our mess
      dump.remove();
      session.save();
      session.logout();
   }

   /* (non-Javadoc)
    * @see org.apache.jackrabbit.oak.benchmark.AbstractTest#runTest()
    */
   @Override
   protected void runTest() throws Exception {
      try{
         for(int i=0; i<NODES_PER_ITERATION; i++){
            String uuid = UUID.randomUUID().toString();
            dump.addNode(uuid, NODE_TYPE).setProperty(INDEXED_PROPERTY, uuid);
            session.save();            
         }
      } catch (RepositoryException e){
         throw new RuntimeException(e);
      }
   }
}
