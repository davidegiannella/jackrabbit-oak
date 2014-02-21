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

import javax.jcr.Node;

import org.apache.jackrabbit.oak.benchmark.util.OakIndexUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedPropertyIndexEditorProvider;

/**
 *
 */
public class OrderedPropertyIndexInsertTest extends BaseOrderedIndexTest {
   private Node index = null;
   
   @Override
   void defineIndex() throws Exception{
      index = new OakIndexUtils.PropertyIndex().property(INDEXED_PROPERTY).create(session,OrderedPropertyIndexEditorProvider.TYPE);
      if(index == null) throw new RuntimeException("Error while creating the index definition. index node is null.");
      if(!OrderedPropertyIndexEditorProvider.TYPE.equals(index.getProperty(IndexConstants.TYPE_PROPERTY_NAME).getString())) throw new RuntimeException("The index type does not match the expected");
      session.save();
   }

   @Override
   protected void afterTest() throws Exception {
      //deleting the index. no need for session.save(); as it will be run by the super.afterTest();
      index.remove();
      super.afterTest();
   }
}
