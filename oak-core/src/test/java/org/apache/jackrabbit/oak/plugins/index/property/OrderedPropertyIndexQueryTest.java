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
package org.apache.jackrabbit.oak.plugins.index.property;

import static junit.framework.Assert.*;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.jcr.RepositoryException;

import net.sf.cglib.proxy.ProxyRefDispatcher;

import org.apache.jackrabbit.commons.webdav.NodeTypeConstants;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;

import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.*;

import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.h2.index.IndexCondition;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class OrderedPropertyIndexQueryTest extends AbstractQueryTest {
   public final static String ORDERED_PROPERTY = "foo";

   private class ValuePathTuple implements Comparable<ValuePathTuple>{
      private String value;
      private String path;
      
      ValuePathTuple(String value, String path) {
         this.value=value;
         this.path=path;
      }
      
      @Override
      public int hashCode() {
         final int prime = 31;
         int result = 1;
         result = prime * result + getOuterType().hashCode();
         result = prime * result + ((path == null) ? 0 : path.hashCode());
         result = prime * result + ((value == null) ? 0 : value.hashCode());
         return result;
      }

      @Override
      public boolean equals(Object obj) {
         if (this == obj)
            return true;
         if (obj == null)
            return false;
         if (getClass() != obj.getClass())
            return false;
         ValuePathTuple other = (ValuePathTuple) obj;
         if (!getOuterType().equals(other.getOuterType()))
            return false;
         if (path == null) {
            if (other.path != null)
               return false;
         } else if (!path.equals(other.path))
            return false;
         if (value == null) {
            if (other.value != null)
               return false;
         } else if (!value.equals(other.value))
            return false;
         return true;
      }

      @Override
      public int compareTo(ValuePathTuple o) {
         if(this.equals(o)) return 0;
         
         if(this.value.compareTo(o.value)<0) return -1;
         if(this.value.compareTo(o.value)>0) return 1;

         if(this.path.compareTo(o.path)<0) return -1;
         if(this.path.compareTo(o.path)>0) return 1;
         
         return 0;
      }

      private OrderedPropertyIndexQueryTest getOuterType() {
         return OrderedPropertyIndexQueryTest.this;
      }
      
   }
   
   @Test public void valuePathTupleComparison(){
      try{
         new ValuePathTuple("value", "path").compareTo(null);
         fail("It should have raisen a NPE");
      }catch(NullPointerException e){
         //so far so good
      }
      assertEquals(0,(new ValuePathTuple("value", "path")).compareTo(new ValuePathTuple("value", "path")));
      assertEquals(-1,(new ValuePathTuple("value", "path")).compareTo(new ValuePathTuple("value1", "path")));
      assertEquals(-1,(new ValuePathTuple("value1", "path")).compareTo(new ValuePathTuple("value1", "path1")));
      assertEquals(1,(new ValuePathTuple("value1", "path")).compareTo(new ValuePathTuple("value", "path")));
      assertEquals(1,(new ValuePathTuple("value1", "path1")).compareTo(new ValuePathTuple("value1", "path")));

      assertEquals(-1,(new ValuePathTuple("value000", "/test/n1")).compareTo(new ValuePathTuple("value001", "/test/n0")));
      assertEquals(1,(new ValuePathTuple("value001", "/test/n0")).compareTo(new ValuePathTuple("value000", "/test/n1")));
}
   
   @Override
   protected ContentRepository createRepository() {
      return new Oak().with(new InitialContent())
            .with(new OpenSecurityProvider())
            .with(new PropertyIndexProvider())
            .with(new PropertyIndexEditorProvider())
            .with(new OrderedPropertyIndexProvider())
            .with(new OrderedPropertyIndexEditorProvider())
            .createContentRepository();
   }

   /**
    * create a child node for the provided father
    * 
    * @param father
    * @param name the name of the node to create
    * @param propName the name of the property to assign
    * @param propValue the value of the property to assign
    * @return
    */
   private Tree child(Tree father, String name, String propName, String propValue){
      Tree child = father.addChild(name);
      child.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME);
      child.setProperty(propName,propValue,Type.STRING);
      return child;
   }

   /**
    * generate a list of values to be used as ordered set
    * @param amount
    * @return
    */
   private List<String> generateOrderedValues(int amount){
      if(amount>1000) throw new RuntimeException("amount cannot be greater than 100");
      
      List<String> values = new ArrayList<String>(amount);
      NumberFormat nf = new DecimalFormat("000");
      for(int i=0;i<amount;i++) values.add( String.format("value%s",String.valueOf(nf.format(i))) );
      
      return values;
   }

   private List<ValuePathTuple> addChildNodes(final List<String> values, final Tree father){
      List<ValuePathTuple> nodes = new ArrayList<ValuePathTuple>();
      Random rnd = new Random();
      int counter = 0;
      while(values.size()>0){
         String v = values.remove(rnd.nextInt(values.size()));
         Tree t = child(father, String.format("n%s",counter++), ORDERED_PROPERTY, v);
         nodes.add(new ValuePathTuple(v, t.getPath()));
      }
      
      Collections.sort(nodes);
      return nodes;
   }
   
   @Test public void query() throws CommitFailedException, ParseException, RepositoryException{
      setTravesalEnabled(false);
      
      Tree tree = root.getTree("/");

      IndexUtils.createIndexDefinition(
            new NodeUtil(tree.getChild(IndexConstants.INDEX_DEFINITIONS_NAME)),
            "indextest", 
            false, 
            new String[] { ORDERED_PROPERTY },
            null
       );
      root.commit();

      Tree test = tree.addChild("test");
      List<ValuePathTuple> nodes = addChildNodes(generateOrderedValues(10),test);
      for(ValuePathTuple n : nodes){
         System.out.println(String.format("%s - %s",n.value,n.path));
      }
      
      
////      Tree forth = child(test,"d",ORDERED_PROPERTY,ORDERED_VALUES[3]);
//      Tree second = child(test,"b",ORDERED_PROPERTY,ORDERED_VALUES[1]);
//      root.commit();
////      Tree third = child(test,"c",ORDERED_PROPERTY,ORDERED_VALUES[2]);
//      Tree first = child(test,"a",ORDERED_PROPERTY,ORDERED_VALUES[0]);
//      root.commit();
//
//      //querying
//      Iterator<? extends ResultRow> results;
//      results = executeQuery(
//            String.format("SELECT * from [%s] WHERE foo IS NOT NULL", NT_UNSTRUCTURED , ORDERED_PROPERTY), 
////            String.format("SELECT * from [%s] WHERE foo IS NOT NULL ORDER BY %s", NT_UNSTRUCTURED , ORDERED_PROPERTY), 
//            SQL2, 
//            null
//      ).getRows().iterator();
//      assertTrue("two elements expected",results.hasNext());
//      System.out.println(first.getPath());
//      assertEquals("Wrong item found. Order not respected.",first.getPath(),results.next().getPath());
      

      setTravesalEnabled(true);
   }
}
