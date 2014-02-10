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

package org.apache.jackrabbit.oak.plugins.index.property.strategy;

import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.spi.state.AbstractChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;


public class OrderedContentMirrorStoreStrategy extends ContentMirrorStoreStrategy {
   private final static Logger log = LoggerFactory.getLogger(OrderedContentMirrorStoreStrategy.class);
   public final static String NEXT = ":next";
   public final static String START = ":start";

   @Override
   NodeBuilder fetchKeyNode(@Nonnull NodeBuilder index, @Nonnull String key) {
      log.debug("fetchKeyNode() - index: {} - key: {}",index,key);
      NodeBuilder _key = null;
      NodeBuilder start = index.child(START);
      
      //identifying the right place for insert
      String n = start.getString(NEXT);
      if(Strings.isNullOrEmpty(n)){
         //new/empty index
         _key = index.child(key);
         _key.setProperty(NEXT,"");
         start.setProperty(NEXT, key);
      }else{
         //specific use-case where the item has to be added as first of the list
         String nextKey = n;
         if(key.compareTo(nextKey)<0){
            _key = index.child(key);
            _key.setProperty(NEXT,nextKey);
            start.setProperty(NEXT, key);
         }else{
            @SuppressWarnings("unchecked") Iterable<ChildNodeEntry> children = (Iterable<ChildNodeEntry>)getChildNodeEntries(index.getNodeState());
            for(ChildNodeEntry child : children){
               nextKey = child.getNodeState().getString(NEXT);
               if(Strings.isNullOrEmpty(nextKey)){
                  //we're at the last element, therefore our 'key' has to be appended
                  index.getChildNode(child.getName()).setProperty(NEXT, key);
                  _key = index.child(key);
                  _key.setProperty(NEXT, "");
               } else {
                  if(key.compareTo(nextKey)<0){
                     index.getChildNode(child.getName()).setProperty(NEXT, key);
                     _key = index.child(key);
                     _key.setProperty(NEXT, nextKey);
                     break;
                  } 
               }
            }
         }
      }
      
      return _key;
   }

   @Override
   void prune(Deque<NodeBuilder> builders) {
      for(NodeBuilder node : builders){
         if(node.hasProperty("match") || node.getChildNodeCount(1) > 0){
            return;
         }else if (node.exists()) {
            if(node.hasProperty(NEXT)){
               //it's an index key and we have to relink the list
               //(1) find the previous element
               //(2) find the next element
               //(3) re-link the previous to the next
            }else{
               node.remove();
            }
         }
      }
   }

   @SuppressWarnings("unchecked")
   @Nullable NodeState findPrevious(@Nonnull final NodeState index, @Nonnull final NodeState node){
      ChildNodeEntry previous = null;
      ChildNodeEntry current = null; 
      boolean found = false;
      Iterator<ChildNodeEntry> it = (Iterator<ChildNodeEntry>) getChildNodeEntries(index).iterator();
      
      while(!found && it.hasNext()){
         current = it.next();
         if(previous==null){
            //first iteration
            previous = current;
         }else{
            found = node.equals(current.getNodeState());
            if(!found) previous = current;
         }
      }
      
      return ((found) ? previous.getNodeState() : null);
   }
   
   @Override
   public void update(NodeBuilder index, String path, Set<String> beforeKeys, Set<String> afterKeys) {
      log.debug("update() - index     : {}",index);
      log.debug("update() - path      : {}",path);
      log.debug("update() - beforeKeys: {}",beforeKeys);
      log.debug("update() - afterKeys : {}",afterKeys);
      super.update(index, path, beforeKeys, afterKeys);
   }
   
   
   /**
    * retrieve an Iterable for going through the index in the right order without the :start node
    * 
    * @param index the root of the index (:index)
    * @return
    */
   @Nonnull Iterable<? extends ChildNodeEntry> getChildNodeEntries(@Nonnull final NodeState index){
      return getChildNodeEntries(index, false);
   }
   
   /**
    * retrive an Iterable for going through the index in the right order with potentially the :start node
    * 
    * @param index the root of the index (:index)
    * @param includeStart true if :start should be included as first element
    * @return
    */
   @Nonnull Iterable<? extends ChildNodeEntry> getChildNodeEntries(@Nonnull final NodeState index, final boolean includeStart){
      Iterable<? extends ChildNodeEntry> cne = null;
      NodeState start = index.getChildNode(START);
      
      if(!start.exists() || Strings.isNullOrEmpty(start.getString(NEXT))) {
         //if the property is not there or is empty it means we're empty
         cne = Collections.emptyList();
      }else{
         cne = new Iterable<ChildNodeEntry>(){
            private NodeState _index = index;
            private NodeState _start = _index.getChildNode(START);
            private NodeState current = _start;
            private boolean _includeStart = includeStart;

            @Override
            public Iterator<ChildNodeEntry> iterator() {
               return new Iterator<ChildNodeEntry>(){

                  @Override
                  public boolean hasNext() {
                     return !Strings.isNullOrEmpty(current.getString(NEXT));
                  }

                  @Override
                  public ChildNodeEntry next() {
                     ChildNodeEntry _cne = null;
                     if(_includeStart && _start.equals(current)){
                        _cne = new OrderedChildNodeEntry(START, current);
                        _includeStart = false; //let's set it to false. We just included it.
//                        current = _index.getChildNode(current.getString(NEXT));
                     } else {
                        if(hasNext()){
                           final String name = current.getString(NEXT);
                           current = _index.getChildNode(name);
                           _cne = new OrderedChildNodeEntry(name, current);
                        }else{
                           throw new NoSuchElementException();
                        }
                     }
                     return _cne;
                  }

                  @Override
                  public void remove() {
                     throw new UnsupportedOperationException();
                  }
               };
            }
         }; 
      }
      return cne;   
   }
   
   private final class OrderedChildNodeEntry extends AbstractChildNodeEntry {
      private final String name;
      private final NodeState state;
      
      public OrderedChildNodeEntry(@Nonnull final String name,@Nonnull final NodeState state){
         this.name = name;
         this.state = state;
      }
      
      /* (non-Javadoc)
       * @see org.apache.jackrabbit.oak.spi.state.ChildNodeEntry#getName()
       */
      @Override
      @Nonnull
      public String getName() {
         return name;
      }

      /* (non-Javadoc)
       * @see org.apache.jackrabbit.oak.spi.state.ChildNodeEntry#getNodeState()
       */
      @Override
      @Nonnull
      public NodeState getNodeState() {
         return state;
      }
   }
}
