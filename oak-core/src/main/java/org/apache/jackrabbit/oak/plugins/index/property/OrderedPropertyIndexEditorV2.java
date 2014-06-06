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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexEditor;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.SplitStrategy;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class OrderedPropertyIndexEditorV2 implements IndexEditor {
    /**
     * char used for filling in with nodes.
     */
    public static final String FILLER = ":";

    /**
     * internal use as some libs want a char but Strings are normally better understood.
     */
    public static final char FILLER_C = FILLER.charAt(0);

    /**
     * enum for easing the sort logic management
     */
    public enum SortLogic {
        STRING, DATE, LONG, DOUBLE
    }

    /**
     * used for passing an immutable easy-to-use index definition
     */
    public static class SplitRules {
        private final List<Long> split;
        private final SortLogic logic;
        private int length = -1;
        
        /**
         * Create the class based on the index definition. See the {@link #SplitStrategy}
         * documentation for details.
         * 
         * @param indexDefinition cannot be null
         */
        public SplitRules(@Nonnull final NodeBuilder indexDefinition) {
            PropertyState sp = indexDefinition.getProperty(OrderedIndex.PROPERTY_SPLIT);
            PropertyState lo = indexDefinition.getProperty(OrderedIndex.PROPERTY_LOGIC);
            
            if (sp != null && Type.LONGS.equals(sp.getType())) {
                split = Collections.unmodifiableList(Lists.newArrayList(sp.getValue(Type.LONGS)));
            } else {
                SplitStrategy.LOG.debug(
                    "Property 'split' is null or not of the correct type. Setting to null. split: {}",
                    sp);
                split = null;
            }
            
            if (lo != null && Type.STRING.equals(lo.getType())) {
                String s = lo.getValue(Type.STRING);
                SortLogic l;
                try {
                    l = SortLogic.valueOf(s.toUpperCase());
                } catch (IllegalArgumentException e) {
                    SplitStrategy.LOG.debug("Wrong logic specified. Defaulting to String");
                    l = SortLogic.STRING;
                }
                logic = l;
            } else {
                SplitStrategy.LOG.debug(
                    "Property 'logic' is null or not of the correct type. Setting to null. logic: {}",
                    lo);
                logic = null;
            }
            
        }
        
        /**
         * return the split rules to be applied
         * 
         * @return
         */
        public List<Long> getSplit() {
            return split;
        }
        
        /**
         * return the sort logic to be applied
         * @return
         */
        public SortLogic getLogic() {
            return logic;
        }
        
        /**
         * lazily compute and return the length the key should have for "balancing" the tree. See
         * {@link #SplitStrategy} for details.
         * 
         * @return
         */
        public int getLength() {
            if (length == -1) {
                // not computed yet. Let's do it.
                length = 0;
                for (Long l : getSplit()) {
                    length += l;
                }
            }
            return length;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(OrderedPropertyIndexEditorV2.class);
    
    /**
     * the index definition
     */
    private final NodeBuilder definition;

    /**
     * the propertyNames as by {@link #definition}
     */
    private final Set<String> propertyNames;
    private final OrderedPropertyIndexEditorV2 parent;
    private final String name;
    private final IndexUpdateCallback callback;
    
    private String path;
    private Set<String> beforeKeys;
    private Set<String> afterKeys;

    
    public OrderedPropertyIndexEditorV2(NodeBuilder definition, NodeState root,
                                        IndexUpdateCallback callback) {
        this.parent = null;
        this.name = null;
        this.path = "/";
        this.definition = definition;

        PropertyState pns = definition.getProperty(IndexConstants.PROPERTY_NAMES);
        String pn = pns.getValue(Type.NAME, 0);
        if (LOG.isDebugEnabled() && pns.count() > 1) {
            LOG.debug(
                "as we don't manage multi-property ordered indexes only the first one will be used. Using: '{}'",
                pn);
        }
        this.propertyNames = Collections.singleton(pn);
        this.callback = callback;
    }
    
    public OrderedPropertyIndexEditorV2(@Nonnull final OrderedPropertyIndexEditorV2 parent, @Nonnull final String name) {
        this.parent = parent;
        this.name = name;
        this.path = null;
        this.definition = parent.definition;
        this.propertyNames = parent.propertyNames;
        this.callback = parent.callback;
    }
    
    /**
     * retrieve the currently set of propertyNames
     * @return
     */
    Set<String> getPropertyNames() {
        return propertyNames;
    }
    
    /**
     * tells whether the current property has to be processed or not.
     * 
     * @param name
     * @return
     */
    boolean isToProcess(@Nonnull final PropertyState state) {
        return getPropertyNames().contains(state.getName()) && state.count() > 0
               && PropertyType.BINARY != state.getType().tag();
    }

    /**
     * encode the PropertyValue for being used by the Strategy
     * 
     * @param pv
     * @return
     */
    public static Set<String> encode(final PropertyValue pv) {
        Set<String> set;
        
        if (pv == null) {
            set = null;
        } else {
            // TODO consider different use-cases on type based on configuration. Date, Long, etc.
            set = Sets.newHashSet();
            for (String s : pv.getValue(Type.STRINGS)) {
                set.add(encode(s));
            }
        }
        
        return set;
    }

    /**
     * convert a single String according to the rules
     * 
     * @param s
     * @return
     */
    public static String encode(@Nonnull final String s) {
        String ss;
        
        try {
            ss = URLEncoder.encode(s, Charsets.UTF_8.name()).replaceAll("\\*", "%2A");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("UTF-8 is unsupported", e);
        }

        return ss;
    }
    
    @Override
    public void enter(NodeState before, NodeState after) throws CommitFailedException {
        LOG.debug("enter()");
        
        beforeKeys = Sets.newHashSet();
        afterKeys = Sets.newHashSet();
    }

    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        NodeBuilder index;
        if (LOG.isDebugEnabled()) {
            LOG.debug("leave() - before: {}", before);
            LOG.debug("leave() - after: {}", after);
        }
        
        if (!beforeKeys.isEmpty() || !afterKeys.isEmpty()) {
            LOG.debug("updating...");
            callback.indexUpdate();
            index = definition.child(IndexConstants.INDEX_CONTENT_NODE_NAME);
            getStrategy().update(index, getPath(), beforeKeys, afterKeys);
        }
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        boolean toProcess = isToProcess(after);

        LOG.debug("propertyAdded() - name: {} - toProcess: {}", after.getName(), toProcess);
        
        if (toProcess) {
            afterKeys.addAll(encode(PropertyValues.create(after)));
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        // TODO Auto-generated method stub
        LOG.debug("propertyChanged()");
        LOG.debug("-- before: {}", before);
        LOG.debug("-- after: {}", after);
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        // TODO Auto-generated method stub
        LOG.debug("propertyDeleted()");
        LOG.debug("-- before: {}", before);
    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
        LOG.debug("childNodeAdded() - name: '{}'", name);
        return childIndexEditor(this, name);
    }

    @Override
    public Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        LOG.debug("childNodeChanged() - name: '{}'", name);
        return childIndexEditor(this, name);
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        LOG.debug("childNodeDeleted() - name: '{}'", name);
        return childIndexEditor(this, name);
    }
    
    OrderedPropertyIndexEditorV2 childIndexEditor(@Nonnull final OrderedPropertyIndexEditorV2 editor, 
                                                  @Nonnull final String name) {
        return new OrderedPropertyIndexEditorV2(editor, name);
    }
    
    IndexStoreStrategy getStrategy() {
        return new SplitStrategy();
    }
    
    public String getPath() {
        if (path == null) {
            path = PathUtils.concat(parent.getPath(), name);
        }
        return path;
    }

    /**
     * convert the input key into the set of nodes for creating the keys-tree. See
     * {@link #SplitStrategy} for details
     * 
     * @param key
     * @param rules
     * @return
     */
    public static Iterable<String> tokenise(@Nonnull final String key, @Nonnull final SplitRules rules) {
        String k = Strings.padEnd(key, rules.getLength(), FILLER_C);
        List<String> tokens = new ArrayList<String>();
        Iterator<Long> iter = rules.getSplit().iterator();
        int start = 0;
        while (iter.hasNext()) {
            int l = (int) (long) iter.next();
            String s = k.substring(start, Math.min(k.length(), start+l));
            if (s.startsWith(FILLER)) {
                s = FILLER;
            }
            tokens.add(s);
            start += l;
        }
        return Collections.unmodifiableList(tokens);
    }
}