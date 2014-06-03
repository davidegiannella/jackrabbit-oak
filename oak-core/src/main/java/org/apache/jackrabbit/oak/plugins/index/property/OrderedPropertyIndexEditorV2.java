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
import java.nio.charset.UnsupportedCharsetException;
import java.util.Collections;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexEditor;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;

public class OrderedPropertyIndexEditorV2 implements IndexEditor {
    private static final Logger LOG = LoggerFactory.getLogger(OrderedPropertyIndexEditorV2.class);
    
    
    /**
     * the index definition
     */
    private final NodeBuilder definition;

    /**
     * the propertyNames as by {@link #definition}
     */
    private final Set<String> propertyNames;
    private Set<String> beforeKeys;
    private Set<String> afterKeys;

    private OrderedPropertyIndexEditorV2 parent;
    private final String name;
    
    public OrderedPropertyIndexEditorV2(NodeBuilder definition, NodeState root,
                                        IndexUpdateCallback callback) {
        this.parent = null;
        this.name = null;
        this.definition = definition;

        PropertyState pns = definition.getProperty(IndexConstants.PROPERTY_NAMES);
        String pn = pns.getValue(Type.NAME, 0);
        if (LOG.isDebugEnabled() && pns.count() > 1) {
            LOG.debug(
                "as we don't manage multi-property ordered indexes only the first one will be used. Using: '{}'",
                pn);
        }
        this.propertyNames = Collections.singleton(pn);
    }
    
    public OrderedPropertyIndexEditorV2(@Nonnull final OrderedPropertyIndexEditorV2 parent, @Nonnull final String name) {
        this.parent = parent;
        this.name = name;
        this.definition = parent.definition;
        this.propertyNames = parent.propertyNames;
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
    static Set<String> encode(final PropertyValue pv) {
        Set<String> set;
        
        if (pv == null) {
            set = null;
        } else {
            // TODO consider different use-cases on type based on configuration. Date, Ints, etc.
            set = Sets.newHashSet();
            try {
                for (String s : pv.getValue(Type.STRINGS)) {
                    s = URLEncoder.encode(s, Charsets.UTF_8.name()).replaceAll("\\*", "%2A");
                    set.add(s);
                }
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException("UTF-8 is unsupported", e);
            }
        }
        
        return set;
    }
    
    @Override
    public void enter(NodeState before, NodeState after) throws CommitFailedException {
        LOG.debug("enter()");
        
        beforeKeys = Sets.newHashSet();
        afterKeys = Sets.newHashSet();
    }

    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        // TODO Auto-generated method stub
        LOG.debug("leave()");
        LOG.debug("-- before: {}", before);
        LOG.debug("-- after: {}", after);
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
}
