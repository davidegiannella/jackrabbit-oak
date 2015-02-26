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
package org.apache.jackrabbit.oak.plugins.atomic;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.PartialConflictHandler;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OakCounterConflictHandler implements PartialConflictHandler {
    private static final Logger LOG = LoggerFactory.getLogger(OakCounterConflictHandler.class);

    @Override
    public Resolution addExistingProperty(final NodeBuilder parent, 
                                          final PropertyState ours, 
                                          final PropertyState theirs) {
        LOG.debug("addExistingProperty()");
        return null;
    }

    @Override
    public Resolution changeDeletedProperty(final NodeBuilder parent, final PropertyState ours) {
        LOG.debug("changeDeletedProperty()");
        return null;
    }

    @Override
    public Resolution changeChangedProperty(final NodeBuilder parent, 
                                            final PropertyState ours,
                                            final PropertyState theirs) {
        if (AtomicCounterEditor.PROP_COUNTER.equals(ours.getName())) {
            LOG.debug("-------- changeChangedProperty");
            LOG.debug("parent: {}", parent);
            LOG.debug("ours: {}", ours);
            LOG.debug("theirs: {}", theirs);
            LOG.debug("parent: {}", parent.getProperty(AtomicCounterEditor.PROP_COUNTER));
            LOG.debug("parent.basestate: {}", parent.getBaseState().getProperty(AtomicCounterEditor.PROP_COUNTER));

            long c = ours.getValue(Type.LONG).longValue() + theirs.getValue(Type.LONG).longValue();
            parent.setProperty(AtomicCounterEditor.PROP_COUNTER, c, Type.LONG);
            return Resolution.MERGED;
        }
        
        return null;
    }

    @Override
    public Resolution deleteDeletedProperty(NodeBuilder parent, PropertyState ours) {
        LOG.debug("deleteDeletedProperty");
        return null;
    }

    @Override
    public Resolution deleteChangedProperty(NodeBuilder parent, PropertyState theirs) {
        LOG.debug("deleteChangedProperty");
        return null;
    }

    @Override
    public Resolution addExistingNode(NodeBuilder parent, String name, NodeState ours,
                                      NodeState theirs) {
        LOG.debug("addExistingNode");
        return null;
    }

    @Override
    public Resolution changeDeletedNode(NodeBuilder parent, String name, NodeState ours) {
        LOG.debug("changeDeletedNode");
        return null;
    }

    @Override
    public Resolution deleteChangedNode(NodeBuilder parent, String name, NodeState theirs) {
        LOG.debug("deleteChangedNode");
        return null;
    }

    @Override
    public Resolution deleteDeletedNode(NodeBuilder parent, String name) {
        LOG.debug("deleteDeletedNode");
        return null;
    }

}
