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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.index.IndexEditor;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class OrderedPropertyIndexEditorV2 implements IndexEditor {

    public OrderedPropertyIndexEditorV2(NodeBuilder definition, NodeState root,
                                        IndexUpdateCallback callback) {
        // TODO Auto-generated constructor stub
    }

    @Override
    public void enter(NodeState before, NodeState after) throws CommitFailedException {
        // TODO Auto-generated method stub

    }

    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        // TODO Auto-generated method stub

    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        // TODO Auto-generated method stub

    }

    @Override
    public void
        propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        // TODO Auto-generated method stub

    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        // TODO Auto-generated method stub

    }

    @Override
    public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public
        Editor
        childNodeChanged(String name, NodeState before, NodeState after)
                                                                        throws CommitFailedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        // TODO Auto-generated method stub
        return null;
    }

}
