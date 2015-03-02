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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.MIX_ATOMIC_COUNTER;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

public abstract class AtomicCounterEditor extends DefaultEditor {
    private static final Logger LOG = LoggerFactory.getLogger(AtomicCounterEditor.class);
    
    /**
     * property to be set for incrementing/decrementing the counter
     */
    public static final String PROP_INCREMENT = "oak:increment";
    /**
     * property with the consolidated counter
     */
    public static final String PROP_COUNTER = "oak:counter";

    /**
     * prefix used internally for tracking the counting requests
     */
    public static final String PREFIX_PROP_COUNTER = ":oak-counter-";

    protected static boolean shallWeProcessProperty(final PropertyState property, final String path, final NodeBuilder builder) {
        boolean process = false;
        PropertyState mixin = checkNotNull(builder).getProperty(JCR_MIXINTYPES);
        if (mixin != null && PROP_INCREMENT.equals(property.getName()) &&
                Iterators.contains(mixin.getValue(NAMES).iterator(), MIX_ATOMIC_COUNTER)) {
            if (LONG.equals(property.getType())) {
                process = true;
            } else {
                LOG.warn(
                    "although the {} property is set is not of the right value: LONG. Not processing node: {}.",
                    PROP_INCREMENT, path);
            }
        }
        return process;
    }

}
