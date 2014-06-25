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
package org.apache.jackrabbit.oak.util;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;


public class NodeCounter {
    /**
     * standard prefix for the count property
     */
    public static final String PREFIX = ":count-";
    
    /**
     * the standard approximation count resolution
     */
    public static final long APPROX_RESOLUTION = 1000L;
    
    /**
     * value returned if during the count no properties has been found
     */
    public static final long NO_PROPERTIES = 0;
    
    /**
     * Same as {@link #getApproxAdded(NodeBuilder, String)} providing {@link #PREFIX} as
     * {@code prefix}
     * 
     * @param node
     * @return
     */
    public static final long getApproxAdded(@Nonnull final NodeBuilder node) {
        return getApproxAdded(node, PREFIX);
    }

    /**
     * retrieve the approximate count of the provided node. Check for the return value if
     * {@link #NO_PROPERTIES} some actions will have to be taken by the caller.
     * 
     * @param node the node to analyse
     * @param prefix the prefix of the property to inspect
     * @return the approximate count of nodes or {@link #NO_PROPERTIES} in case it was not possible
     *         to compute it.
     */
    public static final long getApproxAdded(@Nonnull final NodeBuilder node,
                                            @Nonnull final String prefix) {
        return NO_PROPERTIES;
    }
}
