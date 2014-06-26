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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.isEmpty;

import java.util.Random;
import java.util.UUID;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;


public class NodeCounter {
    private static enum Operation {ADDED, REMOVED};
    
    /**
     * standard prefix for the count property
     */
    public static final String PREFIX = ":count-";
    
    /**
     * the standard approximation count resolution
     */
    public static final long APPROX_MIN_RESOLUTION = 1000L;
    
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
    public static long getApproxAdded(@Nonnull final NodeBuilder node) {
        return getApproxAdded(node, PREFIX);
    }

    private static long getApprox(@Nonnull final NodeBuilder node,
                                  @Nonnull final String prefix,
                                  @Nonnull final Operation op) {
        long count, value;
        Iterable<? extends PropertyState> properties;
        
        checkNotNull(node);
        checkNotNull(prefix);
        checkNotNull(op);
        
        properties = node.getProperties();
        if (isEmpty(properties)) {
            count = NO_PROPERTIES;
        } else {
            count = 0;
            for (PropertyState p : properties) {
                if (p.getName().startsWith(prefix)) {
                    value = p.getValue(Type.LONG);
                    switch (op) {
                    case ADDED: 
                        if (value > 0) {
                            count += value;
                        }
                        break;
                    case REMOVED:
                        if (value < 0) {
                            count += -value;
                        }
                        break;
                    }
                }
            }
        }
        
        return count;
        
    }
    
    /**
     * retrieve the approximate count of the added nodes under the provided one.
     * 
     * @param node the node to analyse
     * @param prefix the prefix of the property to inspect
     * @return the approximate count of nodes or {@link #NO_PROPERTIES} in case it was not possible
     *         to compute it.
     */
    public static long getApproxAdded(@Nonnull final NodeBuilder node, 
                                      @Nonnull final String prefix) {
        return getApprox(node, prefix, Operation.ADDED);
    }
    
    /**
     * same as {@link #getApproxRemoved(NodeBuilder, String)} by passing {@link #PREFIX} as prefix.
     * 
     * @param node
     * @return
     */
    public static long getApproxRemoved(@Nonnull final NodeBuilder node) {
        return getApproxRemoved(node, PREFIX);
    }
    
    /**
     * retrieve the approximate count of the removed nodes under the provided one.
     * 
     * @param node the node to analyse
     * @param prefix the prefix of the property to inspect
     * @return the approximate count of nodes or {@link #NO_PROPERTIES} in case it was not possible
     *         to compute it.
     */
    public static long getApproxRemoved(@Nonnull final NodeBuilder node, 
                                        @Nonnull final String prefix) {
        return getApprox(node, prefix, Operation.REMOVED);
    }
    
    /**
     * as {@link #nodeAdded(Random, NodeBuilder, String, long)} by providing {@link #PREFIX} as
     * {@code prefix} and {@link #APPROX_MIN_RESOLUTION} as {@code minResolution}
     * 
     * @param rnd
     * @param node
     * @return
     */
    public static NodeBuilder nodeAdded(@Nonnull final Random rnd,
                                        @Nonnull final NodeBuilder node) {
        return nodeAdded(rnd, node, PREFIX, APPROX_MIN_RESOLUTION);
    }
    
    /**
     * Issue the NodeCounter algorithm on the provided node telling that a new Child has been added.
     * 
     * @param rnd the random generator to be used
     * @param node the node under which the child has been added
     * @param prefix the prefix for the count property
     * @param minResolution the minimum resolution to be used. Must be greater than 0.
     * @return the updated parent node.
     */
    public static NodeBuilder nodeAdded(@Nonnull final Random rnd,
                                        @Nonnull final NodeBuilder node,
                                        @Nonnull final String prefix,
                                        final long minResolution) {
        checkNotNull(rnd, "Random generator cannot be null");
        checkNotNull(node, "NodeBuilder cannot be null");
        checkNotNull(prefix, "Prefix cannot be null");
        checkArgument(minResolution > 0, "minResolution must be greater than 0");
        
        long count = Math.max(minResolution, getApproxAdded(node, prefix));
        // TODO casting down to int for now. What can we do to achieve a more precise range?
        // somewhere around
        // http://stackoverflow.com/questions/2546078/java-random-long-number-in-0-x-n-range could
        // work.
        if (rnd.nextInt((int) count) == 0) {
            String propertyName = prefix + UUID.randomUUID();
            node.setProperty(propertyName, count, Type.LONG);
        }
        return node;
    }

    public static NodeBuilder nodeRemoved(@Nonnull final Random rnd,
                                          @Nonnull final NodeBuilder node) {
        return nodeRemoved(rnd, node, PREFIX, APPROX_MIN_RESOLUTION);
    }
    
    /**
     * Issue the NodeCounter algorithm on the provided node telling that a child has been deleted.
     * 
     * @param rnd the random generator to be used
     * @param node the node under which the child has been deleted
     * @param prefix the prefix for the count property
     * @param minResolution the minimum resolution to be used. Must be greater than 0.
     * @return the updated parent node.
     */
    public static NodeBuilder nodeRemoved(@Nonnull final Random rnd,
                                          @Nonnull final NodeBuilder node,
                                          @Nonnull final String prefix,
                                          final long minResolution) {
        checkNotNull(rnd, "Random generator cannot be null");
        checkNotNull(node, "NodeBuilder cannot be null");
        checkNotNull(prefix, "Prefix cannot be null");
        checkArgument(minResolution > 0, "minResolution must be greater than 0");

        long count = Math.max(minResolution, getApproxRemoved(node, prefix));
        // TODO casting down to int for now. What can we do to achieve a more precise range?
        // see http://stackoverflow.com/questions/2546078/java-random-long-number-in-0-x-n-range
        if (rnd.nextInt((int) count) == 0) {
            String propertyName = prefix + UUID.randomUUID();
            node.setProperty(propertyName, -count, Type.LONG);
        }
        
        return node;
    }
    
    /**
     * as {@link #getApproxCount(NodeBuilder, String)} by passing {@link #PREFIX} as prefix;
     * 
     * @param node
     * @return
     */
    public static long getApproxCount(@Nonnull final NodeBuilder node) {
        return getApproxCount(node, PREFIX);
    }
    
    /**
     * <p>
     * retrieve the approximate child node count for the provided node inspecting by the provided
     * prefix properties.
     * </p>
     * 
     * <p>
     * In case no properties will be found, {@link #NO_PROPERTIES} will be returned. To be used the
     * caller to act after it.
     * </p>
     * 
     * @param node
     * @param prefix
     * @return
     */
    public static long getApproxCount(@Nonnull final NodeBuilder node,
                                      @Nonnull final String prefix) {
        
        checkNotNull(node, "NodeBuilder cannot be null");
        checkNotNull(prefix, "Predix cannot be null");
        
        long added = getApproxAdded(node, prefix);
        long removed = getApproxRemoved(node, prefix);
        
        if (added == NO_PROPERTIES && removed == NO_PROPERTIES) {
            return NO_PROPERTIES; 
        } else {
            added = (added == NO_PROPERTIES) ? 0 : added;
            removed = (removed == NO_PROPERTIES) ? 0 : removed;
            return Math.max(added / 2, added - removed);
        }
    }
    
    /**
     * same as {@link #getApproxCount(NodeBuilder, String)} by passing {@code node.builder()} as
     * node.
     * 
     * @param node
     * @param prefix
     * @return
     */
    public static long getApproxCount(@Nonnull final NodeState node,
                                      @Nonnull final String prefix) {
        checkNotNull(node, "NodeState cannot be null");
        return getApproxCount(node.builder(), prefix);
    }
    
    /**
     * same as {@link #getApproxCount(NodeState, String)} by passing {@link #PREFIX} as prefix.
     * 
     * @param node
     * @return
     */
    public static long getApproxCount(@Nonnull final NodeState node) {
        return getApproxCount(node, PREFIX);
    }
}
