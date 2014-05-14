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
package org.apache.jackrabbit.oak.scalability;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;

import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.scalability.ScalabilityAbstractSuite.ExecutionContext;

/**
 * Searches on the NodeType 
 *
 */
public class NodeTypeSearcher extends SearchScalabilityBenchmark {
    
    @SuppressWarnings("deprecation")
    @Override
    protected Query getQuery(@Nonnull final QueryManager qm, ExecutionContext context) throws RepositoryException {
        return qm.createQuery(
                "/jcr:root/" + ((String) context.getMap().get(ScalabilityBlobSearchSuite.CTX_ROOT_NODE_NAME_PROP)) + "//element(*, "
                        + NodeTypeConstants.NT_UNSTRUCTURED + ")",
                Query.XPATH);
    }
}