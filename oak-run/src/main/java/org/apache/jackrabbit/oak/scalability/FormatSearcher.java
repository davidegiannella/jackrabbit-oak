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

import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;

import org.apache.jackrabbit.oak.benchmark.util.MimeType;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.scalability.ScalabilityAbstractSuite.ExecutionContext;

/**
 * Searches on the file format/Mime type 
 *
 */
public class FormatSearcher extends SearchScalabilityBenchmark {
    @SuppressWarnings("deprecation")
    @Override
    protected Query getQuery(QueryManager qm, ExecutionContext context) throws RepositoryException {
        StringBuilder statement = new StringBuilder("/jcr:content/");
        
        statement.append(((String) context.getMap().get("ROOT_NODE_NAME"))).append("//element(*, ")
            .append(NodeTypeConstants.NT_UNSTRUCTURED).append(")");
        statement.append("[((");
        
        // adding all the possible mime-types in an OR fashion
        for (MimeType mt : MimeType.values()) {
            statement.append("@").append(ScalabilityBlobSearchSuite.FORMAT_PROP).append(" = '")
                .append(mt.getValue()).append("' or ");
        }

        // removing latest ' or '
        statement.delete(statement.lastIndexOf(" or "), statement.length());
        
        statement.append("))]");
        
        LOG.debug("{}", statement);
        
        return qm.createQuery(statement.toString(), Query.XPATH);
    }
    
}