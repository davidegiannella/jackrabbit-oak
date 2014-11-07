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

import static javax.jcr.query.Query.JCR_SQL2;

import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;

import org.apache.jackrabbit.oak.scalability.ScalabilityAbstractSuite.ExecutionContext;

public class PropertyEqualsValueSearcher extends SearchScalabilityBenchmark {

    @Override
    protected Query getQuery(QueryManager qm, ExecutionContext context) throws RepositoryException {
        final String statement = String.format(
            "SELECT * FROM [%s] WHERE [jcr:content/jcr:lastModified]=CAST('%s' AS Date)",
            context.getMap().get(ScalabilityBlobSearchSuite.CTX_FILE_NODE_TYPE_PROP),
            context.getMap().get(ScalabilityBlobSearchSuite.LAST_MODIFIED_VALUE_PROP)
            );
        LOG.debug("PropertyEqualsValueSearcher. executing {}", statement);
        return qm.createQuery(statement, JCR_SQL2);
    }
}
