/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.solr.query;

import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.solr.client.solrj.SolrQuery;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * Testcase for {@link org.apache.jackrabbit.oak.plugins.index.solr.query.FilterQueryParser}
 */
public class FilterQueryParserTest {

    @Test
    public void testMatchAllConversionWithNoConstraints() throws Exception {
        Filter filter = mock(Filter.class);
        OakSolrConfiguration configuration = mock(OakSolrConfiguration.class);
        SolrQuery solrQuery = FilterQueryParser.getQuery(filter, null, configuration);
        assertNotNull(solrQuery);
        assertEquals("*:*", solrQuery.getQuery());
    }

}
