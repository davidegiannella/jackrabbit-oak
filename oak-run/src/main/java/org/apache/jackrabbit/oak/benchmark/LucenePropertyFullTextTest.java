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
package org.apache.jackrabbit.oak.benchmark;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableSet.of;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLucenePropertyIndexDefinition;

import java.io.File;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.Repository;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.benchmark.wikipedia.WikipediaImport;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneInitializerHelper;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import com.google.common.collect.ImmutableSet;

public class LucenePropertyFullTextTest extends AbstractTest<LucenePropertyFullTextTest.TestContext> {

    /**
     * context used across the tests
     */
    class TestContext {
        final Session session = loginWriter();
    }

    /**
     * helper class to initialise the Lucene Property index definition
     */
    static class LucenePropertyInitialiser implements RepositoryInitializer {
        private String name;
        private Set<String> properties;
        
        public LucenePropertyInitialiser(@Nonnull final String name, 
                                         @Nonnull final Set<String> properties) {
            this.name = checkNotNull(name);
            this.properties = checkNotNull(properties);
        }
                
        private boolean isAlreadyThere(@Nonnull final NodeBuilder root) {
            return checkNotNull(root).hasChildNode(INDEX_DEFINITIONS_NAME) &&
                root.getChildNode(INDEX_DEFINITIONS_NAME).hasChildNode(name);
        }
        
        @Override
        public void initialize(final NodeBuilder builder) {
            if (!isAlreadyThere(builder)) {
                newLucenePropertyIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), name,
                    properties, "async");
            }
        }
        
    }
    
    private WikipediaImport importer;    
    private Boolean storageEnabled;

    public LucenePropertyFullTextTest(final File dump, 
                                      final boolean flat, 
                                      final boolean doReport, 
                                      final Boolean storageEnabled) {
        this.importer = new WikipediaImport(dump, flat, doReport);
        this.storageEnabled = storageEnabled;
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    LuceneIndexProvider provider = new LuceneIndexProvider();
                    oak.with((QueryIndexProvider) provider)
                       .with((Observer) provider)
                       .with(new LuceneIndexEditorProvider())
                       .with((new LuceneInitializerHelper("luceneGlobal", storageEnabled)).async())
                       .with(new LucenePropertyInitialiser("luceneTitle", of("title")));
                    return new Jcr(oak);
                }
            });
        }
        return super.createRepository(fixture);
    }

    @Override
    protected void runTest() throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public void run(Iterable<RepositoryFixture> fixtures) {
        // TODO Auto-generated method stub

    }

}
