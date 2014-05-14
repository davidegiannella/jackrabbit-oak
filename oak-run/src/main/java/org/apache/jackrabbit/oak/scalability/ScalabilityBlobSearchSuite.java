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

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;

import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.ValueFormatException;
import javax.jcr.lock.LockException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeType;
import javax.jcr.version.VersionException;

import org.apache.commons.io.output.NullOutputStream;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.benchmark.TestInputStream;
import org.apache.jackrabbit.oak.benchmark.util.MimeType;
import org.apache.jackrabbit.oak.fixture.JcrCustomizer;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneInitializerHelper;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * The suite test will incrementally increase the load and execute searches.
 * Each test run thus adds blobs and executes different searches. This way we measure time taken for
 * search(es) execution.
 * 
 */
public class ScalabilityBlobSearchSuite extends ScalabilityAbstractSuite {
    
    private static final Logger LOG = LoggerFactory.getLogger(ScalabilityBlobSearchSuite.class);    

    private static final int FILE_SIZE = Integer.getInteger("fileSize", 1);

    /**
     * Controls the number of concurrent threads for writing blobs
     */
    private static final int WRITERS = Integer.getInteger("fileWriters", 0);

    /**
     * Controls the number of concurrent thread for reading blobs
     */
    private static final int READERS = Integer.getInteger("fileReaders", 0);

    /**
     * Controls the number of concurrent thread for searching
     */
    private static final int SEARCHERS = Integer.getInteger("fileSearchers", 0);

    /**
     * Controls the max child nodes created under a node.
     */
    private static final int MAX_ASSETS_PER_LEVEL = Integer.getInteger("maxAssets", 500);

    public static final String CTX_SEARCH_PATHS_PROP = "searchPaths";
    
    public static final String CTX_ROOT_NODE_NAME_PROP = "rootNodeName";
    
    private static final String CUSTOM_PATH_PROP = "contentPath";

    private static final String CUSTOM_REF_PROP = "references";

    public static final String FORMAT_PROP = "format";

    protected static final String ROOT_NODE_NAME =
            "LongevitySearchAssets" + TEST_ID;

    private final Random random = new Random(29);

    private List<String> searchPaths;

    private List<String> readPaths;

    private Boolean storageEnabled;

    public ScalabilityBlobSearchSuite(Boolean storageEnabled) {
        this.storageEnabled = storageEnabled;
    }

    @Override
    protected ScalabilitySuite addBenchmarks(ScalabilityBenchmark... tests) {
        benchmarks.addAll(Arrays.asList(tests));
        return this;
    }

    @Override
    protected void beforeSuite() throws Exception {
        Session session = loginWriter();
        session.getRootNode().addNode(ROOT_NODE_NAME);
        session.save();
    }

    /**
     * Executes before each test run
     */
    @Override
    public void beforeIteration(ExecutionContext context) throws RepositoryException {
        if (DEBUG) {
            System.out.println("Started beforeIteration()");
        }

        // recreate paths created in this run
        searchPaths = newArrayList();
        readPaths = newArrayListWithCapacity(READERS);

        // Creates assets for this run
        Writer writer = new Writer(context.getIncrement(), context.getIncrement());
        writer.run();

        // Add background jobs to simulate workload
        for (int i = 0; i < WRITERS; i++) {
            addBackgroundJob(new Writer(i, 1));
        }
        for (int i = 0; i < READERS; i++) {
            addBackgroundJob(new Reader());
        }
        for (int i = 1; i < SEARCHERS; i++) {
            addBackgroundJob(new FullTextSearcher());
        }

        if (DEBUG) {
            System.out.println("Finish beforeIteration()");
        }
        
        context.getMap().put(CTX_ROOT_NODE_NAME_PROP, ROOT_NODE_NAME);
        context.getMap().put(CTX_SEARCH_PATHS_PROP, searchPaths);
    }

    @Override
    protected void executeBenchmark(ScalabilityBenchmark benchmark, ExecutionContext context) throws Exception {
        benchmark.execute(getRepository(), CREDENTIALS, context);
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCustomizer() {
                @Override
                public Jcr customize(Jcr jcr) {
                    LuceneIndexProvider provider = new LuceneIndexProvider();
                    jcr.with((QueryIndexProvider) provider)
                            .with((Observer) provider)
                            .with(new LuceneIndexEditorProvider())
                            .with(new LuceneInitializerHelper("luceneGlobal", storageEnabled));
                    return jcr;
                }
            });
        }
        return super.createRepository(fixture);
    }

    private synchronized String getRandomReadPath() {
        if (readPaths.size() > 0) {
            return readPaths.get(random.nextInt(readPaths.size()));
        } else {
            return "";
        }
    }

    private synchronized void addReadPath(String file) {
        // Limit the number of paths added to be no more than the number of readers to limit the
        // heap used.
        int limit = 1000;
        if (readPaths.size() < limit) {
            readPaths.add(file);
        } else if (random.nextDouble() < 0.5) {
            readPaths.set(random.nextInt(limit), file);
        }
    }

    private synchronized void addSearchPath(String path) {
        if (!searchPaths.contains(path)) {
            searchPaths.add(path);
        }
    }
    
    private class Reader implements Runnable {

        private final Session session = loginWriter();

        @Override
        public void run() {
            try {
                String path = getRandomReadPath();
                session.refresh(false);
                JcrUtils.readFile(
                        session.getNode(path), new NullOutputStream());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private class Writer implements Runnable {

        private final Node parent;

        private long counter;

        /** The maximum number of assets to be written by this thread. */
        private int maxAssets;

        Writer(int id, int maxAssets) throws RepositoryException {
            this.maxAssets = maxAssets;
            this.parent = loginWriter()
                    .getRootNode()
                    .getNode(ROOT_NODE_NAME)
                    .addNode("writer-" + id);
            parent.getSession().save();
        }

        @Override
        public void run() {
            try {
                while (counter < maxAssets) {
                    parent.getSession().refresh(false);

                    List<String> levels = Lists.newArrayList();
                    getParentLevels(counter, maxAssets, levels);

                    String fileNamePrefix = getFileNamePrefix(levels);
                    String parentDir = getParentSuffix(levels);

                    Node file = putFile(fileNamePrefix, parentDir);

                    parent.getSession().save();

                    // record for searching and reading
                    addReadPath(file.getPath());
                    addSearchPath(fileNamePrefix);

                    if (DEBUG && counter % 1000 == 0) {
                        System.out.println("Added assets : " + counter);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private Node putFile(String fileNamePrefix, String parentDir) throws RepositoryException,
                UnsupportedRepositoryOperationException, ValueFormatException, VersionException,
                LockException, ConstraintViolationException {
            String type = NodeTypeConstants.NT_UNSTRUCTURED;
//            TODO find a way to differentiate for the JR2 vs OAK cases.
//              it will have to be matched then by the NodeTypeSearcher when building the query.
//            if (parent.getSession().getWorkspace().getNodeTypeManager().hasNodeType(
//                    NodeTypeConstants.NT_OAK_UNSTRUCTURED)) {
//                type = NodeTypeConstants.NT_OAK_UNSTRUCTURED;
//            }

            Node filepath = JcrUtils.getOrAddNode(parent, parentDir, type);
            Node file =
                    JcrUtils.getOrAddNode(filepath,
                            (fileNamePrefix + "File" + counter++),
                            type);
            
            // adding a custom format/mime-type for later querying.
            file.setProperty(FORMAT_PROP, MimeType.randomMimeType().getValue());
                        
            Binary binary =
                    parent.getSession().getValueFactory().createBinary(
                            new TestInputStream(FILE_SIZE * 1024));
            try {
                Node content =
                        JcrUtils.getOrAddNode(file, Node.JCR_CONTENT, NodeType.NT_RESOURCE);

                content.setProperty(Property.JCR_MIMETYPE, "application/octet-stream");
                content.setProperty(Property.JCR_LAST_MODIFIED, Calendar.getInstance());
                content.setProperty(Property.JCR_DATA, binary);

                file.setProperty(CUSTOM_PATH_PROP, file.getPath());
                String reference = getRandomReadPath();
                if (!Strings.isNullOrEmpty(reference)) {
                    file.setProperty(CUSTOM_REF_PROP, reference);
                }
            } finally {
                binary.dispose();
            }
            return file;
        }


        /**
         * Create a handy filename to search known files.
         * 
         * @param levels
         * @return
         */
        private String getFileNamePrefix(List<String> levels) {
            String name = "";
            for (String level : levels) {
                name = name + "Level" + level;
            }
            return name;
        }

        private String getParentSuffix(List<String> levels) {
            String parentSuffix = "";
            for (String level : levels) {
                parentSuffix = parentSuffix + level + "/";
            }
            return parentSuffix;
        }

        /**
         * Assigns the asset to it appropriate folder. The folder hierarchy is construted such that
         * each
         * folder has only MAX_ASSETS_PER_LEVEL children.
         * 
         * @param assetNum
         * @param maxAssets
         * @param levels
         */
        private void getParentLevels(long assetNum, long maxAssets,
                List<String> levels) {

            int maxAssetsNextLevel =
                    (int) Math.ceil((double) maxAssets / (double) MAX_ASSETS_PER_LEVEL);
            long nextAssetBucket = assetNum / maxAssetsNextLevel;

            levels.add(String.valueOf(nextAssetBucket));
            if (maxAssetsNextLevel > MAX_ASSETS_PER_LEVEL) {
                getParentLevels((assetNum - nextAssetBucket * maxAssetsNextLevel),
                        maxAssetsNextLevel,
                        levels);
            }
        }
    }
}
