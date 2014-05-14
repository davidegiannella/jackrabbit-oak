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
import static com.google.common.collect.Maps.newConcurrentMap;
import static com.google.common.collect.Maps.newHashMap;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.apache.jackrabbit.oak.benchmark.CSVResultGenerator;
import org.apache.jackrabbit.oak.benchmark.util.Profiler;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;

/**
 * Longevity suite to load test.
 * 
 */
public abstract class ScalabilityAbstractSuite implements ScalabilitySuite, CSVResultGenerator {
    /**
     * A random string to guarantee concurrently running tests don't overwrite
     * each others changes (for example in a cluster).
     * <p>
     * The probability of duplicates, for 50 concurrent processes, is less than 1 in 1 million.
     */
    protected static final String TEST_ID = Integer.toHexString(new Random().nextInt());

    protected static final boolean PROFILE = Boolean.getBoolean("profile");

    protected static final boolean DEBUG = Boolean.getBoolean("debug");

    /**
     * Controls the incremental load for each iteration
     */
    protected static final List<String> INCREMENTS = Splitter.on(",").trimResults()
                    .omitEmptyStrings().splitToList(System.getProperty("increments", "1,5,7,10"));

    protected static final Credentials CREDENTIALS = new SimpleCredentials("admin", "admin"
            .toCharArray());

    private PrintStream out;

    protected Map<String, ScalabilityBenchmark> benchmarks;

    /**
     * Variables per suite run
     */
    private Repository repository;

    private Credentials credentials;

    private LinkedList<Session> sessions;

    private ExecutionContext context;

    private Result result;

    private volatile boolean running;

    private List<Thread> threads;

    private RepositoryFixture fixture;

    public ScalabilityAbstractSuite() {
        this.benchmarks = newHashMap();
    }

    @Override
    public void run(Iterable<RepositoryFixture> fixtures) {
        for (RepositoryFixture fixture : fixtures) {
            try {
                Repository[] cluster = createRepository(fixture);
                try {
                    runSuite(fixture, cluster[0]);
                } finally {
                    fixture.tearDownCluster();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void runSuite(RepositoryFixture fixture, Repository repository) throws Exception {

        setUp(repository, fixture, CREDENTIALS);

        try {
            for (String increment : INCREMENTS) {
                context.setIncrement(Integer.parseInt(increment.trim()));

                if (DEBUG) {
                    System.out.println("Start load : " + increment);
                }
                // create the load for this iteration
                beforeIteration(context);

                warmup();

                if (DEBUG) {
                    System.out.println("Started test");
                }

                // Run one iteration
                runIteration(context);

                if (DEBUG) {
                    System.out.println("Finished test");
                }

                afterIteration();
            }
        } finally {
            tearDown();
        }
    }

    private void warmup() throws Exception {
        for (ScalabilityBenchmark benchmark : benchmarks.values()) {
            executeBenchmark(benchmark, context);
        }
    }

    /**
     * Setup any options before the benchmarks.
     * 
     * @throws Exception
     */
    protected void beforeSuite() throws Exception {}

    /**
     * Prepares this performance benchmark.
     * 
     * @param repository the repository to use
     * @param fixture credentials of a user with write access
     * @throws Exception if the benchmark can not be prepared
     */
    public void setUp(Repository repository, RepositoryFixture fixture, Credentials credentials)
            throws Exception {
        this.repository = repository;
        this.credentials = credentials;
        this.sessions = new LinkedList<Session>();
        this.fixture = fixture;
        this.threads = newArrayList();
        context = new ExecutionContext();
        result = new Result();

        beforeSuite();

        if (PROFILE) {
            context.startProfiler();
        }
    }

    /**
     * Cleanup after the benchmarks are run.
     * 
     * @throws Exception
     */
    protected void afterSuite() throws Exception {}

    /**
     * Cleans up after this performance benchmark.
     * 
     * @throws Exception if the benchmark can not be cleaned up
     */
    public void tearDown() throws Exception {

        context.stopProfiler();
        result.out();

        shutdownBackgroundJobs();

        afterSuite();

        for (Session session : sessions) {
            if (session.isLive()) {
                session.logout();
            }
        }

        this.threads = null;
        this.sessions = null;
        this.credentials = null;
        this.repository = null;
        this.context = null;
        this.result = null;
        this.benchmarks = null;
    }

    /**
     * Adds the benchmarks to run.
     * 
     * @param benchmarks
     * @return
     */
    public abstract ScalabilitySuite addBenchmarks(ScalabilityBenchmark... benchmarks);
    
    /**
     * Removes the benchmark.
     */
    @Override
    public boolean removeBenchmark(String benchmark) {
        return benchmarks.remove(benchmark) != null;
    }
    
    @Override
    public Map<String, ScalabilityBenchmark> getBenchmarks() {
        return benchmarks;
    }
    /**
     * Runs the benchmark.
     * 
     * @param benchmark
     * @throws Exception 
     */
    protected abstract void executeBenchmark(ScalabilityBenchmark benchmark,
            ExecutionContext context) throws Exception;

    /**
     * Runs the iteration of the benchmarks added.
     * 
     * @param context
     * @throws Exception 
     */
    private void runIteration(ExecutionContext context) throws Exception {
        Preconditions.checkArgument(benchmarks != null && !benchmarks.isEmpty(),
                "No Benchmarks configured");

        for (ScalabilityBenchmark benchmark : benchmarks.values()) {
            if (result.getBenchmarkStatistics(benchmark) == null) {
                result.addBenchmarkStatistics(benchmark, new SynchronizedDescriptiveStatistics());
            }

            Stopwatch watch = Stopwatch.createStarted();

            executeBenchmark(benchmark, context);

            watch.stop();
            result.getBenchmarkStatistics(benchmark).addValue(watch.elapsed(TimeUnit.MILLISECONDS));
            
            if (DEBUG) {
                System.out.println("Execution time for " + benchmark + "-"
                                            + watch.elapsed(TimeUnit.MILLISECONDS));
            }
        }
    }

    /**
     * Executes once for each iteration.
     * 
     * @param context the context
     * @throws Exception the repository exception
     */
    public void beforeIteration(ExecutionContext context) throws Exception {}

    /**
     * Run after all iterations of this test have been executed. Subclasses can
     * override this method to clean up static test content.
     * 
     * @throws Exception if an error occurs
     */
    protected void afterIteration() throws Exception {}

    /**
     * Adds a background thread that repeatedly executes the given job
     * until all the iterations of this test have been executed.
     * 
     * @param job background job
     */
    protected void addBackgroundJob(final Runnable job) {
        Thread thread = new Thread("Background job " + job) {
            @Override
            public void run() {
                while (running) {
                    job.run();
                }
            }
        };
        thread.start();
        threads.add(thread);
    }

    protected void shutdownBackgroundJobs() throws InterruptedException {
        this.running = false;
        for (Thread thread : threads) {
            thread.join();
        }
    }

    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        return fixture.setUpCluster(1);
    }

    /**
     * Returns a new writer session that will be automatically closed once
     * all the iterations of this test have been executed.
     * 
     * @return writer session
     */
    protected Session loginWriter() {
        try {
            Session session = repository.login(credentials);
            synchronized (sessions) {
                sessions.add(session);
            }
            return session;
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Collects the execution times for each benchmark.
     */
    class Result {
        private Map<ScalabilityBenchmark, DescriptiveStatistics> stats;

        public Result() {
            this.stats = newHashMap();
        }

        public void addBenchmarkStatistics(ScalabilityBenchmark benchmark,
                SynchronizedDescriptiveStatistics stat) {
            stats.put(benchmark, stat);
        }

        public DescriptiveStatistics getBenchmarkStatistics(ScalabilityBenchmark benchmark) {
            return stats.get(benchmark);
        }

        public void out() {
            Iterator<Entry<ScalabilityBenchmark, DescriptiveStatistics>> iter =
                    stats.entrySet().iterator();
            while (iter.hasNext()) {
                Entry<ScalabilityBenchmark, DescriptiveStatistics> entry = iter.next();
                DescriptiveStatistics statistics = entry.getValue();
                ScalabilityBenchmark benchmark = entry.getKey();

                System.out
                        .format(
                                "# %-26.26s       min     10%%     50%%     90%%     max       N%n",
                                benchmark.toString());
                if (out != null) {
                    out.format(
                            "# %-26.26s       min     10%%     50%%     90%%     max       N%n",
                            benchmark.toString());
                }

                System.out.format(
                        "%-30.30s  %6.0f  %6.0f  %6.0f  %6.0f  %6.0f  %6d%n",
                        fixture.toString(),
                        statistics.getMin(),
                        statistics.getPercentile(10.0),
                        statistics.getPercentile(50.0),
                        statistics.getPercentile(90.0),
                        statistics.getMax(),
                        statistics.getN());

                if (out != null) {
                    out.format(
                            "%-30.30s  %-6.0f  %-6.0f  %-6.0f  %-6.0f  %-6.0f  %-6d%n",
                            fixture.toString(),
                            statistics.getMin(),
                            statistics.getPercentile(10.0),
                            statistics.getPercentile(50.0),
                            statistics.getPercentile(90.0),
                            statistics.getMax(),
                            statistics.getN());
                }

                StringBuffer header = new StringBuffer();
                header.append("\t# %-26.26s");
                for (String increment : INCREMENTS) {
                    header.append("\t");
                    header.append(increment);
                }
                header.append("%n");
                System.out.format(header.toString(), "Iterations/Load");

                StringBuffer format = new StringBuffer();
                format.append("%-30.30s");
                System.out.format(format.toString(), "\t" + "Time (ms)");
                if (out != null) {
                    out.format(format.toString(), "\t" + "Time (ms)");
                }

                for (int idx = 0; idx < INCREMENTS.size(); idx++) {
                    format = new StringBuffer();
                    format.append("\t");
                    format.append("%-7.0f");
                    System.out.format(format.toString(), statistics.getValues()[idx]);
                    if (out != null) {
                        out.format(format.toString(), statistics.getValues()[idx]);
                    }
                }
                System.out.format("%n");
            }
        }
    }

    /**
     * Execution context to be pass information to and from the suite to the benchmarks.
     */
    class ExecutionContext {
        private Profiler profiler;
        private AtomicLong iteration = new AtomicLong();
        private Map<Object, Object> map = newConcurrentMap();

        protected void setIncrement(int increment) {
            iteration.getAndSet(increment);
        }

        public int getIncrement() {
            return iteration.intValue();
        }

        public void startProfiler() {
            if (PROFILE) {
                profiler = new Profiler().startCollecting();
            }
        }

        public void stopProfiler() {
            if (profiler != null) {
                System.out.println(profiler.stopCollecting().getTop(5));
                profiler = null;
            }
        }

        public Map<Object, Object> getMap() {
            return map;
        }

        public void setMap(Map<Object, Object> map) {
            this.map = map;
        }
    }

    @Override
    public String toString() {
        String name = getClass().getName();
        return name.substring(name.lastIndexOf('.') + 1);
    }

    @Override
    public void setPrintStream(PrintStream out) {
        this.out = out;
    }

    protected Repository getRepository() {
        return repository;
    }
}
