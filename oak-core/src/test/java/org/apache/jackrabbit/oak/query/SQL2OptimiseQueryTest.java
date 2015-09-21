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
package org.apache.jackrabbit.oak.query;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.of;
import static javax.jcr.query.Query.JCR_SQL2;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.query.QueryEngineImpl.ForceOptimised.CHEAPEST;
import static org.apache.jackrabbit.oak.query.QueryEngineImpl.ForceOptimised.OPTIMISED;
import static org.apache.jackrabbit.oak.query.QueryEngineImpl.ForceOptimised.ORIGINAL;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Test;

/**
 * aim to cover the various aspects of Query.optimise()
 */
public class SQL2OptimiseQueryTest extends  AbstractQueryTest {

    @Test
    public void orToUnions() throws RepositoryException, CommitFailedException {
        Tree test, t;
        List<String> original, optimised, cheapest, expected;
        String statement;
        
        test = root.getTree("/").addChild("test");
        test.setProperty(JCR_PRIMARYTYPE, NT_OAK_UNSTRUCTURED, NAME);
        t = addChildWithProperty(test, "a", "p", "a");
        t.setProperty("p1", "a1");
        t = addChildWithProperty(test, "b", "p", "b");
        t.setProperty("p1", "b1");
        addChildWithProperty(test, "c", "p", "c");
        addChildWithProperty(test, "d", "p", "d");
        addChildWithProperty(test, "e", "p", "e");
        root.commit();
        
        statement = String.format("SELECT * FROM [%s] WHERE p = 'a' OR p = 'b'",
            NT_OAK_UNSTRUCTURED);
        expected = of("/test/a", "/test/b");
        setForceOptimised(ORIGINAL);
        original = executeQuery(statement, JCR_SQL2, true);
        setForceOptimised(OPTIMISED);
        optimised = executeQuery(statement, JCR_SQL2, true);
        setForceOptimised(CHEAPEST);
        cheapest = executeQuery(statement, JCR_SQL2, true);
        assertOrToUnionResults(expected, original, optimised, cheapest);
        
        statement = String.format(
            "SELECT * FROM [%s] WHERE p = 'a' OR p = 'b' OR p = 'c' OR p = 'd' OR p = 'e' ",
            NT_OAK_UNSTRUCTURED);
        expected = of("/test/a", "/test/b", "/test/c", "/test/d", "/test/e");
        setForceOptimised(ORIGINAL);
        original = executeQuery(statement, JCR_SQL2, true);
        setForceOptimised(OPTIMISED);
        optimised = executeQuery(statement, JCR_SQL2, true);
        setForceOptimised(CHEAPEST);
        cheapest = executeQuery(statement, JCR_SQL2, true);
        assertOrToUnionResults(expected, original, optimised, cheapest);

        statement = String.format(
            "SELECT * FROM [%s] WHERE (p = 'a' OR p = 'b') AND (p1 = 'a1' OR p1 = 'b1')",
            NT_OAK_UNSTRUCTURED);
        expected = of("/test/a", "/test/b");
        setForceOptimised(ORIGINAL);
        original = executeQuery(statement, JCR_SQL2, true);
        setForceOptimised(OPTIMISED);
        optimised = executeQuery(statement, JCR_SQL2, true);
        setForceOptimised(CHEAPEST);
        cheapest = executeQuery(statement, JCR_SQL2, true);
        assertOrToUnionResults(expected, original, optimised, cheapest);

        statement = String.format(
            "SELECT * FROM [%s] WHERE (p = 'a' AND p1 = 'a1') OR (p = 'b' AND p1 = 'b1')",
            NT_OAK_UNSTRUCTURED);
        expected = of("/test/a", "/test/b");
        setForceOptimised(ORIGINAL);
        original = executeQuery(statement, JCR_SQL2, true);
        setForceOptimised(OPTIMISED);
        optimised = executeQuery(statement, JCR_SQL2, true);
        setForceOptimised(CHEAPEST);
        cheapest = executeQuery(statement, JCR_SQL2, true);
        assertOrToUnionResults(expected, original, optimised, cheapest);
    }
    
    private static void assertOrToUnionResults(@Nonnull List<String> expected, 
                                               @Nonnull List<String> original,
                                               @Nonnull List<String> optimised,
                                               @Nonnull List<String> cheapest) {
        // checks that all the three list are the expected content
        assertThat(checkNotNull(original), is(checkNotNull(expected)));        
        assertThat(checkNotNull(optimised), is(expected));
        assertThat(checkNotNull(cheapest), is(expected));
        
        // check that all the three lists contains the same. Paranoid but still a fast check
        assertThat(original, is(optimised));
        assertThat(optimised, is(cheapest));
        assertThat(cheapest, is(original));
    }

    private static Tree addChildWithProperty(@Nonnull Tree father, @Nonnull String name,
                                             @Nonnull String propName, @Nonnull String propValue) {
        Tree t = checkNotNull(father).addChild(checkNotNull(name));
        t.setProperty(JCR_PRIMARYTYPE, NT_OAK_UNSTRUCTURED, NAME);
        t.setProperty(checkNotNull(propName), checkNotNull(propValue));
        return t;
    }
    
    @Override
    protected ContentRepository createRepository() {
        return new Oak()
        .with(new OpenSecurityProvider())
        .with(new InitialContent())
        .with(new QueryEngineSettings() {
            @Override
            public boolean isSql2Optimisation() {
                // TODO Auto-generated method stub
                return true;
            }
        })
        .createContentRepository();
    }
}
