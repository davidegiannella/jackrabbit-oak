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

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import javax.annotation.Nonnull;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.security.auth.Subject;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.fixture.JcrCreator;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.util.Text;

/**
 * Test the performance of removing members from groups. The
 * following parameters can be used to run the benchmark:
 *
 * - numberOfMembers : numberOfMembers : the number of members that should be
 *   added in the test setup to each group and later on removed during the test-run.
 *
 * Note the members to be removed are picked randomly and may or may not/no longer
 * be member of the target group.
 */
public class RemoveMemberTest extends AbstractTest {

    static final String REL_TEST_PATH = "testPath";
    static final String USER = "user";
    static final String GROUP = "group";
    static final int GROUP_CNT = 100;

    final Random random = new Random();
    final int numberOfMembers;

    private final List<String> groupPaths = new ArrayList(GROUP_CNT);

    public RemoveMemberTest(int numberOfMembers) {
        this.numberOfMembers = numberOfMembers;
    }

    @Override
    public void beforeSuite() throws Exception {
        super.beforeSuite();

        Session s = loginAdministrative();
        try {
            UserManager userManager = ((JackrabbitSession) s).getUserManager();
            createUsers(userManager);

            for (int i = 0; i <= GROUP_CNT; i++) {
                Group g = userManager.createGroup(new PrincipalImpl(GROUP + i), REL_TEST_PATH);
                groupPaths.add(g.getPath());

                List<String> ids = new ArrayList<String>();
                for (int j = 0; j <= numberOfMembers; j++) {
                    ids.add(USER + j);
                }
                g.addMembers(ids.toArray(new String[ids.size()]));
                s.save();
            }
        } finally {
            s.logout();
        }
        System.out.println("setup done");
    }


    protected void createUsers(@Nonnull UserManager userManager) throws Exception {

    }

    @Override
    public void afterSuite() throws Exception {
        Session s = loginAdministrative();
        try {
            Authorizable authorizable = ((JackrabbitSession) s).getUserManager().getAuthorizable(GROUP + "0");
            if (authorizable != null) {
                Node n = s.getNode(Text.getRelativeParent(authorizable.getPath(), 1));
                n.remove();
            }

            // remove test-users if they have been created
            authorizable = ((JackrabbitSession) s).getUserManager().getAuthorizable(USER + "0");
            if (authorizable != null) {
                Node n = s.getNode(Text.getRelativeParent(authorizable.getPath(), 1));
                n.remove();
            }

            s.save();
        } finally {
            s.logout();
        }
    }

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            return ((OakRepositoryFixture) fixture).setUpCluster(1, new JcrCreator() {
                @Override
                public Jcr customize(Oak oak) {
                    SecurityProvider sp = new SecurityProviderImpl(ConfigurationParameters.of(UserConfiguration.NAME,
                            ConfigurationParameters.of(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_BESTEFFORT)));
                    return new Jcr(oak).with(sp);
                }
            });
        } else {
            return super.createRepository(fixture);
        }
    }

    @Override
    public void runTest() throws Exception {
        Session s = null;
        try {
            // use system session login to avoid measuring the login-performance here
            s = Subject.doAsPrivileged(SystemSubject.INSTANCE, new PrivilegedExceptionAction<Session>() {
                @Override
                public Session run() throws Exception {
                    return getRepository().login(null, null);
                }
            }, null);
            UserManager userManager = ((JackrabbitSession) s).getUserManager();
            String groupPath = groupPaths.get(random.nextInt(GROUP_CNT));
            Group g = (Group) userManager.getAuthorizableByPath(groupPath);
            removeMembers(userManager, g, s);
        } catch (RepositoryException e) {
            if (s.hasPendingChanges()) {
                s.refresh(false);
            }
        } finally {
            if (s != null) {
                s.logout();
            }
        }
    }

    protected void removeMembers(@Nonnull UserManager userManger, @Nonnull Group group, @Nonnull Session s) throws Exception {
        for (int i = 0; i <= numberOfMembers; i++) {
            Authorizable member = userManger.getAuthorizable(USER + random.nextInt(numberOfMembers));
            if (group.removeMember(member)) {
                s.save();
            }
        }
    }
}
