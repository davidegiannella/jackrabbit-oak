/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.observation.filter;

import static java.util.Collections.disjoint;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.plugins.observation.ChangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChangeSetFilterImpl implements ChangeSetFilter {

    private static final Logger LOG = LoggerFactory.getLogger(ChangeSetFilterImpl.class);
    
    private final Set<String> rootIncludePaths;
    private final Set<Pattern> includePathPatterns;
    private final Set<Pattern> excludePathPatterns;
    private final Set<String> parentNodeNames;
    private final Set<String> parentNodeTypes;
    private final Set<String> propertyNames;
    
    @Override
    public String toString() {
        return "ChangeSetFilterImpl[rootIncludePaths="+rootIncludePaths+", includePathPatterns="+includePathPatterns+
                ", excludePathPatterns="+excludePathPatterns+", parentNodeNames="+parentNodeNames+", parentNodeTypes="+
                parentNodeTypes+", propertyNames="+propertyNames+"]";
    }

    public ChangeSetFilterImpl(@Nonnull Set<String> includedParentPaths, boolean isDeep, Set<String> excludedParentPaths,
            Set<String> parentNodeNames, Set<String> parentNodeTypes, Set<String> propertyNames) {
        this(includedParentPaths, isDeep, null, excludedParentPaths, parentNodeNames, parentNodeTypes, propertyNames);
    }

    public ChangeSetFilterImpl(@Nonnull Set<String> includedParentPaths, boolean isDeep,
            @Nullable Set<String> additionalIncludedParentPaths, Set<String> excludedParentPaths,
            Set<String> parentNodeNames, Set<String> parentNodeTypes, Set<String> propertyNames) {
        this.rootIncludePaths = new HashSet<String>();
        this.includePathPatterns = new HashSet<Pattern>();
        for (String aRawIncludePath : includedParentPaths) {
            final String aGlobbingIncludePath;
            if (aRawIncludePath.contains("*")) {
                // then isDeep is not applicable, it is already a glob path
                aGlobbingIncludePath = aRawIncludePath;
            } else {
                aGlobbingIncludePath = !isDeep ? aRawIncludePath : concat(aRawIncludePath, "**");
            }
            this.rootIncludePaths.add(aRawIncludePath);
            this.includePathPatterns.add(asPattern(aGlobbingIncludePath));
        }
        if (additionalIncludedParentPaths != null) {
            for (String path : additionalIncludedParentPaths) {
                this.rootIncludePaths.add(path);
                this.includePathPatterns.add(asPattern(path));
            }
        }
        this.excludePathPatterns = new HashSet<Pattern>();
        for (String aRawExcludePath : excludedParentPaths) {
            this.excludePathPatterns.add(asPattern(concat(aRawExcludePath, "**")));
        }
        this.propertyNames = propertyNames == null ? null : new HashSet<String>(propertyNames);
        this.parentNodeTypes = parentNodeTypes == null ? null : new HashSet<String>(parentNodeTypes);
        this.parentNodeNames = parentNodeNames == null ? null : new HashSet<String>(parentNodeNames);
    }
    
    /** for testing only **/
    public Set<String> getRootIncludePaths() {
        return rootIncludePaths;
    }

    private Pattern asPattern(String patternWithGlobs) {
        return Pattern.compile(GlobbingPathHelper.globPathAsRegex(patternWithGlobs));
    }

    @Override
    public boolean excludes(ChangeSet changeSet) {
        try{
            return doExcludes(changeSet);
        } catch(Exception e) {
            LOG.warn("excludes: got an Exception while evaluating excludes: " + e.getMessage() + 
                    ", changeSet=" + changeSet, e);
            return false; // false is the safer option
        }
    }
    
    private boolean doExcludes(ChangeSet changeSet) {
        if (changeSet.anyOverflow()) {
            // in case of an overflow we could
            // either try to still determine include/exclude based on non-overflown
            // sets - or we can do a fail-stop and determine this as too complex
            // to try-to-exclude, and just include
            //TODO: optimize this later
            return false;
        }
        final Set<String> parentPaths = new HashSet<String>(changeSet.getParentPaths());

        // first go through excludes to remove those that are explicitly
        // excluded
        if (this.excludePathPatterns.size() != 0) {
            final Iterator<String> it = parentPaths.iterator();
            while (it.hasNext()) {
                final String aParentPath = it.next();
                if (patternsMatch(this.excludePathPatterns, aParentPath)) {
                    // if an exclude pattern matches, remove the parentPath
                    it.remove();
                }
            }
        }
        // note that cut-off paths are not applied with excludes,
        // eg if excludePaths contains /var/foo/bar and path contains /var/foo
        // with a maxPathLevel of 2, that might very well mean that
        // the actual path would have been /var/foo/bar, but we don't know.
        // so we cannot exclude it here and thus have a potential false negative
        // (ie we didn't exclude it in the prefilter)

        // now remainingPaths contains what is not excluded,
        // then check if it is included
        boolean included = false;
        for (String aPath : parentPaths) {
            // direct set contains is fastest, lets try that first
            if (this.rootIncludePaths.contains(aPath)) {
                included = true;
                break;
            }
            if (patternsMatch(this.includePathPatterns, aPath)) {
                included = true;
                break;
            }
        }

        if (!included) {
            // well then we can definitely say that this commit is excluded
            return true;
        }

        if (this.propertyNames != null && this.propertyNames.size() != 0) {
            if (disjoint(changeSet.getPropertyNames(), this.propertyNames)) {
                // if propertyNames are defined then if we can't find any
                // at this stage (if !included) then this equals to filtering out
                return true;
            }
            // otherwise we have found a match, but one of the
            // nodeType/nodeNames
            // could still filter out, so we have to continue...
        }

        if (this.parentNodeTypes != null && this.parentNodeTypes.size() != 0) {
            if (disjoint(changeSet.getParentNodeTypes(), this.parentNodeTypes)) {
                // same story here: if nodeTypes is defined and we can't find any
                // match
                // then we're done now
                return true;
            }
            // otherwise, again, continue
        }

        if (this.parentNodeNames != null && this.parentNodeNames.size() != 0) {
            // and a 3rd time, if we can't find any nodeName match
            // here, then we're filtering out
            if (disjoint(changeSet.getParentNodeNames(), this.parentNodeNames)) {
                return true;
            }
        }

        // at this stage we haven't found any exclude, so we're likely including
        return false;
    }

    private static boolean patternsMatch(Set<Pattern> pathPatterns, String path) {
        if (path == null) {
            return false;
        }
        for (Pattern pathPattern : pathPatterns) {
            if (pathPattern.matcher(path).matches()) {
                return true;
            }
        }
        return false;
    }
}
