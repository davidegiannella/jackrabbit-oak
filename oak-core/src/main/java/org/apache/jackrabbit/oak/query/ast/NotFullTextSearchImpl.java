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
package org.apache.jackrabbit.oak.query.ast;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.query.index.FilterImpl;

public class NotFullTextSearchImpl extends FullTextSearchImpl {

    public NotFullTextSearchImpl(String selectorName, String propertyName,
                                 StaticOperandImpl fullTextSearchExpression) {
        super(selectorName, propertyName, fullTextSearchExpression);
    }

    public NotFullTextSearchImpl(FullTextSearchImpl ft) {
        this(ft.selectorName, ft.propertyName, ft.fullTextSearchExpression);
    }
    
    @Override
    String getRawText(PropertyValue v) {
        return "-" + super.getRawText(v);
    }

    @Override
    void restrictPropertyOnFilter(String propertyName, FilterImpl f) {
        // Intentionally left empty. A NOT CONTAINS() can be valid if the property is actually not
        // there.
    }

    @Override
    public String toString() {
        return "not " + super.toString();
    }
}
