/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.planner.projection.builder;

import io.crate.analyze.QuerySpec;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import sun.awt.util.IdentityArrayList;

/**
* Created by bd on 30.1.15.
*/
public class SplitPoints {
    private final IdentityArrayList<Symbol> toCollect;
    private final IdentityArrayList<Function> aggregates;
    private final IdentityArrayList<Symbol> leafes;
    private final QuerySpec querySpec;

    SplitPoints(QuerySpec querySpec) {
        int estimate = querySpec.outputs().size();
        this.querySpec = querySpec;
        this.toCollect = new IdentityArrayList<>(estimate);
        this.aggregates = new IdentityArrayList<>(estimate-1);
        this.leafes = new IdentityArrayList<>(estimate);
    }

    void allocateCollectSymbol(Symbol symbol){
        if (!toCollect.contains(symbol)) {
            toCollect.add(symbol);
        }
    }

    void allocateAggregate(Function aggregate){
        if (!aggregates.contains(aggregate)) {
            aggregates.add(aggregate);
        }
    }

    public QuerySpec querySpec() {
        return querySpec;
    }

    public IdentityArrayList<Symbol> toCollect() {
        return toCollect;
    }

    public IdentityArrayList<Function> aggregates() {
        return aggregates;
    }

    public IdentityArrayList<Symbol> leafes() {
        return leafes;
    }
}
