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

package io.crate.planner.consumer;

import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedTable;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.analyze.where.WhereClauseContext;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.QueryThenFetchNode;

public class QueryThenFetchConsumer implements Consumer {

    private final Visitor visitor;

    public QueryThenFetchConsumer(AnalysisMetaData analysisMetaData) {
        visitor = new Visitor(analysisMetaData);
    }

    @Override
    public boolean consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        PlannedAnalyzedRelation plannedAnalyzedRelation = visitor.process(rootRelation, context);
        if (plannedAnalyzedRelation == null) {
            return false;
        }
        context.rootRelation(plannedAnalyzedRelation);
        return true;
    }

    private static class Visitor extends AnalyzedRelationVisitor<ConsumerContext, PlannedAnalyzedRelation> {

        private final AnalysisMetaData analysisMetaData;

        public Visitor(AnalysisMetaData analysisMetaData) {
            this.analysisMetaData = analysisMetaData;
        }

        @Override
        public PlannedAnalyzedRelation visitQueriedTable(QueriedTable table, ConsumerContext context) {
            if (table.querySpec().hasAggregates() || table.querySpec().groupBy()!=null) {
                return null;
            }
            TableInfo tableInfo = table.tableRelation().tableInfo();
            if (tableInfo.schemaInfo().systemSchema() || tableInfo.rowGranularity() != RowGranularity.DOC) {
                return null;
            }

            WhereClause where = table.querySpec().where();
            if (where != null){
                WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, table.tableRelation());
                WhereClauseContext whereClauseContext = whereClauseAnalyzer.analyze(where);
                if(whereClauseContext.whereClause().version().isPresent()){
                    context.validationException(new VersionInvalidException());
                    return null;
                }
                where = whereClauseContext.whereClause();
                if (where.noMatch()){
                    return new NoopPlannedAnalyzedRelation(table);
                }
            }
            OrderBy orderBy = table.querySpec().orderBy();
            if (orderBy == null){
                return new QueryThenFetchNode(
                        tableInfo.getRouting(where, null),
                        table.querySpec().outputs(),
                        null, null, null,
                        table.querySpec().limit(),
                        table.querySpec().offset(),
                        where,
                        tableInfo.partitionedByColumns()
                );
            } else {
                table.tableRelation().validateOrderBy(orderBy);
                return new QueryThenFetchNode(
                        tableInfo.getRouting(where, null),
                        table.querySpec().outputs(),
                        orderBy.orderBySymbols(),
                        orderBy.reverseFlags(),
                        orderBy.nullsFirst(),
                        table.querySpec().limit(),
                        table.querySpec().offset(),
                        where,
                        tableInfo.partitionedByColumns()
                );
            }
        }

        @Override
        protected PlannedAnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, ConsumerContext context) {
            return null;
        }
    }
}
