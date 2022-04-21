/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;

/** Rule that rewrites Join on TableFunctionScan to Correlate. */
public class JoinTableFunctionScanToCorrelateRule extends RelRule<RelRule.Config> {

    private static final Config RULE_CONFIG =
            Config.EMPTY
                    .withOperandSupplier(
                            b0 ->
                                    b0.operand(LogicalJoin.class)
                                            .inputs(
                                                    b1 -> b1.operand(RelNode.class).anyInputs(),
                                                    b2 ->
                                                            b2.operand(
                                                                            LogicalTableFunctionScan
                                                                                    .class)
                                                                    .noInputs()))
                    .withDescription("JoinTableFunctionScanToCorrelateRule");

    public static final JoinTableFunctionScanToCorrelateRule INSTANCE =
            new JoinTableFunctionScanToCorrelateRule();

    private JoinTableFunctionScanToCorrelateRule() {
        super(RULE_CONFIG);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        RelNode leftInput = call.rel(1);
        LogicalTableFunctionScan logicalTableFunctionScan = call.rel(2);
        RelNode correlate =
                call.builder()
                        .push(leftInput)
                        .push(logicalTableFunctionScan)
                        .correlate(join.getJoinType(), join.getCluster().createCorrel())
                        .build();
        call.transformTo(correlate);
    }
}
