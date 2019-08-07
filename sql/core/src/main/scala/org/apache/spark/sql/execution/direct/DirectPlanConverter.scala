/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.direct

import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.direct.window.WindowDirectExec
import org.apache.spark.sql.execution.joins.{BroadcastNestedLoopJoinExec, CartesianProductExec, HashJoin, SortMergeJoinExec}
import org.apache.spark.sql.execution.window.WindowExec


object DirectPlanConverter {

  def convert(plan: SparkPlan): DirectPlan = {

    val plan0 = plan.transformAllExpressions {
      case subquery: expressions.ScalarSubquery =>
        val directExecutedPlan =
          DirectPlanConverter.convert(new QueryExecution(
            DirectExecutionContext.get().activeSparkSession,
            subquery.plan).sparkPlan)
        ScalarDirectSubquery(
          SubqueryDirectExec(s"scalar-subquery#${subquery.exprId.id}", directExecutedPlan),
          subquery.exprId)
    }

    val plan1 = plan0.transformUp {
    case operator: SparkPlan =>
      var children: Seq[SparkPlan] = operator.children
      val requiredChildOrderings: Seq[Seq[SortOrder]] = operator.requiredChildOrdering
      children = children.zip(requiredChildOrderings).map {
        case (child, requiredOrdering) =>
          // If child.outputOrdering already satisfies the requiredOrdering, we do not need to sort.
          if (SortOrder.orderingSatisfies(child.outputOrdering, requiredOrdering)) {
            child
          } else {
            SortExec(requiredOrdering, global = false, child = child)
          }
      }
      operator.withNewChildren(children)
    }

    plan1 match {
      // basic
      case ProjectExec(projectList, child) =>
        ProjectDirectExec(projectList, convert(child))
      case FilterExec(condition, child) =>
        FilterDirectExec(condition, convert(child))
      case DynamicLocalTableScanExec(output, name) =>
        LocalTableScanDirectExec(output, name)
      case CollectLimitExec(limit, child) =>
        LimitDirectExec(limit, convert(child))

      // join
      case hashJoin: HashJoin =>
        HashJoinDirectExec(
          hashJoin.leftKeys,
          hashJoin.rightKeys,
          hashJoin.joinType,
          hashJoin.condition,
          convert(hashJoin.left),
          convert(hashJoin.right))

      case sortMergeJoin: SortMergeJoinExec =>
        HashJoinDirectExec(
          sortMergeJoin.leftKeys,
          sortMergeJoin.rightKeys,
          sortMergeJoin.joinType,
          sortMergeJoin.condition,
          convert(sortMergeJoin.left),
          convert(sortMergeJoin.right))

      case broadcastNestedLoopJoinExec: BroadcastNestedLoopJoinExec =>
        DirectPlanAdapter(broadcastNestedLoopJoinExec)
      case cartesianProductExec: CartesianProductExec =>
        DirectPlanAdapter(cartesianProductExec)

      // aggregate
      case objectHashAggregateExec: ObjectHashAggregateExec =>
        ObjectHashAggregateDirectExec(
          objectHashAggregateExec.groupingExpressions,
          objectHashAggregateExec.aggregateExpressions,
          objectHashAggregateExec.aggregateAttributes,
          objectHashAggregateExec.initialInputBufferOffset,
          objectHashAggregateExec.resultExpressions,
          convert(objectHashAggregateExec.child))

      case hashAggregateExec: HashAggregateExec =>
        HashAggregateDirectExec(
          hashAggregateExec.groupingExpressions,
          hashAggregateExec.aggregateExpressions,
          hashAggregateExec.aggregateAttributes,
          hashAggregateExec.initialInputBufferOffset,
          hashAggregateExec.resultExpressions,
          convert(hashAggregateExec.child))

      case windowExec: WindowExec =>
        WindowDirectExec(
          windowExec.windowExpression,
          windowExec.partitionSpec,
          windowExec.orderSpec,
          convert(windowExec.child))

      case sortAggregateExec: SortAggregateExec =>
        SortAggregateDirectExec(
          sortAggregateExec.groupingExpressions,
          sortAggregateExec.aggregateExpressions,
          sortAggregateExec.aggregateAttributes,
          sortAggregateExec.initialInputBufferOffset,
          sortAggregateExec.resultExpressions,
          convert(sortAggregateExec.child))
      case sortExec: SortExec =>
        SortDirectExec(sortExec.sortOrder,
          sortExec.global,
          convert(sortExec.child),
          sortExec.testSpillFrequency)

      // TODO other
      case other =>
        // DirectPlanAdapter(other)
        throw new UnsupportedOperationException("can't convert this SparkPlan " + other)
    }
  }
}
