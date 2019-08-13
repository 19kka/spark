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
import org.apache.spark.sql.execution.aggregate.{
  HashAggregateExec,
  ObjectHashAggregateExec,
  SortAggregateExec
}
import org.apache.spark.sql.execution.joins.{
  BroadcastNestedLoopJoinExec,
  CartesianProductExec,
  HashJoin,
  SortMergeJoinExec
}
import org.apache.spark.sql.execution.window.{WindowDirectExec, WindowExec}
object DirectPlanConverter {

  private def planSubqueries(plan: SparkPlan): SparkPlan = {
    plan.transformAllExpressions {
      case subquery: expressions.ScalarSubquery =>
        val directExecutedPlan =
          DirectPlanConverter.convert(new QueryExecution(
            DirectExecutionContext.get().activeSparkSession,
            subquery.plan).sparkPlan)
        ScalarDirectSubquery(
          SubqueryDirectExec(s"scalar-subquery#${subquery.exprId.id}", directExecutedPlan),
          subquery.exprId)
    }
  }

  private def ensureOrdering(operator: SparkPlan): SparkPlan = {
    operator.transformUp {
      case operator: SparkPlan =>
        var children: Seq[SparkPlan] = operator.children
        val requiredChildOrderings: Seq[Seq[SortOrder]] = operator.requiredChildOrdering
        children = children.zip(requiredChildOrderings).map {
          case (child, requiredOrdering) =>
            if (SortOrder.orderingSatisfies(child.outputOrdering, requiredOrdering)) {
              child
            } else {
              SortExec(requiredOrdering, global = false, child = child)
            }
        }
        operator.withNewChildren(children)
    }
  }

  def convert(plan: SparkPlan): DirectPlan = {
    var preparedSparkPlan = planSubqueries(plan)
    preparedSparkPlan = ensureOrdering(preparedSparkPlan)
    convertToDirectPlan(preparedSparkPlan)
  }

  private def convertToDirectPlan(plan: SparkPlan): DirectPlan = {
    plan match {
      // basic
      case ProjectExec(projectList, child) =>
        ProjectDirectExec(projectList, convertToDirectPlan(child))
      case FilterExec(condition, child) =>
        FilterDirectExec(condition, convertToDirectPlan(child))
      case DynamicLocalTableScanExec(output, name) =>
        LocalTableScanDirectExec(output, name)

      // join
      case hashJoin: HashJoin =>
        HashJoinDirectExec(
          hashJoin.leftKeys,
          hashJoin.rightKeys,
          hashJoin.joinType,
          hashJoin.condition,
          convertToDirectPlan(hashJoin.left),
          convertToDirectPlan(hashJoin.right))

      case sortMergeJoin: SortMergeJoinExec =>
        HashJoinDirectExec(
          sortMergeJoin.leftKeys,
          sortMergeJoin.rightKeys,
          sortMergeJoin.joinType,
          sortMergeJoin.condition,
          convertToDirectPlan(sortMergeJoin.left),
          convertToDirectPlan(sortMergeJoin.right))

      case broadcastNestedLoopJoinExec: BroadcastNestedLoopJoinExec =>
        DirectPlanAdapter(broadcastNestedLoopJoinExec)
      case cartesianProductExec: CartesianProductExec =>
        DirectPlanAdapter(cartesianProductExec)

      // window
      case windowExec: WindowExec =>
        WindowDirectExec(
          windowExec.windowExpression,
          windowExec.partitionSpec,
          windowExec.orderSpec,
          convertToDirectPlan(windowExec.child))

      // sort
      case sortExec: SortExec =>
        SortDirectExec(sortExec.sortOrder, convertToDirectPlan(sortExec.child))

      // aggregate
      case objectHashAggregateExec: ObjectHashAggregateExec =>
        ObjectHashAggregateDirectExec(
          objectHashAggregateExec.groupingExpressions,
          objectHashAggregateExec.aggregateExpressions,
          objectHashAggregateExec.aggregateAttributes,
          objectHashAggregateExec.initialInputBufferOffset,
          objectHashAggregateExec.resultExpressions,
          convertToDirectPlan(objectHashAggregateExec.child))

      case hashAggregateExec: HashAggregateExec =>
        HashAggregateDirectExec(
          hashAggregateExec.groupingExpressions,
          hashAggregateExec.aggregateExpressions,
          hashAggregateExec.aggregateAttributes,
          hashAggregateExec.initialInputBufferOffset,
          hashAggregateExec.resultExpressions,
          convertToDirectPlan(hashAggregateExec.child))

      case sortAggregateExec: SortAggregateExec =>
        SortAggregateDirectExec(
          sortAggregateExec.groupingExpressions,
          sortAggregateExec.aggregateExpressions,
          sortAggregateExec.aggregateAttributes,
          sortAggregateExec.initialInputBufferOffset,
          sortAggregateExec.resultExpressions,
          convertToDirectPlan(sortAggregateExec.child))

      case generateExec: GenerateExec =>
        GenerateDirectExec(
          generateExec.generator,
          generateExec.requiredChildOutput,
          generateExec.outer,
          generateExec.generatorOutput,
          convertToDirectPlan(generateExec.child))
      case expandExec: ExpandExec =>
        ExpandDirectExec(expandExec.projections,
          expandExec.output,
          convertToDirectPlan(expandExec.child))

      // TODO other
      case other =>
        // DirectPlanAdapter(other)
        throw new UnsupportedOperationException("can't convert this SparkPlan " + other)
    }
  }


}
