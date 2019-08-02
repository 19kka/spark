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

import java.util.concurrent.TimeUnit._

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.execution.{BlockingOperatorWithCodegen, CodegenSupport, SortPrefixUtils, UnsafeExternalRowSorter}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.{SparkEnv, TaskContext}

/**
 * Performs (external) sorting.
 *
 * @param global when true performs a global sort of all partitions by shuffling the data first
 *               if necessary.
 * @param testSpillFrequency Method for configuring periodic spilling in unit tests. If set, will
 *                           spill every `frequency` records.
 */
case class SortDirectExec(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: DirectPlan,
    testSpillFrequency: Int = 0)
  extends UnaryDirectExecNode {

  override def output: Seq[Attribute] = child.output

  def outputOrdering: Seq[SortOrder] = sortOrder

  // sort performed is local within a given partition so will retain
  // child operator's partitioning
//  override def outputPartitioning: Partitioning = child.outputPartitioning

  def requiredChildDistribution: Seq[Distribution] =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  private val enableRadixSort = sqlContext.conf.enableRadixSort


  def createSorter(): UnsafeExternalRowSorter = {
    val ordering = newOrdering(sortOrder, output)

    // The comparator for comparing prefix
    val boundSortExpression = BindReferences.bindReference(sortOrder.head, output)
    val prefixComparator = SortPrefixUtils.getPrefixComparator(boundSortExpression)

    val canUseRadixSort = enableRadixSort && sortOrder.length == 1 &&
      SortPrefixUtils.canSortFullyWithPrefix(boundSortExpression)

    // The generator for prefix
    val prefixExpr = SortPrefix(boundSortExpression)
    val prefixProjection = UnsafeProjection.create(Seq(prefixExpr))
    val prefixComputer = new UnsafeExternalRowSorter.PrefixComputer {
      private val result = new UnsafeExternalRowSorter.PrefixComputer.Prefix
      override def computePrefix(row: InternalRow):
          UnsafeExternalRowSorter.PrefixComputer.Prefix = {
        val prefix = prefixProjection.apply(row)
        result.isNull = prefix.isNullAt(0)
        result.value = if (result.isNull) prefixExpr.nullValue else prefix.getLong(0)
        result
      }
    }

    val pageSize = SparkEnv.get.memoryManager.pageSizeBytes
    val sorter = UnsafeExternalRowSorter.create(
      schema, ordering, prefixComparator, prefixComputer, pageSize, canUseRadixSort)

//    if (testSpillFrequency > 0) {
//      sorter.setTestSpillFrequency(testSpillFrequency)
//    }
    sorter
  }

  override def doExecute(): Iterator[InternalRow] = {

    val peakMemory = longMetric("peakMemory", DirectSQLMetrics.createSizeMetric())
    val spillSize = longMetric("spillSize", DirectSQLMetrics.createSizeMetric())
    val sortTime = longMetric("sortTime", DirectSQLMetrics.createTimingMetric())

    val iter = child.execute()

      val sorter = createSorter()

      val metrics = TaskContext.get().taskMetrics()

      // Remember spill data size of this task before execute this operator so that we can
      // figure out how many bytes we spilled for this operator.
      val spillSizeBefore = metrics.memoryBytesSpilled
      val sortedIterator = sorter.sort(iter.asInstanceOf[Iterator[UnsafeRow]])
      sortTime += NANOSECONDS.toMillis(sorter.getSortTimeNanos)
      peakMemory += sorter.getPeakMemoryUsage
      spillSize += metrics.memoryBytesSpilled - spillSizeBefore
      metrics.incPeakExecutionMemory(sorter.getPeakMemoryUsage)

      sortedIterator

  }

//  override def usedInputs: AttributeSet = AttributeSet(Seq.empty)
//
//  override def inputRDDs(): Seq[RDD[InternalRow]] = {
//    child.asInstanceOf[CodegenSupport].inputRDDs()
//  }

  // Name of sorter variable used in codegen.
  private var sorterVariable: String = _

//  override protected def doProduce(ctx: CodegenContext): String = {
//    val needToSort =
//      ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "needToSort", v => s"$v = true;")
//
//    // Initialize the class member variables. This includes the instance of the Sorter and
//    // the iterator to return sorted rows.
//    val thisPlan = ctx.addReferenceObj("plan", this)
//    // Inline mutable state since not many Sort operations in a task
//    sorterVariable = ctx.addMutableState(classOf[UnsafeExternalRowSorter].getName, "sorter",
//      v => s"$v = $thisPlan.createSorter();", forceInline = true)
//    val metrics = ctx.addMutableState(classOf[TaskMetrics].getName, "metrics",
//      v => s"$v = org.apache.spark.TaskContext.get().taskMetrics();", forceInline = true)
//    val sortedIterator = ctx.addMutableState("scala.collection.Iterator<UnsafeRow>", "sortedIter",
//      forceInline = true)
//
//    val addToSorter = ctx.freshName("addToSorter")
//    val addToSorterFuncName = ctx.addNewFunction(addToSorter,
//      s"""
//        | private void $addToSorter() throws java.io.IOException {
//        |   ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
//        | }
//      """.stripMargin.trim)
//
//    val outputRow = ctx.freshName("outputRow")
//    val peakMemory = metricTerm(ctx, "peakMemory")
//    val spillSize = metricTerm(ctx, "spillSize")
//    val spillSizeBefore = ctx.freshName("spillSizeBefore")
//    val sortTime = metricTerm(ctx, "sortTime")
//    s"""
//       | if ($needToSort) {
//       |   long $spillSizeBefore = $metrics.memoryBytesSpilled();
//       |   $addToSorterFuncName();
//       |   $sortedIterator = $sorterVariable.sort();
//       |   $sortTime.add($sorterVariable.getSortTimeNanos() / $NANOS_PER_MILLIS);
//       |   $peakMemory.add($sorterVariable.getPeakMemoryUsage());
//       |   $spillSize.add($metrics.memoryBytesSpilled() - $spillSizeBefore);
//       |   $metrics.incPeakExecutionMemory($sorterVariable.getPeakMemoryUsage());
//       |   $needToSort = false;
//       | }
//       |
//       | while ($limitNotReachedCond $sortedIterator.hasNext()) {
//       |   UnsafeRow $outputRow = (UnsafeRow)$sortedIterator.next();
//       |   ${consume(ctx, null, outputRow)}
//       |   if (shouldStop()) return;
//       | }
//     """.stripMargin.trim
//  }
//
//  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
//    s"""
//       |${row.code}
//       |$sorterVariable.insertRow((UnsafeRow)${row.value});
//     """.stripMargin
//  }
}
