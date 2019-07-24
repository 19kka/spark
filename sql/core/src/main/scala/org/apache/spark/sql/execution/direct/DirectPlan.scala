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

import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import org.codehaus.commons.compiler.CompileException
import org.codehaus.janino.InternalCompilerException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Ascending,
  Attribute,
  AttributeSet,
  BoundReference,
  Expression,
  InterpretedPredicate,
  MutableProjection,
  SortOrder
}
import org.apache.spark.sql.catalyst.expressions.codegen.{Predicate => GenPredicate, _}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.DataType

abstract class DirectPlan
    extends QueryPlan[DirectPlan]
    with Enumerable[InternalRow]
    with Logging {

  /**
   * A handle to the SQL Context that was used to create this plan. Since many operators need
   * access to the sqlContext for RDD operations or configuration this field is automatically
   * populated by the query planning infrastructure.
   */
  @transient final val sqlContext = SparkSession.getActiveSession.map(_.sqlContext).orNull

  protected def sparkContext = sqlContext.sparkContext

  // sqlContext will be null when SparkPlan nodes are created without the active sessions.
  val subexpressionEliminationEnabled: Boolean = if (sqlContext != null) {
    sqlContext.conf.subexpressionEliminationEnabled
  } else {
    false
  }

  // whether we should fallback when hitting compilation errors caused by codegen
  private val codeGenFallBack = (sqlContext == null) || sqlContext.conf.codegenFallback

  /**
   * @return All metrics containing metrics of this SparkPlan.
   */
  def metrics: Map[String, SQLMetric] = Map.empty

  /**
   * Resets all the metrics.
   */
  def resetMetrics(): Unit = {
    metrics.valuesIterator.foreach(_.reset())
  }

  /**
   * @return [[SQLMetric]] for the `name`.
   */
  def longMetric(name: String): SQLMetric = metrics(name)

  def prepare(): Unit = children.foreach(_.prepare())

  def execute(): Iterator[InternalRow] = {
    new EnumeratorIterator[InternalRow](enumerator())
  }

  protected def newMutableProjection(
      expressions: Seq[Expression],
      inputSchema: Seq[Attribute],
      useSubexprElimination: Boolean = false): MutableProjection = {
    log.debug(s"Creating MutableProj: $expressions, inputSchema: $inputSchema")
    MutableProjection.create(expressions, inputSchema)
  }

  private def genInterpretedPredicate(
      expression: Expression,
      inputSchema: Seq[Attribute]): InterpretedPredicate = {
    val str = expression.toString
    val logMessage = if (str.length > 256) {
      str.substring(0, 256 - 3) + "..."
    } else {
      str
    }
    logWarning(s"Codegen disabled for this expression:\n $logMessage")
    InterpretedPredicate.create(expression, inputSchema)
  }

  protected def newPredicate(
      expression: Expression,
      inputSchema: Seq[Attribute]): GenPredicate = {
    try {
      GeneratePredicate.generate(expression, inputSchema)
    } catch {
      case _ @(_: InternalCompilerException | _: CompileException) if codeGenFallBack =>
        genInterpretedPredicate(expression, inputSchema)
    }
  }

  protected def newOrdering(
      order: Seq[SortOrder],
      inputSchema: Seq[Attribute]): Ordering[InternalRow] = {
    GenerateOrdering.generate(order, inputSchema)
  }

  /**
   * Creates a row ordering for the given schema, in natural ascending order.
   */
  protected def newNaturalAscendingOrdering(dataTypes: Seq[DataType]): Ordering[InternalRow] = {
    val order: Seq[SortOrder] = dataTypes.zipWithIndex.map {
      case (dt, index) => SortOrder(BoundReference(index, dt, nullable = true), Ascending)
    }
    newOrdering(order, Seq.empty)
  }
}

trait LeafDirectExecNode extends DirectPlan {
  override final def children: Seq[DirectPlan] = Nil
  override def producedAttributes: AttributeSet = outputSet
}

object UnaryDirectExecNode {
  def unapply(a: Any): Option[(DirectPlan, DirectPlan)] = a match {
    case s: DirectPlan if s.children.size == 1 => Some((s, s.children.head))
    case _ => None
  }
}

trait UnaryDirectExecNode extends DirectPlan {
  def child: DirectPlan

  override final def children: Seq[DirectPlan] = child :: Nil
}

trait BinaryDirectExecNode extends DirectPlan {
  def left: DirectPlan
  def right: DirectPlan

  override final def children: Seq[DirectPlan] = Seq(left, right)
}

case class DirectPlanAdapter(sparkPlan: SparkPlan) extends DirectPlan {

  override def output: Seq[Attribute] = sparkPlan.output

  override def children: Seq[DirectPlan] = sparkPlan.children.map(DirectPlanConverter.convert)

  override def enumerator(): Enumerator[InternalRow] = {
    val s = new Stopwatch().start()
    val r = sparkPlan.executeCollect()
    s.stop()
    println(
      "sparkPlan execute spend " + s.elapsed(TimeUnit.MICROSECONDS) * 0.001 + ", " + sparkPlan)

    new IterableEnumerator[InternalRow](r.toIterator)
  }

}
