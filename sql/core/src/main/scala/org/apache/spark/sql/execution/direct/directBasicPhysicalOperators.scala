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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  Expression,
  GenericInternalRow,
  IsNotNull,
  NamedExpression,
  NullIntolerant,
  PredicateHelper,
  UnsafeProjection
}
import org.apache.spark.sql.catalyst.expressions.codegen.Predicate

case class ProjectDirectExec(projectList: Seq[NamedExpression], child: DirectPlan)
    extends UnaryDirectExecNode {

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def doExecute(): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      val project: UnsafeProjection = {
        val project = UnsafeProjection.create(projectList, child.output)
        project.initialize(0)
        project
      }
      val childIter: Iterator[InternalRow] = child.doExecute()

      override def hasNext: Boolean = {
        childIter.hasNext
      }

      override def next: InternalRow = {
        val numOutputRows = longMetric("numOutputRows", DirectSQLMetrics.createMetric())
        numOutputRows += 1
        project(childIter.next())
      }

    }
  }

}

case class LimitDirectExec(limit: Int, child: DirectPlan) extends UnaryDirectExecNode {
    override def doExecute(): Iterator[InternalRow] = {
      child.execute().take(limit)
    }
    override def output: Seq[Attribute] = child.output
  }

case class FilterDirectExec(condition: Expression, child: DirectPlan)
    extends UnaryDirectExecNode
    with PredicateHelper {

  // Split out all the IsNotNulls from condition.
  private val (notNullPreds, otherPreds) = splitConjunctivePredicates(condition).partition {
    case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
    case _ => false
  }

  // If one expression and its children are null intolerant, it is null intolerant.
  private def isNullIntolerant(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerant)
    case _ => false
  }

  // The columns that will filtered out by `IsNotNull` could be considered as not nullable.
  private val notNullAttributes = notNullPreds.flatMap(_.references).distinct.map(_.exprId)

  override def output: Seq[Attribute] = {
    child.output.map { a =>
      if (a.nullable && notNullAttributes.contains(a.exprId)) {
        a.withNullability(false)
      } else {
        a
      }
    }
  }

  override def doExecute(): Iterator[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows", DirectSQLMetrics.createMetric())
    new Iterator[InternalRow] {

      val predicate: Predicate = {
        val predicate: Predicate = newPredicate(condition, child.output)
        predicate.initialize(0)
        predicate
      }
      val childIter: Iterator[InternalRow] = child.doExecute()
      val DUMMY_ROW = new GenericInternalRow(null)
      var nextRow: InternalRow = DUMMY_ROW

      override def hasNext: Boolean = {
        while (childIter.hasNext) {
          nextRow = childIter.next()
          if (predicate.eval(nextRow)) {
            return true
          }
        }
        nextRow = DUMMY_ROW
        false
      }

      override def next: InternalRow = {
        if (nextRow != DUMMY_ROW || hasNext) {
          numOutputRows += 1
          nextRow
        } else {
          throw new NoSuchElementException
        }
      }
    }

  }

}
