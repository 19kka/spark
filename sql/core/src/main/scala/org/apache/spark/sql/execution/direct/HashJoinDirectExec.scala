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

import java.util.concurrent.TimeUnit.NANOSECONDS

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.joins.{DirectHashJoin, HashedRelation}

case class HashJoinDirectExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: DirectPlan,
    right: DirectPlan)
    extends BinaryDirectExecNode
    with DirectHashJoin {

  override def enumerator(): Enumerator[InternalRow] = {
    val buildEnumerator = left.enumerator()
    val streamedEnumerator = streamedPlan.enumerator()

    val buildDataSize = longMetric("buildDataSize", DirectSQLMetrics.createSizeMetric())
    val buildTime = longMetric("buildTime", DirectSQLMetrics.createTimingMetric())
    val start = System.nanoTime()
    val relation =
      HashedRelation(new EnumeratorIterator[InternalRow](buildEnumerator), buildKeys)
    buildTime += NANOSECONDS.toMillis(System.nanoTime() - start)
    buildDataSize += relation.estimatedSize

    DirectExecutionContext.get().addExecutionCompletionListener { _ =>
      relation.close()
    }
    val numOutputRows = longMetric("numOutputRows", DirectSQLMetrics.createMetric())

    val iterator = join(streamedPlan.execute(), relation, numOutputRows)
    new IterableEnumerator[InternalRow](iterator) {

      DirectExecutionContext.get().addExecutionCompletionListener { _ =>
        relation.close()
      }
      override def close(): Unit = {
        super.close()
        buildEnumerator.close()
        streamedEnumerator.close()
      }
    }
  }
}
