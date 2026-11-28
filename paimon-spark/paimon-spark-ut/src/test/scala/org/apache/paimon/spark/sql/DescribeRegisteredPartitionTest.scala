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

package org.apache.paimon.spark.sql

import org.apache.paimon.catalog.Identifier
import org.apache.paimon.spark.PaimonSparkTestWithRestCatalogBase

import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException

import java.util.Collections

/** DESCRIBE ... PARTITION of a partition the REST catalog lists but no snapshot holds. */
class DescribeRegisteredPartitionTest extends PaimonSparkTestWithRestCatalogBase {

  test("Paimon describe: describe a partition registered before its first write") {
    withTable("dp_registered") {
      sql("""
            |CREATE TABLE dp_registered (id INT, p STRING) PARTITIONED BY (p)
            |TBLPROPERTIES ('metastore.partitioned-table' = 'true')
            |""".stripMargin)
      sql("INSERT INTO dp_registered VALUES (1, 'a')")
      // The test server forgets partitions without statistics at each commit, so register the
      // partition after the last one.
      paimonCatalog.createPartitions(
        Identifier.create(dbName0, "dp_registered"),
        Collections.singletonList(Collections.singletonMap("p", "registered")))
      assert(
        paimonCatalog
          .listPartitions(Identifier.create(dbName0, "dp_registered"))
          .size() == 2)

      val details =
        partitionDetailsOf("DESCRIBE FORMATTED dp_registered PARTITION (p = 'registered')")
      assert(details("Partition Values") == "[p=registered]", details)
      assert(details("Created Time") == "UNKNOWN", details)
      assert(!details.contains("Partition Statistics"), details)
      assert(sql("DESCRIBE dp_registered PARTITION (p = 'registered')").collect().nonEmpty)

      // The catalog lists the partitions of the current snapshot only.
      withSparkSQLConf("spark.paimon.scan.snapshot-id" -> "1") {
        intercept[NoSuchPartitionException] {
          sql("DESCRIBE dp_registered PARTITION (p = 'registered')")
        }
      }
      intercept[NoSuchPartitionException] {
        sql("DESCRIBE dp_registered PARTITION (p = 'missing')")
      }
    }
  }

  /** The "# Detailed Partition Information" rows of a DESCRIBE statement, by row name. */
  private def partitionDetailsOf(statement: String): Map[String, String] = {
    val rows = sql(statement).collect().map(row => (row.getString(0), row.getString(1))).toSeq
    val header = rows.indexWhere(_._1 == "# Detailed Partition Information")
    assert(header >= 0, rows.mkString("\n"))
    rows.drop(header + 1).takeWhile(_._1.nonEmpty).toMap
  }
}
