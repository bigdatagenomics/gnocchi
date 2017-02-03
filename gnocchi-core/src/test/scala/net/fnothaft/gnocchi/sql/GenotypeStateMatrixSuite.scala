/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
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
package net.fnothaft.gnocchi.sql

import net.fnothaft.gnocchi.models.GenotypeState
import org.scalatest.FunSuite
import scala.collection.JavaConverters._

class GenotypeStateMatrixSuite extends FunSuite {

  test("should filter out reference calls") {
    val gt = GenotypeState("1",
      1000L,
      1001L,
      "A",
      "G",
      "mySample",
      0,
      0)
    val row = GenotypeStateMatrix.filterAndJoin(gt, Map.empty)
    assert(row.isEmpty)
  }

  test("correctly process nonref calls") {
    val gt = GenotypeState("1",
      1000L,
      1001L,
      "A",
      "G",
      "mySample",
      2,
      0)
    val row = GenotypeStateMatrix.filterAndJoin(gt, Map(("mySample" -> 2),
      ("yourSample" -> 3)))
    assert(row.isDefined)
    val (_, (id, count)) = row.get
    assert(id === 2)
    assert(count > 1.99999 && count < 2.00001)
  }
}
