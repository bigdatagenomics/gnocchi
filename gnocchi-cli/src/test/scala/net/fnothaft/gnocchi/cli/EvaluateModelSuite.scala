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
package net.fnothaft.gnocchi.cli

import net.fnothaft.gnocchi.GnocchiFunSuite
import net.fnothaft.gnocchi.association._
import net.fnothaft.gnocchi.models.Association
import org.bdgenomics.formats.avro.Variant

class EvaluateModelSuite extends GnocchiFunSuite {
  sparkTest("Test Average Ensembler: Pass in potential SNP and values ") {
    val fakeVariant = new Variant()

    val weights = Array(-3.4495484, .0022939, .77701357, -0.5600314)
    val statistics = Map("weights" -> weights)
    val assoc = Association(fakeVariant, "", 0.0, statistics)

    val elem1 = (1.0, 1.0, assoc)
    val elem2 = (2.0, 1.0, assoc)
    val elem3 = (3.0, 1.0, assoc)

    val snpArray = Array(elem1, elem2, elem3)

    val res = Ensembler("AVG", snpArray)._1

    assert(res == 2.0)
  }

  sparkTest("Test MaxProb Ensembler: Pass in potential SNP and values ") {
    val fakeVariant = new Variant()

    val weights = Array(-3.4495484, .0022939, .77701357, -0.5600314)
    val statistics = Map("weights" -> weights)
    val assoc = Association(fakeVariant, "", 0.0, statistics)

    val elem1 = (1.0, 1.0, assoc)
    val elem2 = (2.0, 1.0, assoc)
    val elem3 = (3.0, 1.0, assoc)

    val snpArray = Array(elem1, elem2, elem3)

    val res = Ensembler("MAX_PROB", snpArray)._1

    assert(res == 3.0)
  }

  sparkTest("Test WeightedAverage Ensembler: Pass in potential SNP and values ") {
    val fakeVariant = new Variant()

    val weights = Array(-3.4495484, .0022939, .77701357, -0.5600314)
    val statistics = Map("weights" -> weights)
    val assoc = Association(fakeVariant, "", 0.0, statistics)

    val elem1 = (1.0, 1.0, assoc)
    val elem2 = (2.0, 1.0, assoc)
    val elem3 = (3.0, 1.0, assoc)

    val avg_weights = Array[Double](0.1, 0.3, 0.6)

    val snpArray = Array(elem1, elem2, elem3)

    val res = Ensembler("W_AVG", snpArray, avg_weights)._1

    assert(res == 2.5)
  }
}
