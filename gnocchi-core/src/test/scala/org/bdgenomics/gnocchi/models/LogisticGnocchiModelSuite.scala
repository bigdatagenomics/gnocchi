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
package org.bdgenomics.gnocchi.models

import org.apache.spark.sql.SparkSession
import org.bdgenomics.gnocchi.models.variant.LogisticVariantModel
import org.bdgenomics.gnocchi.primitives.genotype.GenotypeState
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.sql.GnocchiSession._
import org.bdgenomics.gnocchi.utils.GnocchiFunSuite
import org.mockito.Mockito

import scala.collection.mutable

class LogisticGnocchiModelSuite extends GnocchiFunSuite {
  sparkTest("LogisticGnocchiModel creation works") {
    val ss = sc.sparkSession
    import ss.implicits._

    val variantModel =
      LogisticVariantModel(
        "rs8330247",
        14,
        21373362,
        "C",
        "T",
        List(0.21603874149667546, -0.20074885895765007, 0.21603874149667546, -0.20074885895765007))

    val variantModels = ss.createDataset(List(variantModel))

    try {
      LogisticGnocchiModel(
        variantModels,
        "pheno_1",
        List("covar_1", "covar_2"),
        Set("6432", "8569", "2411", "6238", "6517", "1939", "6571", "8414", "8327", "6629", "6582", "5600", "7276", "4288", "1890"),
        15,
        "ADDITIVE")
    } catch {
      case _: Throwable => fail("Error creating a LogisticGnocchiModel")
    }
  }
}
