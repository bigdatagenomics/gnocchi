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
package org.bdgenomics.gnocchi.api.java.models

import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.models.variant.{ LogisticVariantModel }
import org.bdgenomics.gnocchi.models.{ LogisticGnocchiModel }
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.sql.GnocchiSession

import scala.collection.JavaConversions._

class JavaLogisticGnocchiModel(val lgm: LogisticGnocchiModel) {
  def save(saveTo: java.lang.String): Unit = {
    lgm.save(saveTo)
  }

  def getVariantModels(): Dataset[LogisticVariantModel] = {
    lgm.variantModels
  }

  def getModelType(): java.lang.String = {
    lgm.allelicAssumption
  }

  def getPhenotype(): java.lang.String = {
    lgm.phenotypeName
  }

  def getCovariates(): java.lang.String = {
    lgm.covariatesNames.mkString(",")
  }

  def getNumSamples(): java.lang.Integer = {
    lgm.numSamples
  }
}
