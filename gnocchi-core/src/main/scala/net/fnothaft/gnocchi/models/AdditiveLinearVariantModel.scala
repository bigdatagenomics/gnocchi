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
package net.fnothaft.gnocchi.models

import breeze.linalg.{ DenseVector => BreezeDense }
import net.fnothaft.gnocchi.association.AdditiveLinearAssociation
import net.fnothaft.gnocchi.gnocchiModel.BuildAdditiveLinearVariantModel
import org.bdgenomics.adam.models.ReferenceRegion

class AdditiveLinearVariantModel extends LinearVariantModel {

  var modelType = "Additive Linear Variant Model"
  val regressionName = "Additive Linear Regression"

  def update(observations: Array[(Double, Array[Double])],
             locus: ReferenceRegion,
             altAllele: String,
             phenotype: String): Unit = {

    val clippedObs = BuildAdditiveLinearVariantModel.arrayClipOrKeepState(observations)
    val assoc = AdditiveLinearAssociation.regressSite(clippedObs, variant, phenotype)
    if (assoc.statistics.nonEmpty) {
      assoc.statistics = assoc.statistics + ("numSamples" -> observations.length)
      val numNewSamples = observations.length
      val totalSamples = numSamples + numNewSamples
      val oldWeight = numSamples.toDouble / totalSamples.toDouble
      val newWeight = 1.0 - oldWeight
      val newWeights = BreezeDense(assoc.statistics("weights").asInstanceOf[Array[Double]])
      weights = (oldWeight * BreezeDense(weights) + newWeight * newWeights).toArray
      numSamples = totalSamples
    }
  }
}

//object BuildAdditiveLinearVariantModel extends BuildVariantModel with LinearSiteRegression with AdditiveVariant {
//  def compute(observations: Array[(Double, Array[Double])],
//              locus: ReferenceRegion,
//              altAllele: String,
//              phenotype: String): Association = {
//
//    val clippedObs = arrayClipOrKeepState(observations)
//    regressSite(observations, locus, altAllele, phenotype)
//  }
//
//  //  def extractVariantModel(assoc: Association): VariantModel = {
//  //
//  //    // code for extracting the VariantModel from the Association
//  //
//  //  }
//
//  val regressionName = "Additive Linear Regression"
//}

//object BuildDominantLinearVariantModel extends BuildVariantModel with LinearSiteRegression with DominantVariant {
//  def compute(observations: Array[(Double, Array[Double])],
//              locus: ReferenceRegion,
//              altAllele: String,
//              phenotype: String): Association = {
//
//    val clippedObs = arrayClipOrKeepState(observations)
//    regressSite(observations, locus, altAllele, phenotype)
//  }
//
//  //  def extractVariantModel(assoc: Association): VariantModel = {
//  //
//  //    // code for extracting the VariantModel from the Association
//  //
//  //  }
//
//  val regressionName = "Dominant Linear Regression"
//}

