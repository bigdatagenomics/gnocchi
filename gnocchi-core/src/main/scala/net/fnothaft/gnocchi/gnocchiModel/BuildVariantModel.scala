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
package net.fnothaft.gnocchi.gnocchiModel

import net.fnothaft.gnocchi.algorithms.siteregression.{LinearSiteRegression, LogisticSiteRegression}
import net.fnothaft.gnocchi.models._
import net.fnothaft.gnocchi.models.variant.linear.AdditiveLinearVariantModel
import net.fnothaft.gnocchi.models.variant.VariantModel
import net.fnothaft.gnocchi.models.variant.logistic.AdditiveLogisticVariantModel
import net.fnothaft.gnocchi.rdd.association.Association
import net.fnothaft.gnocchi.rdd.genotype.GenotypeState
import org.bdgenomics.formats.avro.Variant

trait BuildVariantModel {

  def apply(observations: Array[(Double, Array[Double])],
            variant: Variant,
            phenotype: String): VariantModel = {

    // call RegressPhenotypes on the data
    val assoc = compute(observations, variant, phenotype)

    // extract the model parameters (including p-value) for the variant and build VariantModel
    extractVariantModel(assoc)

  }

  def compute(observations: Array[(Double, Array[Double])],
              variant: Variant,
              phenotype: String): Association

  def extractVariantModel(assoc: Association): VariantModel

}

trait AdditiveVariant extends BuildVariantModel {
  def arrayClipOrKeepState(observations: Array[(Double, Array[Double])]): Array[(Double, Array[Double])] = {
    observations.map(obs => {
      (obs._1.toDouble, obs._2)
    })
  }

  protected def clipOrKeepState(gs: GenotypeState): Double = {
    gs.genotypeState.toDouble
  }
}

trait DominantVariant extends BuildVariantModel {
  def arrayClipOrKeepState(observations: Array[(Double, Array[Double])]): Array[(Double, Array[Double])] = {
    observations.map(obs => {
      if (obs._1 == 0) (0.0, obs._2) else (1.0, obs._2)
    })
  }

  protected def clipOrKeepState(gs: GenotypeState): Double = {
    if (gs.genotypeState == 0) 0.0 else 1.0
  }
}

object BuildAdditiveLinearVariantModel extends BuildVariantModel with LinearSiteRegression with AdditiveVariant {

  def compute(observations: Array[(Double, Array[Double])],
              variant: Variant,
              phenotype: String): Association = {

    val clippedObs = arrayClipOrKeepState(observations)
    val assoc = regressSite(clippedObs, variant, phenotype)
    assoc
  }

  def extractVariantModel(assoc: Association): AdditiveLinearVariantModel = {

    val linRegModel: AdditiveLinearVariantModel = new AdditiveLinearVariantModel(assoc.statistics("numSamples").asInstanceOf[Int],
      "assoc.variantID",
      assoc.statistics("weights").asInstanceOf[Array[Double]],
      assoc,
     "assoc.HaplotypeBlock") // assoc.numSamples
  }

  val regressionName = "Additive Linear Regression"
}

//object BuildDominantLinearVariantModel extends BuildVariantModel with LinearSiteRegression with DominantVariant {
//
//  def compute(observations: Array[(Double, Array[Double])],
//              locus: ReferenceRegion,
//              altAllele: String,
//              phenotype: String): Association = {
//
//    val clippedObs = arrayClipOrKeepState(observations)
//    regressSite(clippedObs, locus, altAllele, phenotype)
//  }
//
//  //  def extractVariantModel(assoc: Association): VariantModel = {
//  //
//  //    // code for extracting the VariantModel from the Association
//  //
//  //  }
//  val regressionName = "Dominant Linear Regression"
//}

object BuildAdditiveLogisticVariantModel extends BuildVariantModel with LogisticSiteRegression with AdditiveVariant {

  def compute(observations: Array[(Double, Array[Double])],
              variant: Variant,
              phenotype: String): Association = {

    val clippedObs = arrayClipOrKeepState(observations)
    val assoc = regressSite(clippedObs, variant, phenotype)
    assoc.statistics = assoc.statistics + ("numSamples" -> observations.length)
    assoc
  }

  def extractVariantModel(assoc: Association): AdditiveLogisticVariantModel = {

    val logRegModel = new AdditiveLogisticVariantModel
    logRegModel.setHaplotypeBlock("assoc.HaploTypeBlock")
      .setNumSamples(assoc.statistics("numSamples").asInstanceOf[Int]) // assoc.numSamples
      .setVariantId("assoc.variantID")
      .setWeights(assoc.statistics("weights").asInstanceOf[Array[Double]])
    logRegModel
  }

  val regressionName = "Additive Logistic Regression"
}

//object BuildDominantLogisticVariantModel extends BuildVariantModel with LogisticSiteRegression with DominantVariant {
//
//  def compute(observations: Array[(Double, Array[Double])],
//              locus: ReferenceRegion,
//              altAllele: String,
//              phenotype: String): Association = {
//
//    val clippedObs = arrayClipOrKeepState(observations)
//    regressSite(clippedObs, locus, altAllele, phenotype)
//  }
//
//  //  def extractVariantModel(assoc: Association): VariantModel = {
//  //
//  //    // code for extracting the VariantModel from the Association
//  //
//  //  }
//  val regressionName = "Dominant Logistic Regression"
//}

