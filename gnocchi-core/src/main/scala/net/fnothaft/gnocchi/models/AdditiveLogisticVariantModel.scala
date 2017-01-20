/**
 * Copyright 2016 Taner Dagdelen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import net.fnothaft.gnocchi.association.{ AdditiveLogisticAssociation }
import net.fnothaft.gnocchi.gnocchiModel.BuildAdditiveLogisticVariantModel
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.Variant

class AdditiveLogisticVariantModel extends LogisticVariantModel {

  var modelType = "Additive Logistic Variant Model"
  val regressionName = "Additive Logistic Regression"

  def update(observations: Array[(Double, Array[Double])],
             locus: ReferenceRegion,
             altAllele: String,
             phenotype: String): Unit = {

    val clippedObs = BuildAdditiveLogisticVariantModel.arrayClipOrKeepState(observations)
    val assoc = AdditiveLogisticAssociation.regressSite(clippedObs, variant, phenotype)
    if (assoc.statistics.nonEmpty) {
      association = assoc
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
