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

package net.fnothaft.gnocchi.gnocchiModel

trait BuildDominantLogistic extends BuildVariantModel with LogisticSiteRegression {
  def compute(observations: Array[(Double, Array[Double])],
              locus: ReferenceRegion,
              altAllele: String,
              phenotype: String): Association = {

    val clippedObs = cliipOrKeepState(observations)
    regressSite(observations, locus, altPallele, phenotype)
  }

  def extractVariantModel(assoc: Association): VariantModel[T] = {

    // code for extracting the VariantModel from the Association

  }
}

object BuildAdditiveLinearVariantModel extends BuildAdditiveLinear with AdditiveVariant {
  val regressionType = "Additive Linear Regression"
}

object BuildDominantLinearVariantModel extends BuildDominantLinear with DominantVariant {
  val regressionType = "Dominant Linear Regression"
}
