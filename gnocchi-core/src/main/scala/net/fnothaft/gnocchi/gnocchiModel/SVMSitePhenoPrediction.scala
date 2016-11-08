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

import net.fnothaft.gnocchi.models._
import net.fnothaft.gnocchi.transformations.GPM2Predictions
import org.apache.spark.SparkContext

trait SVMSitePhenoPrediction extends SitePhenoPrediction {

  def predictWithSite(sc: SparkContext,
                      variantId: String,
                      dataAndModel: (Array[(GenotypeState, Phenotype[Array[Double]])], GeneralizedLinearSiteModel)): (String, Array[Prediction]) = {
    GPM2Predictions(sc, variantId, dataAndModel, clipOrKeepState)
  }
}

object AdditiveSVMSitePhenoPrediction extends SVMSitePhenoPrediction with Additive {
  val evaluationName = "additiveSVMPhenoPrediction"
}

object DominantSVMSitePhenoPrediciton extends SVMSitePhenoPrediction with Dominant {
  val evaluationName = "dominantSVMPhenoPrediction"
}