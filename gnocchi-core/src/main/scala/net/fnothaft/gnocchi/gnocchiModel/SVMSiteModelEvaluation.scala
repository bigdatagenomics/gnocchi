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

import net.fnothaft.gnocchi.models.{ GeneralizedLinearSiteModel, GenotypeState, Phenotype, TestResult }
import net.fnothaft.gnocchi.transformations.GPM2TestResult
import org.apache.spark.SparkContext

trait SVMSiteModelEvaluation extends SiteModelEvaluation {

  def evaluateModelAtSite(sc: SparkContext,
                          variantId: String,
                          dataAndModel: (Array[(GenotypeState, Phenotype[Array[Double]])], GeneralizedLinearSiteModel)): TestResult = {
    // evaluate the model at the site and produce TestResult
    GPM2TestResult(sc, variantId, dataAndModel, clipOrKeepState)
  }
}

object AdditiveSVMSiteModelEvaluation extends SVMSiteModelEvaluation with Additive {
  val evaluationName = "additiveSVMEvaluation"
}

object DominantSVMSiteModelEvaluation extends SVMSiteModelEvaluation with Dominant {
  val evaluationName = "dominantSVMEvaluation"
}