/**
 * Copyright 2016 Frank Austin Nothaft, Taner Dagdelen
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
package net.fnothaft.gnocchi.association

import net.fnothaft.gnocchi.models.Association
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.bdgenomics.adam.models.ReferenceRegion

trait LinearSiteRegression extends SiteRegression {

  protected def regressSite(observations: Array[(Int, Array[Double])],
                            locus: ReferenceRegion,
                            altAllele: String,
                            phenotype: String): Association = {
    // taner: fill this in!
    // prolly use this class for linreg: org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
    // see http://commons.apache.org/proper/commons-math/javadocs/api-3.6.1/org/apache/commons/math3/stat/regression/OLSMultipleLinearRegression.html
    ???
  }
}

object AdditiveLinearAssociation extends LinearSiteRegression with Additive {
  val regressionName = "additiveLinearRegression"
}

object DominantLinearAssociation extends LinearSiteRegression with Dominant {
  val regressionName = "dominantLinearRegression"
}
