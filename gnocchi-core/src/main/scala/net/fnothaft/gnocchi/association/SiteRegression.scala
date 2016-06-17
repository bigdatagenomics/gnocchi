/**
 * Copyright 2016 Frank Austin Nothaft
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

import net.fnothaft.gnocchi.{ Association, Phenotype }
import org.apache.spark.rdd.RDD

trait SiteRegression extends Serializable {

  

  /**
   * Method to run regression on a site.
   *
   * Computes the association score of a genotype against a phenotype and
   * covariates. To be implemented by any class that implements this trait.
   */
  protected def regressSite(observations: Array[(Int, Array[Double])],
                            site: Variant,
                            phenotype: String): Association
}
