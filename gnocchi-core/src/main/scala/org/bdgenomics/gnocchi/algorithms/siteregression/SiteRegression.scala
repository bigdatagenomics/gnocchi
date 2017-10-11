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
package org.bdgenomics.gnocchi.algorithms.siteregression

import org.bdgenomics.gnocchi.models.variant.VariantModel
import org.bdgenomics.gnocchi.primitives.association.Association
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.utils.misc.Logging

import scala.collection.immutable.Map

trait SiteRegression[VM <: VariantModel[VM]] extends Serializable with Logging {

  val regressionName: String

  //  def apply(genotypes: Dataset[CalledVariant],
  //            phenotypes: Broadcast[Map[String, Phenotype]],
  //            validationStringency: String = "STRICT"): Dataset[VM]

  def applyToSite(phenotypes: Map[String, Phenotype],
                  genotypes: CalledVariant): Association

  /**
   * Known implementations: [[Additive]], [[Dominant]]
   *
   * @param gs GenotypeState object to be clipped
   * @return Formatted GenotypeState object
   */
  def clipOrKeepState(gs: Double): Double
}

trait Additive {

  /**
   * Formats a GenotypeState object by converting the state to a double. Uses cumulative weighting of genotype
   * states which is typical of an Additive model.
   *
   * @param gs GenotypeState object to be clipped
   * @return Formatted GenotypeState object
   */
  def clipOrKeepState(gs: Double): Double = {
    gs
  }
}

trait Dominant {

  /**
   * 1/0 or 0/1 or 1/1 ==> response
   *
   * @param gs GenotypeState object to be clipped
   * @return Formatted GenotypeState object
   */
  def clipOrKeepState(gs: Double): Double = {
    if (gs == 0.0) 0.0 else 1.0
  }
}

trait Recessive {

  /**
   * 1/1 ==> response
   *
   * @param gs GenotypeState object to be clipped
   * @return Formatted GenotypeState object
   */
  def clipOrKeepState(gs: Double): Double = {
    if (gs == 2.0) 1.0 else 0.0
  }
}