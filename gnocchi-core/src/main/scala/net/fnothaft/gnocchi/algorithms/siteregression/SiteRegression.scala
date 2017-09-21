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
package net.fnothaft.gnocchi.algorithms.siteregression

import org.apache.commons.math3.linear.SingularMatrixException
import net.fnothaft.gnocchi.models.variant.VariantModel
import net.fnothaft.gnocchi.primitives.association.Association
import net.fnothaft.gnocchi.primitives.phenotype.Phenotype
import net.fnothaft.gnocchi.primitives.genotype.GenotypeState
import net.fnothaft.gnocchi.primitives.variants.CalledVariant
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.bdgenomics.formats.avro.Variant
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
  def clipOrKeepState(gs: GenotypeState): Double
}

trait Additive {

  /**
   * Formats a GenotypeState object by converting the state to a double. Uses cumulative weighting of genotype
   * states which is typical of an Additive model.
   *
   * @param gs GenotypeState object to be clipped
   * @return Formatted GenotypeState object
   */
  def clipOrKeepState(gs: GenotypeState): Double = {
    gs.toDouble
  }
}

trait Dominant {

  /**
   * Formats a GenotypeState object by taking any non-zero as positive response, zero response otherwise.
   *
   * @param gs GenotypeState object to be clipped
   * @return Formatted GenotypeState object
   */
  def clipOrKeepState(gs: GenotypeState): Double = {
    if (gs.toDouble == 0) 0.0 else 1.0
  }
}
