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
import net.fnothaft.gnocchi.primitives.genotype.Genotype
import net.fnothaft.gnocchi.primitives.phenotype.Phenotype
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.Variant
import org.bdgenomics.utils.misc.Logging

trait SiteApplication[VM <: VariantModel[VM], A <: Association[VM]] extends Serializable with Logging {

  val regressionName: String

  /**
   * Known implementations: [[Additive]], [[Dominant]]
   *
   * @param gs GenotypeState object to be clipped
   * @return Formatted GenotypeState object
   */
  def clipOrKeepState(gs: Genotype): Double

  /**
   * Apply method for SiteRegression. Takes in an RDD of Genotype and Phenotype observations and returns an RDD of
   * Association objects containing the statistics for each site.
   *
   * @param validationStringency the validation level by which to throw exceptions
   * @return an rdd of [[net.fnothaft.gnocchi.primitives.association.Association]] objects
   */
  final def apply(observations: RDD[((Variant, String, Int), Array[(Double, Array[Double])])],
                  validationStringency: String = "STRICT"): RDD[Association[VM]] = {
    //    val joinedGenoPheno = genotypes.keyBy(_.sampleId).join(phenotypes.keyBy(_.sampleId))
    //
    //    val keyedGenoPheno = joinedGenoPheno.map(keyGenoPheno => {
    //      val (_, genoPheno) = keyGenoPheno
    //      val (gs, pheno) = genoPheno
    //      val variant = new Variant()
    //      variant.setContigName(gs.contigName)
    //      variant.setStart(gs.start)
    //      variant.setEnd(gs.end)
    //      variant.setAlternateAllele(gs.alt)
    //      //      variant.setNames(List(""))
    //      //      variant.setFiltersFailed(List(""))
    //      ((variant, pheno.phenotype, gs.phaseSetId), genoPheno)
    //    })
    //      .groupByKey()
    //
    //    keyedGenoPheno.map(site => {
    //      val ((variant, pheno, phaseSetId), observations) = site
    //      val formattedObs = observations.map(p => {
    //        val (genotypeState, phenotype) = p
    //        (clipOrKeepState(genotypeState), phenotype.toDouble)
    //      }).toArray
    observations
      .map(site => {
        val ((variant, pheno, phaseSetId), formattedObs) = site
        try {
          applyToSite(formattedObs, variant, pheno, phaseSetId)
        } catch {
          case error: SingularMatrixException => {
            validationStringency match {
              case "STRICT" => throw new SingularMatrixException()
              case "LENIENT" => {
                val variantNames = variant.getNames
                logError(s"Singular Matrix found in SiteRegression for variant represented by $variantNames")
                constructAssociation(variant.getContigName, 1, "", new Array[Double](formattedObs.head._2.length + 1), 0.0, variant, "", 0.0, 0.0, 0, Map(("", "")))
              }
              case "SILENT" => {
                constructAssociation(variant.getContigName, 1, "", new Array[Double](formattedObs.head._2.length + 1), 0.0, variant, "", 0.0, 0.0, 0, Map(("", "")))
              }
            }
          }
        }
      }).asInstanceOf[RDD[Association[VM]]]
      //TODO: What is this doing here? Take this out.
      .filter(assoc => assoc.logPValue != 0.0)
  }

  /**
   * Performs regression on a single site. A site in this context is the unique pairing of a
   * [[org.bdgenomics.formats.avro.Variant]] object and a [[net.fnothaft.gnocchi.primitives.phenotype.Phenotype]] name.
   *
   * @param observations Array of tuples. The first element is a coded genotype taken from
   *                     [[net.fnothaft.gnocchi.primitives.genotype.Genotype]]. The second is an array of observed phenotypes
   *                     taken from [[net.fnothaft.gnocchi.primitives.phenotype.Phenotype]] objects.
   * @param variant      [[org.bdgenomics.formats.avro.Variant]] to be regressed on
   * @param phenotype    Phenotype value, stored as a String
   * @return [[net.fnothaft.gnocchi.primitives.association.Association]] containing statistic for particular site
   */
  protected def applyToSite(observations: Array[(Double, Array[Double])],
                            variant: Variant,
                            phenotype: String,
                            phaseSetId: Int): Association[VM]

  def constructAssociation(variantId: String,
                           numSamples: Int,
                           modelType: String,
                           weights: Array[Double],
                           geneticParameterStandardError: Double,
                           variant: Variant,
                           phenotype: String,
                           logPValue: Double,
                           pValue: Double,
                           phaseSetId: Int,
                           statistics: Map[String, Any]): Association[VM]
}

trait Additive {

  /**
   * Formats a GenotypeState object by converting the state to a double. Uses cumulative weighting of genotype
   * states which is typical of an Additive model.
   *
   * @param gs GenotypeState object to be clipped
   * @return Formatted GenotypeState object
   */
  def clipOrKeepState(gs: Genotype): Double = {
    gs.genotypeState.toDouble
  }
}

trait Dominant {

  /**
   * Formats a GenotypeState object by taking any non-zero as positive response, zero response otherwise.
   *
   * @param gs GenotypeState object to be clipped
   * @return Formatted GenotypeState object
   */
  def clipOrKeepState(gs: Genotype): Double = {
    if (gs.genotypeState == 0) 0.0 else 1.0
  }
}
