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
package net.fnothaft.gnocchi.association

import net.fnothaft.gnocchi.models.{ Association, GenotypeState, MultipleRegressionDoublePhenotype, Phenotype }
import org.apache.commons.math3.linear.SingularMatrixException
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{ Contig, Variant }
import org.bdgenomics.utils.misc.Logging
import scala.collection.JavaConversions._

trait SiteRegression extends Serializable with Logging {

  val regressionName: String

  /**
   * Known implementations: [[Additive]], [[Dominant]]
   *
   * @param gs GenotypeState object to be clipped
   * @return Formatted GenotypeState object
   */
  protected def clipOrKeepState(gs: GenotypeState): Double

  /**
   * Apply method for SiteRegression. Takes in an RDD of Genotypes and Phenotypes and returns an RDD of
   * Association objects containing the statistics for each site.
   *
   * @param genotypes an rdd of [[net.fnothaft.gnocchi.models.GenotypeState]] objects to be regressed upon
   * @param phenotypes an rdd of [[net.fnothaft.gnocchi.models.Phenotype]] objects used as observations
   * @param validationStringency the validation level by which to throw exceptions
   *
   * @return an rdd of [[net.fnothaft.gnocchi.models.Association]] objects
   */
  final def apply[T](genotypes: RDD[GenotypeState],
                     phenotypes: RDD[Phenotype[T]],
                     validationStringency: String = "STRICT"): RDD[Association] = {
    val joinedGenoPheno = genotypes.keyBy(_.sampleId).join(phenotypes.keyBy(_.sampleId))

    /* Individuals with the same contigs (pairing of chromosome, end position, alt value) will be grouped together */
    val keyedGenoPheno = joinedGenoPheno.map(keyGenoPheno => {
      val (_, genoPheno) = keyGenoPheno
      val (gs, pheno) = genoPheno
      val variant = new Variant()
      variant.setContigName(gs.contigName)
      variant.setStart(gs.start)
      variant.setEnd(gs.end)
      variant.setAlternateAllele(gs.alt)
      variant.setNames(Seq())
      variant.setFiltersFailed(Seq())
      ((variant, pheno.phenotype), genoPheno)
    })
      .groupByKey()

    keyedGenoPheno.map(site => {
      val ((variant, pheno), observations) = site
      val formattedObvs = observations.map(p => {
        val (genotypeState, phenotype) = p
        (clipOrKeepState(genotypeState), phenotype.toDouble)
      }).toArray
      try { regressSite(formattedObvs, variant, pheno) }
      catch {
        case error: SingularMatrixException => {
          validationStringency match {
            case "STRICT" => throw new SingularMatrixException()
            case "LENIENT" => {
              logError("Singular Matrix found in SiteRegression")
              Association(variant, pheno, 0.0, null)
            }
            case "SILENT" => Association(variant, pheno, 0.0, null)
          }
        }
      }
    })
      .filter(assoc => assoc.logPValue != 0.0)
  }

  /**
   * Performs regression on a single site. A site in this context is the unique pairing of a
   * [[org.bdgenomics.formats.avro.Variant]] object and a [[net.fnothaft.gnocchi.models.Phenotype]] name.
   *
   * @param observations Array of tuples. The first element is a coded genotype taken from
   *                     [[net.fnothaft.gnocchi.models.GenotypeState]]. The second is an array of observed phenotypes
   *                     taken from [[net.fnothaft.gnocchi.models.Phenotype]] objects.
   * @param variant [[org.bdgenomics.formats.avro.Variant]] to be regressed on
   * @param phenotype Phenotype value, stored as a String
   * @return [[net.fnothaft.gnocchi.models.Association]] containing statistic for particular site
   */
  protected def regressSite(observations: Array[(Double, Array[Double])],
                            variant: Variant,
                            phenotype: String): Association
}

trait Additive extends SiteRegression {

  /**
   * Formats a GenotypeState object by converting the state to a double. Uses cumulative weighting of genotype
   * states which is typical of an Additive model.
   *
   * @param gs GenotypeState object to be clipped
   * @return Formatted GenotypeState object
   */
  protected def clipOrKeepState(gs: GenotypeState): Double = {
    gs.genotypeState.toDouble
  }
}

trait Dominant extends SiteRegression {

  /**
   * Formats a GenotypeState object by taking any non-zero as positive response, zero response otherwise.
   *
   * @param gs GenotypeState object to be clipped
   * @return Formatted GenotypeState object
   */
  protected def clipOrKeepState(gs: GenotypeState): Double = {
    if (gs.genotypeState == 0) 0.0 else 1.0
  }
}
