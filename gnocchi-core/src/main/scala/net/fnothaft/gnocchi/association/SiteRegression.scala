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
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{ Contig, Variant }

trait SiteRegression extends Serializable {

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
   * @return an rdd of [[net.fnothaft.gnocchi.models.Association]] objects
   */
  final def apply[T](genotypes: RDD[GenotypeState],
                     phenotypes: RDD[Phenotype[T]]): RDD[Association] = {
    val joinedGenoPheno = genotypes.keyBy(_.sampleId).join(phenotypes.keyBy(_.sampleId))

    /* Individuals with the same contigs (pairing of chromosome, end position, alt value) will be grouped together */
    val keyedGenoPheno = joinedGenoPheno.map(keyGenoPheno => {
      val (_, genoPheno) = keyGenoPheno
      val (gs, pheno) = genoPheno
      val variant = new Variant()
      val contig = new Contig()

      contig.setContigName(gs.contig)
      variant.setContig(contig)
      variant.setStart(gs.start)
      variant.setEnd(gs.end)
      variant.setAlternateAllele(gs.alt)
      ((variant, pheno.phenotype), genoPheno)
    })
      .groupByKey()

    keyedGenoPheno.map(site => {
      val ((variant, pheno), observations) = site
      val formattedObvs = observations.map(p => {
        val (genotypeState, phenotype) = p
        (clipOrKeepState(genotypeState), phenotype.toDouble)
      }).toArray
      regressSite(formattedObvs, variant, pheno)
    })
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
