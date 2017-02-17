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

import net.fnothaft.gnocchi.models.{ Association, GenotypeState, MultipleRegressionDoublePhenotype, Phenotype }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{ Contig, Variant }

trait SiteRegression extends Serializable {

  val regressionName: String

  protected def clipOrKeepState(gs: GenotypeState): Double

  /* 
  Takes in an RDD of GenotypeStates, constructs the proper observations array for each site, and feeds it into 
  regressSite
  */
  final def apply[T](rdd: RDD[GenotypeState],
                     phenotypes: RDD[Phenotype[T]]): RDD[Association] = {
    val temp = rdd.keyBy(_.sampleId)
      // join together the samples with both genotype and phenotype entry
      .join(phenotypes.keyBy(_.sampleId))
      .map(kvv => {
        // unpack the entry of the joined rdd into id and actual info
        val (_, p) = kvv
        // unpack the information into genotype state and pheno
        val (gs, pheno) = p
        // extract referenceAllele and phenotype and pack up with p, then group by key

        // create contig and Variant objects and group by Variant
        // pack up the information into an Association object
        val variant = new Variant()
        val contig = new Contig()
        contig.setContigName(gs.contig)
        variant.setContig(contig)
        variant.setStart(gs.start)
        variant.setEnd(gs.end)
        variant.setAlternateAllele(gs.alt)
        ((variant, pheno.phenotype), p)
      }).groupByKey()
    val temp2 = temp
      .map(site => {
        val ((variant, pheno), observations) = site
        // build array to regress on, and then regress
        ((variant, pheno), observations.map(p => {
          // unpack p
          val (genotypeState, phenotype) = p
          // return genotype and phenotype in the correct form
          (clipOrKeepState(genotypeState), phenotype.toDouble.toList)
        }).toList)
      })
    println(temp2.take(10).toList mkString "\n")
    temp
      .map(site => {
        val ((variant, pheno), observations) = site
        // build array to regress on, and then regress
        regressSite(observations.map(p => {
          // unpack p
          val (genotypeState, phenotype) = p
          // return genotype and phenotype in the correct form
          (clipOrKeepState(genotypeState), phenotype.toDouble)
        }).toArray, variant, pheno)
      })

  }

  /**
   * Method to run regression on a site.
   *
   * Computes the association score of a genotype against a phenotype and
   * covariates. To be implemented by any class that implements this trait.
   */
  protected def regressSite(observations: Array[(Double, Array[Double])],
                            variant: Variant,
                            phenotype: String): Association
}

trait Additive extends SiteRegression {

  protected def clipOrKeepState(gs: GenotypeState): Double = {
    gs.genotypeState.toDouble
  }
}

trait Dominant extends SiteRegression {

  protected def clipOrKeepState(gs: GenotypeState): Double = {
    if (gs.genotypeState == 0) 0.0 else 1.0
  }
}
