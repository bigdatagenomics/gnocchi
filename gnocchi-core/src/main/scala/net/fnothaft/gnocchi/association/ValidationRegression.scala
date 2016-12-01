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

import net.fnothaft.gnocchi.models.{ Association, GenotypeState, Phenotype }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{ Contig, Variant }

trait ValidationRegression extends SiteRegression {

  /*
  Takes in an RDD of GenotypeStates, constructs the proper observations array for each site, and feeds it into
  regressSite
  */
  final def apply[T](rdd: RDD[GenotypeState],
                     phenotypes: RDD[Phenotype[T]],
                     scOption: Option[SparkContext] = None,
                     k: Double = 10): RDD[(Array[(String, (Double, Double))], Association)] = {
    val genoPhenoRdd = rdd.keyBy(_.sampleId).join(phenotypes.keyBy(_.sampleId))
    val Array(trainRdd, testRdd) = genoPhenoRdd.randomSplit(Array(1.0 - (1.0 / k), 1.0 / k))

    val modelRdd = trainRdd
      .map(kvv => {
        // unpack the entry of the joined rdd into id and actual info
        val (_, p) = kvv
        // unpack the information into genotype state and pheno
        val (gs, pheno) = p
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
      .map(site => {
        val ((variant, pheno), observations) = site

        // build array to regress on, and then regress
        val assoc = regressSite(observations.map(p => {
          // unpack p
          val (genotypeState, phenotype) = p
          // return genotype and phenotype in the correct form
          (clipOrKeepState(genotypeState), phenotype.toDouble)
        }).toArray, variant, pheno)
        ((variant, pheno), assoc)
      }).filter(varModel => {
        val ((variant, phenotype), assoc) = varModel
        assoc.statistics.nonEmpty
      })
    //    println("\n\n" + modelRdd.take(1).toList)

    val temp = testRdd
      .map(kvv => {
        // unpack the entry of the joined rdd into id and actual info
        val (sampleid, p) = kvv
        // unpack the information into genotype state and pheno
        val (gs, pheno) = p

        // create contig and Variant objects and group by Variant
        // pack up the information into an Association object
        val variant = new Variant()
        val contig = new Contig()
        contig.setContigName(gs.contig)
        variant.setContig(contig)
        variant.setStart(gs.start)
        variant.setEnd(gs.end)
        variant.setAlternateAllele(gs.alt)
        ((variant, pheno.phenotype), (sampleid, p))
      }).groupByKey()
    //    println("\n\n" + temp.take(1).toList)
    println("pre-join samples at a site: \n" + temp.take(5).toList)
    val temp2 = temp.join(modelRdd)
    println("Post-join samples and models at a site: \n" + temp2.take(0).toList)
    println(temp2.take(1).toList)
    temp2.map(site => {
      val (key, value) = site
      val (sampleObservations, association) = value
      val (variant, phenotype) = key

      (predictSite(sampleObservations.map(p => {
        // unpack p
        val (sampleid, (genotypeState, phenotype)) = p
        // return genotype and phenotype in the correct form
        (clipOrKeepState(genotypeState), phenotype.toDouble, sampleid)
      }).toArray, association), association)
    })

  }

  /**
   * Method to predict phenotype on a site.
   *
   * Predicts the phenotype given the genotype and covariates, after regression.
   * To be implemented by any class that implements this trait.
   */
  protected def predictSite(sampleObservations: Array[(Double, Array[Double], String)],
                            association: Association): Array[(String, (Double, Double))]
}

