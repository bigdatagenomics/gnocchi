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

trait ValidationRegression extends SiteRegression {

  /*
  Takes in an RDD of GenotypeStates, constructs the proper observations array for each site, and feeds it into
  regressSite
  */
  final def apply[T](rdd: RDD[GenotypeState],
                     phenotypes: RDD[Phenotype[T]],
                     scOption: Option[SparkContext] = None): RDD[(Array[(String, (Double, Double))], Association)] = {
    val genoPhenoRdd = rdd.keyBy(_.sampleId).join(phenotypes.keyBy(_.sampleId))
    val Array(trainRdd, testRdd) = genoPhenoRdd.randomSplit(Array(0.9, 0.1))

    val modelRdd = trainRdd
      .map(kvv => {
        // unpack the entry of the joined rdd into id and actual info
        val (_, p) = kvv
        // unpack the information into genotype state and pheno
        val (gs, pheno) = p
        // extract referenceAllele and phenotype and pack up with p, then group by key
        ((gs.referenceAllele, pheno.phenotype), p)
      }).groupByKey()
      .map(site => {
        val (key, observations) = site
        val ((pos, allele), phenotype) = key
        // build array to regress on, and then regress
        (key, regressSite(observations.map(p => {
          // unpack p
          val (genotypeState, phenotype) = p
          // return genotype and phenotype in the correct form
          (clipOrKeepState(genotypeState), phenotype.toDouble)
        }).toArray, pos, allele, phenotype))
      })

    testRdd
      .map(kvv => {
        // unpack the entry of the joined rdd into id and actual info
        val (sampleid, p) = kvv
        // unpack the information into genotype state and pheno
        val (gs, pheno) = p
        // extract referenceAllele and phenotype and pack up with p, then group by key
        ((gs.referenceAllele, pheno.phenotype), (sampleid, p))
      }).groupByKey()
      .join(modelRdd)
      .map(site => {
        val (key, value) = site
        val (sampleObservations, association) = value
        val ((pos, allele), phenotype) = key

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

