/**
 * Copyright 2016 Taner Dagdelen
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
package net.fnothaft.gnocchi.gnocchiModel

trait GnocchiModel extends Serializable {

  val latestTR: TestResult
  val numSamples: Double
  val variables: Array[String]
  val description: String
  val parameters: Array[Double]

  // the methods that are applied for the whole model (like updateSites, batchUpdateSites, etc.) should be implemented
  // here, otherwise the methods that need to run on a per-site basis should be declared here but implemented in the
  // classes that extend the trait.

  // Gnocchi-model functions:
  final def getLatestModel(): Unit = {

  }

  final def updateSites()

  final def batchUpdateatSites(x: RDD[Genotype], y: RDD[Phenotype]): Unit = {
    applyToAllSites(batchUpdate)

  }

  final def recomputeatSites(x: RDD[Genotype], y: RDD[Phenotype]): Unit = {

  }

  final def predict1atSites(x: Genotype, y: Phenotype): RDD[(String, SitePrediction] = {

  }

  final def batchPredictatSites(x: RDD[Genotype], y: RDD[Phenotype]): RDD[Predictions] = {

  }

  final def evaluateAtSites(preds: RDD[Predictions], y: RDD[Phenotype]): TestResult = {

  }

  final def crossValidateAtSites(x: RDD[Genotype], y: RDD[Phenotype]): TestResult

//  final def save() ???

  final def applyToAllSites(mlStep () => T)
    // ***Need to make sure all the sites are there***



  val regressionName: String

  protected def clipOrKeepState(gs: GenotypeState): Double

  /* 
  Takes in an RDD of GenotypeStates, constructs the proper observations array for each site, and feeds it into 
  regressSite
  */
  final def apply[T](rdd: RDD[GenotypeState],
                     phenotypes: RDD[Phenotype[T]]): RDD[Association] = {
    rdd.keyBy(_.sampleId)
      // join together the samples with both genotype and phenotype entry
      .join(phenotypes.keyBy(_.sampleId))
      .map(kvv => {
        // unpack the entry of the joined rdd into id and actual info
        val (_, p) = kvv
        // unpack the information into genotype state and pheno
        val (gs, pheno) = p
        // extract referenceAllele and phenotype and pack up with p, then group by key
        ((gs.referenceAllele, pheno.phenotype), p)
      }).groupByKey()
      .map(site => {
        val (((pos, allele), phenotype), observations) = site
        // build array to regress on, and then regress
        regressSite(observations.map(p => {
          // unpack p
          val (genotypeState, phenotype) = p
          // return genotype and phenotype in the correct form
          (clipOrKeepState(genotypeState), phenotype.toDouble)
        }).toArray, pos, allele, phenotype)
      })
  }

  /**
   * Method to run regression on a site.
   *
   * Computes the association score of a genotype against a phenotype and
   * covariates. To be implemented by any class that implements this trait.
   */
  protected def regressSite(observations: Array[(Double, Array[Double])],
                            locus: ReferenceRegion,
                            altAllele: String,
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
