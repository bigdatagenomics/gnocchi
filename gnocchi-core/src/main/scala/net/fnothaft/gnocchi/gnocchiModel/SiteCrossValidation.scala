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

import net.fnothaft.gnocchi.models.{ GeneralizedLinearSiteModel, GenotypeState, Phenotype, TestResult }
import net.fnothaft.gnocchi.transformations.PnG2MatchedPairByVariant
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait SiteCrossValidation extends Serializable with UpdateOrGenerateSiteSVMwithSGD with SVMSiteModelEvaluation {

  protected def clipOrKeepState(gs: GenotypeState): Double

  final def apply[T](sc: SparkContext,
                     numFolds: Double,
                     genotypes: RDD[GenotypeState],
                     phenotypes: RDD[Phenotype[Array[Double]]]): RDD[(String, Iterable[TestResult])] = {
    //sort the genotypes and phenotypes by sample. (sampleId,(genoState, pheno))
    val bySample = genotypes.keyBy(_.sampleId)
      .cogroup(phenotypes.keyBy(_.sampleId))
    val empty = TestResult("", 0.0)
    var finalResults = sc.parallelize(List(("", empty)))
    var i = 0
    while (i <= numFolds) {
      // randomly split into a folds
      val splitArray = bySample.randomSplit(Array(0.9, 0.1))
      val trainingIds = splitArray(0).map(sample => {
        val (sampleId, genoArray, phenoArray) = sample
        sampleId
      }).collect()
      val testingIds = splitArray(1).map(sample => {
        val (sampleId, genoArray, phenoArray) = sample
        sampleId
      }).collect()
      val trainingGenotypes = genotypes.filter(geno => trainingIds.contains(geno.sampleId))
      val trianingPhenotypes = phenotypes.filter(pheno => trainingIds.contains(pheno.sampleId))
      val testGenotypes = genotypes.filter(geno => !trainingIds.contains(geno.sampleId))
      val testPhenotypes = phenotypes.filter(pheno => !trainingIds.contains(pheno.sampleId))

      val trainingPhenoGeno = PnG2MatchedPairByVariant(genotypes, phenotypes)
      val pathToModel = None: Option[String]
      // build or update model for each site. Returns RDD[GeneralizedLinearSiteModel]
      val models: RDD[GeneralizedLinearSiteModel] = trainingPhenoGeno.map(site => {
        val (((pos, allele), phenotype), observations) = site
        // build array to regress on, and then regress
        val obsRDD = sc.parallelize(observations.toList)
        buildOrUpdateSiteModel(sc, obsRDD, pathToModel)
      })

      // match test genotypes and phenotypes and organize by variantId
      val testPhenoGeno = PnG2MatchedPairByVariant(genotypes, phenotypes)

      // create test results for each site RDD(TestResult)
      val results = testPhenoGeno.join(models.keyBy(_.variantId))
        // build or update model for each site
        .map(site => {
          // group by site the phenotype and genotypes. observations is an array of (GenotypeState, Phenotype) tuples.
          val (variantId, (obs, model)) = site
          // call predict of the model for each site on the data for each site.
          evaluateModelAtSite(sc, variantId, (obs, model))
        })
      if (i == 0) {
        var finalResults: RDD[(String, TestResult)] = results.keyBy(_.variantId)
      } else {
        var finalResults: RDD[(String, TestResult)] = finalResults ++ results.keyBy(_.variantId)
      }
      i += 1
    }
    finalResults.groupByKey
  }

  //  protected def crossValidateAtSite(): _
}