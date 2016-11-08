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

import net.fnothaft.gnocchi.models.{ GeneralizedLinearSiteModel, GenotypeState, Phenotype }
import net.fnothaft.gnocchi.transformations.PnG2MatchedPairByVariant
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.GeneralizedLinearModel

trait SVMSiteModelGeneration extends Serializable {

  val regressionName: String

  protected def clipOrKeepState(gs: GenotypeState): Double

  /* 
  Takes in an RDD of GenotypeStates and an RDD of Phenotypes, groups them appropriately, and feeds it into
  buildOrUpdateSiteModel
  */
  final def apply[T](sc: SparkContext,
                     genotypes: RDD[GenotypeState],
                     phenotypes: RDD[Phenotype[Array[Double]]],
                     pathToModel: Option[String]): RDD[GeneralizedLinearSiteModel] = {
    // join together samples w/ both genotype and phenotype and organize into sites
    // match genotypes and phenotypes and organize by variantId
    val phenoGeno = PnG2MatchedPairByVariant(genotypes, phenotypes)
    // build or update model for each site
    phenoGeno.map(site => {
      val (((pos, allele), phenotype), observations) = site
      // build array to regress on, and then regress
      val obsRDD = sc.parallelize(observations.toList)
      buildOrUpdateSiteModel(sc, obsRDD, pathToModel)
    })
  }

  protected def buildOrUpdateSiteModel(sc: SparkContext,
                                       siteData: RDD[(GenotypeState, Phenotype[Array[Double]])],
                                       pathOption: Option[String]): GeneralizedLinearSiteModel

}

trait Additive {

  protected def clipOrKeepState(gs: GenotypeState): Double = {
    gs.genotypeState.toDouble
  }
}

trait Dominant {

  protected def clipOrKeepState(gs: GenotypeState): Double = {
    if (gs.genotypeState == 0) 0.0 else 1.0
  }
}
