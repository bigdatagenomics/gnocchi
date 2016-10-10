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

import net.fnothaft.gnocchi.models.{GenotypeState, Phenotype, Prediction, SiteModel}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait SitePhenoPrediction extends Serializable {

  protected def clipOrKeepState(gs: GenotypeState): Double

  final def apply[T](sc: SparkContext,
                     genotypes: RDD[GenotypeState],
                     phenotypes: RDD[Phenotype[T]],
                     models: RDD[SiteModel]): RDD[Prediction] = {
      // join together samples w/ both genotype and phenotype and organize into sites
      genotypes.keyBy(_.sampleId)
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
        // build or update model for each site
        .map(site => {
        val (((pos, allele), phenotype), observations) = site
        // build array to regress on, and then regress
        val obsRDD = sc.parallelize(observations.toList)
        predictWithSite(obsRDD)
      })
    }

  }

  protected def predictWithSite(): _

}