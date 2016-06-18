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

import net.fnothaft.gnocchi.models.{
  Association,
  GenotypeState,
  Phenotype
}
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion

trait SiteRegression extends Serializable {

  val regressionName: String

  protected def clipOrKeepState(gs: GenotypeState): Int

  final def apply[T](rdd: RDD[GenotypeState],
                     phenotypes: RDD[Phenotype[T]]): RDD[Association] = {
    rdd.keyBy(_.sampleId)
      .join(phenotypes.keyBy(_.sampleId))
      .map(kvv => {
        val (_, p) = kvv
        val (gs, pheno) = p
        ((gs.referenceAllele, pheno.phenotype), p)
      }).groupByKey()
      .map(site => {
        val (((pos, allele), phenotype), observations) = site

        // build array to regress on, and then regress
        regressSite(observations.map(p => {
          val (genotypeState, phenotype) = p

          (clipOrKeepState(genotypeState), Array(phenotype.toDouble))
        }).toArray, pos, allele, phenotype)
      })
  }

  /**
   * Method to run regression on a site.
   *
   * Computes the association score of a genotype against a phenotype and
   * covariates. To be implemented by any class that implements this trait.
   */
  protected def regressSite(observations: Array[(Int, Array[Double])],
                            locus: ReferenceRegion,
                            altAllele: String,
                            phenotype: String): Association
}

trait Additive extends SiteRegression {

  protected def clipOrKeepState(gs: GenotypeState): Int = {
    gs.genotypeState
  }
}

trait Dominant extends SiteRegression {

  protected def clipOrKeepState(gs: GenotypeState): Int = {
    if (gs.genotypeState == 0) 0 else 1
  }
}
