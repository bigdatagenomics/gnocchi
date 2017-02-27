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

package net.fnothaft.gnocchi.transformations

import net.fnothaft.gnocchi.models.{ GenotypeState, Phenotype }
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.Variant

object PairSamplesWithPhenotypes {

  def apply(rdd: RDD[GenotypeState],
            phenotypes: RDD[Phenotype[Array[Double]]]): RDD[((Variant, String), Iterable[(String, (GenotypeState, Phenotype[Array[Double]]))])] = {
    rdd.keyBy(_.sampleId)
      // join together the samples with both genotype and phenotype entry
      .join(phenotypes.keyBy(_.sampleId))
      .map(kvv => {
        // unpack the entry of the joined rdd into id and actual info
        val (sampleid, gsPheno) = kvv
        // unpack the information into genotype state and pheno
        val (gs, pheno) = gsPheno

        // create contig and Variant objects and group by Variant
        // pack up the information into an Association object
        val variant = Gs2variant(gs)
        ((variant, pheno.phenotype), (sampleid, gsPheno))
      }).groupByKey
  }

}