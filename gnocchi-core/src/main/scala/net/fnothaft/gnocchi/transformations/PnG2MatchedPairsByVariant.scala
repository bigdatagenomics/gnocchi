///**
// * Copyright 2016 Taner Dagdelen
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package net.fnothaft.gnocchi.transformations
//
//import net.fnothaft.gnocchi.models.{ GeneralizedLinearSiteModel, GenotypeState, Phenotype }
//import org.apache.spark.rdd.RDD
//
//object PnG2MatchedPairByVariant {
//
//  def apply(genotypes: RDD[GenotypeState],
//            phenotypes: RDD[Phenotype[Array[Double]]]): RDD[(String, Array[(GenotypeState, Phenotype[Array[Double]])])] = {
//    // join together samples w/ both genotype and phenotype and organize into RDD[(variantId, Array[(GenotypeState, Phenotype)])]
//    genotypes.keyBy(_.sampleId)
//      // join together the samples with both genotype and phenotype entry (sampleId,(genoState, pheno))
//      .join(phenotypes.keyBy(_.sampleId))
//      // re-arrange rdd element to be (variantId, (GenotypeState,Phenotype))
//      .map(kvv => {
//        // unpack the entry of the joined rdd into id and actual info
//        val (_, p) = kvv
//        // unpack the information into genotype state and pheno
//        val (gs, pheno) = p
//        // extract referenceAllele and phenotype and pack up with p, then group by key
//        (gs.variantId, p: (GenotypeState, Phenotype[Array[Double]]))
//      })
//      // re-arrange rdd elements to be (variantId, Array[(GenotypeState,Phenotype)])
//      .groupByKey()
//  }
//}