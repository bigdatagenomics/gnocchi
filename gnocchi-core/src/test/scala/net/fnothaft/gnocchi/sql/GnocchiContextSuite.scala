///**
// * Licensed to Big Data Genomics (BDG) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The BDG licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
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
//package net.fnothaft.gnocchi.sql
//
//import net.fnothaft.gnocchi.GnocchiFunSuite
//
//class GnocchiContextSuite extends GnocchiFunSuite {
//
//  ignore("toGenotypeStateDataFrame should work for ploidy == 1")
//
//  ignore("toGenotypeStateDataFrame should work for ploidy ==2")
//
//  ignore("toGenotypeStateDataFrame should count number of NO_CALL's correctly")
//
//  ignore("filterSamples should not filter any samples if mind == 0")
//
//  ignore("filterSamples should filter on mind if mind is greater than 0 but less than 1")
//
//  ignore("filterSamples should filter out all samples if mind is greater or equal to 1")
//
//  ignore("filterVariants should not filter any varaints if geno and maf thresholds are both 0")
//
//  ignore("filterVariants should filter out variants with genotyping rate less than 0.1 when " +
//    "geno threshold set to 0.1 and maf threshold is greater than 0.1")
//
//  ignore("filterVariants should filter out variants with minor allele frequency less than 0.1 when" +
//    "maf threshold set to 0.1 and geno threshold is greater than 0.1")
//
//  ignore("Results from loadAndFilterGenotypes should match results of calls of filterSamples and then filterVariants")
//
//  ignore("loadFileAndCheckHeader should require that the input file is tab or space delimited")
//
//  ignore("loadFileAndCheckHeader should require that the input file has at least 2 columns")
//
//  ignore("loadFileAndCheckHeader should require that phenoName and each covariate match a" +
//    "column label in the header")
//
//  ignore("combineAndFilterPhenotypes should filter out samples that are missing a phenotype or a covariate")
//
//  ignore("loadPhenotypes should call loadFileAndCheckHeader and thus catch poorly formatted headers")
//
//  ignore("loadPhenotypes should call combineAndFilterPhenotypes and thus produce combined phenotype objects")
//
//  ignore("loadPhenotypes should require that phenoName cannot match a provided covar name")
//
//  ignore("isMissing should yield true iff the value provided is -9.0 or -9")
//
//  ignore("isMissing should throw a NumberFormatException the value provided cannot be converted to a double")
//
//}
