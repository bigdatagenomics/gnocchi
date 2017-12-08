/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.gnocchi.algorithms.siteregression

import breeze.linalg.MatrixSingularException
import org.bdgenomics.gnocchi.GnocchiFunSuite
import org.bdgenomics.gnocchi.primitives.genotype.GenotypeState
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.scalactic.Tolerance._

class LinearSiteRegressionSuite extends GnocchiFunSuite {
  // LinearSiteRegression.applyToSite correctness tests
  /* generate Anscombe's quartet for linear regression
    the quartet data is as follows:
        Anscombe's quartet
       I           II          III          IV
    x      y     x     y     x     y     x     y
    10.0  8.04  10.0  9.14  10.0  7.46  8.0   6.58
    8.0   6.95  8.0   8.14  8.0   6.77  8.0   5.76
    13.0  7.58  13.0  8.74  13.0  12.74 8.0   7.71
    9.0   8.81  9.0   8.77  9.0   7.11  8.0   8.84
    11.0  8.33  11.0  9.26  11.0  7.81  8.0   8.47
    14.0  9.96  14.0  8.10  14.0  8.84  8.0   7.04
    6.0   7.24  6.0   6.13  6.0   6.08  8.0   5.25
    4.0   4.26  4.0   3.10  4.0   5.39  19.0  12.50
    12.0  10.84 12.0  9.13  12.0  8.15  8.0   5.56
    7.0   4.82  7.0   7.26  7.0   6.42  8.0   7.91
    5.0   5.68  5.0   4.74  5.0   5.73  8.0   6.89

    The full description of Anscombe's Quartet can be found here: https://en.wikipedia.org/wiki/Anscombe%27s_quartet
    Each of the members of the quartet should have roughly the same R^2 value, although the shapes of the graph are different shapes.
    Target R^2 values verified values produced here https://rstudio-pubs-static.s3.amazonaws.com/52381_36ec82827e4b476fb968d9143aec7c4f.html.
  */
  ignore("LinearSiteRegression.applyToSite should calculate rsquared and p-value correctly for Anscombe I.") {
    // load AnscombeI into an observations variable

    val observations = new Array[(Double, Double)](11)
    observations(0) = (10.0, 8.04)
    observations(1) = (8.0, 6.95)
    observations(2) = (13.0, 7.58)
    observations(3) = (9.0, 8.81)
    observations(4) = (11.0, 8.33)
    observations(5) = (14.0, 9.96)
    observations(6) = (6.0, 7.24)
    observations(7) = (4.0, 4.26)
    observations(8) = (12.0, 10.84)
    observations(9) = (7.0, 4.82)
    observations(10) = (5.0, 5.68)

    val (genotypes, phenotypes) = observations.unzip
    val genotypeStates = genotypes.toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cv = CalledVariant(1, 1, "rs123456", "A", "C", genotypeStates)

    val phenoMap = phenotypes
      .toList
      .zipWithIndex
      .map(item => (item._2.toString, Phenotype(item._2.toString, "pheno1", item._1)))
      .toMap

    // use additiveLinearRegression to regress on AscombeI
    val regressionResult = LinearSiteRegression.applyToSite(phenoMap, cv, "ADDITIVE")

    // Assert that the rsquared is in the right threshold.
    // R^2 = 1 - (SS_res / SS_tot)
    val rSquared = 1 - regressionResult.ssResiduals / regressionResult.ssDeviations
    assert(rSquared === 0.6665 +- 0.005)

    // Assert that the p-value for independent variable is correct (expectedPVal ~= 0.002169629)
    assert(regressionResult.pValue === 0.002169629 +- 0.00005)
  }

  ignore("LinearSiteRegression.applyToSite should calculate rsquared and p-value correctly for Anscombe II.") {
    // load AnscombeII into an observations variable
    val observations = new Array[(Double, Double)](11)
    observations(0) = (10.0, 9.14)
    observations(1) = (8.0, 8.14)
    observations(2) = (13.0, 8.74)
    observations(3) = (9.0, 8.77)
    observations(4) = (11.0, 9.26)
    observations(5) = (14.0, 8.10)
    observations(6) = (6.0, 6.13)
    observations(7) = (4.0, 3.10)
    observations(8) = (12.0, 9.13)
    observations(9) = (7.0, 7.26)
    observations(10) = (5.0, 4.74)

    val (genotypes, phenotypes) = observations.unzip
    val genotypeStates = genotypes.toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cv = CalledVariant(1, 1, "rs123456", "A", "C", genotypeStates)

    val phenoMap = phenotypes
      .toList
      .zipWithIndex
      .map(item => (item._2.toString, Phenotype(item._2.toString, "pheno1", item._1)))
      .toMap

    // use additiveLinearRegression to regress on AscombeII
    val regressionResult = LinearSiteRegression.applyToSite(phenoMap, cv, "ADDITIVE")

    // Assert that the rsquared is in the right threshold.
    // R^2 = 1 - (SS_res / SS_tot)
    val rSquared = 1 - regressionResult.ssResiduals / regressionResult.ssDeviations
    assert(rSquared === 0.6665 +- 0.005)

    // Assert that the p-value for independent variable is correct (expectedPVal ~= 0.002178816)
    assert(regressionResult.pValue === 0.002178816 +- 0.00005)
  }

  ignore("LinearSiteRegression.applyToSite should calculate rsquared and p-value correctly for Anscombe III.") {
    // load AnscombeIII into an observations variable
    val observations = new Array[(Double, Double)](11)
    observations(0) = (10.0, 7.46)
    observations(1) = (8.0, 6.77)
    observations(2) = (13.0, 12.74)
    observations(3) = (9.0, 7.11)
    observations(4) = (11.0, 7.81)
    observations(5) = (14.0, 8.84)
    observations(6) = (6.0, 6.08)
    observations(7) = (4.0, 5.39)
    observations(8) = (12.0, 8.15)
    observations(9) = (7.0, 6.42)
    observations(10) = (5.0, 5.73)

    val (genotypes, phenotypes) = observations.unzip
    val genotypeStates = genotypes.toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cv = CalledVariant(1, 1, "rs123456", "A", "C", genotypeStates)

    val phenoMap = phenotypes
      .toList
      .zipWithIndex
      .map(item => (item._2.toString, Phenotype(item._2.toString, "pheno1", item._1)))
      .toMap

    // use additiveLinearRegression to regress on AscombeIII
    val regressionResult = LinearSiteRegression.applyToSite(phenoMap, cv, "ADDITIVE")

    // Assert that the rsquared is in the right threshold.
    // R^2 = 1 - (SS_res / SS_tot)
    val rSquared = 1 - regressionResult.ssResiduals / regressionResult.ssDeviations
    assert(rSquared === 0.6665 +- 0.005)

    // Assert that the p-value for independent variable is correct (expectedPVal ~= 0.002176305)
    assert(regressionResult.pValue === 0.002176305 +- 0.00005)
  }

  ignore("LinearSiteRegression.applyToSite should calculate rsquared and p-value correctly for Anscombe IV.") {
    //load AnscombeIV into an observations variable
    val observations = new Array[(Double, Double)](11)
    observations(0) = (8.0, 6.58)
    observations(1) = (8.0, 5.76)
    observations(2) = (8.0, 7.71)
    observations(3) = (8.0, 8.84)
    observations(4) = (8.0, 8.47)
    observations(5) = (8.0, 7.04)
    observations(6) = (8.0, 5.25)
    observations(7) = (19.0, 12.50)
    observations(8) = (8.0, 5.56)
    observations(9) = (8.0, 7.91)
    observations(10) = (8.0, 6.89)

    val (genotypes, phenotypes) = observations.unzip
    val genotypeStates = genotypes.toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cv = CalledVariant(1, 1, "rs123456", "A", "C", genotypeStates)

    val phenoMap = phenotypes
      .toList
      .zipWithIndex
      .map(item => (item._2.toString, Phenotype(item._2.toString, "pheno1", item._1)))
      .toMap

    //use additiveLinearRegression to regress on AscombeIV
    val regressionResult = LinearSiteRegression.applyToSite(phenoMap, cv, "ADDITIVE")

    // Assert that the rsquared is in the right threshold.
    // R^2 = 1 - (SS_res / SS_tot)
    val rSquared = 1 - regressionResult.ssResiduals / regressionResult.ssDeviations
    assert(rSquared === 0.6665 +- 0.005)

    // Assert that the p-value for independent variable is correct (expectedPVal ~= 0.002164602)
    assert(regressionResult.pValue === 0.002164602 +- 0.00005)
  }

  ignore("LinearSiteRegression.applyToSite should work correctly for PIQ data.") {
    /* Tests for multiple regression and covariate correction:
    Data comes from here: https://onlinecourses.science.psu.edu/stat501/node/284
    PIQ is treated as the phenotype of interest and Brain is treated as genotype. Height and Weight are treated as covariates.
      Raw Data:
              PIQ Brain Height  Weight
              124 81.69 64.5  118
              150 103.84  73.3  143
              128 96.54 68.8  172
              134 95.15 65.0  147
              110 92.88 69.0  146
              131 99.13 64.5  138
              98  85.43 66.0  175
              84  90.49 66.3  134
              147 95.55 68.8  172
              124 83.39 64.5  118
              128 107.95  70.0  151
              124 92.41 69.0  155
              147 85.65 70.5  155
              90  87.89 66.0  146
              96  86.54 68.0  135
              120 85.22 68.5  127
              102 94.51 73.5  178
              84  80.80 66.3  136
              86  88.91 70.0  180
              84  90.59 76.5  186
              134 79.06 62.0  122
              128 95.50 68.0  132
              102 83.18 63.0  114
              131 93.55 72.0  171
              84  79.86 68.0  140
              110 106.25  77.0  187
              72  79.35 63.0  106
              124 86.67 66.5  159
              132 85.78 62.5  127
              137 94.96 67.0  191
              110 99.79 75.5  192
              86  88.00 69.0  181
              81  83.43 66.5  143
              128 94.81 66.5  153
              124 94.94 70.5  144
              94  89.40 64.5  139
              74  93.00 74.0  148
              89  93.59 75.5  179
  */
    //load data into an observations variable
    val observations = new Array[(Double, Array[Double])](38)
    observations(0) = (81.69, Array[Double](124, 64.5, 118))
    observations(1) = (103.84, Array[Double](150, 73.3, 143))
    observations(2) = (96.54, Array[Double](128, 68.8, 172))
    observations(3) = (95.15, Array[Double](134, 65.0, 147))
    observations(4) = (92.88, Array[Double](110, 69.0, 146))
    observations(5) = (99.13, Array[Double](131, 64.5, 138))
    observations(6) = (85.43, Array[Double](98, 66.0, 175))
    observations(7) = (90.49, Array[Double](84, 66.3, 134))
    observations(8) = (95.55, Array[Double](147, 68.8, 172))
    observations(9) = (83.39, Array[Double](124, 64.5, 118))
    observations(10) = (107.95, Array[Double](128, 70.0, 151))
    observations(11) = (92.41, Array[Double](124, 69.0, 155))
    observations(12) = (85.65, Array[Double](147, 70.5, 155))
    observations(13) = (87.89, Array[Double](90, 66.0, 146))
    observations(14) = (86.54, Array[Double](96, 68.0, 135))
    observations(15) = (85.22, Array[Double](120, 68.5, 127))
    observations(16) = (94.51, Array[Double](102, 73.5, 178))
    observations(17) = (80.80, Array[Double](84, 66.3, 136))
    observations(18) = (88.91, Array[Double](86, 70.0, 180))
    observations(19) = (90.59, Array[Double](84, 76.5, 186))
    observations(20) = (79.06, Array[Double](134, 62.0, 122))
    observations(21) = (95.50, Array[Double](128, 68.0, 132))
    observations(22) = (83.18, Array[Double](102, 63.0, 114))
    observations(23) = (93.55, Array[Double](131, 72.0, 171))
    observations(24) = (79.86, Array[Double](84, 68.0, 140))
    observations(25) = (106.25, Array[Double](110, 77.0, 187))
    observations(26) = (79.35, Array[Double](72, 63.0, 106))
    observations(27) = (86.67, Array[Double](124, 66.5, 159))
    observations(28) = (85.78, Array[Double](132, 62.5, 127))
    observations(29) = (94.96, Array[Double](137, 67.0, 191))
    observations(30) = (99.79, Array[Double](110, 75.5, 192))
    observations(31) = (88.00, Array[Double](86, 69.0, 181))
    observations(32) = (83.43, Array[Double](81, 66.5, 143))
    observations(33) = (94.81, Array[Double](128, 66.5, 153))
    observations(34) = (94.94, Array[Double](124, 70.5, 144))
    observations(35) = (89.40, Array[Double](94, 64.5, 139))
    observations(36) = (93.00, Array[Double](74, 74.0, 148))
    observations(37) = (93.59, Array[Double](89, 75.5, 179))

    // use additiveLinearAssociation to regress on PIQ data
    val (genotypes, phenotypes) = observations.unzip
    val genotypeStates = genotypes.toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cv = CalledVariant(1, 1, "rs123456", "A", "C", genotypeStates)

    val phenoMap = phenotypes
      .toList
      .zipWithIndex
      .map(item => (item._2.toString, Phenotype(item._2.toString, "pheno1", item._1(0), item._1.slice(1, 3).toList)))
      .toMap

    // use additiveLinearRegression to regress on PIQ
    val regressionResult = LinearSiteRegression.applyToSite(phenoMap, cv, "ADDITIVE")

    // Assert that the rsquared is in the right threshold.
    // R^2 = 1 - (SS_res / SS_tot)
    val rSquared = 1 - regressionResult.ssResiduals / regressionResult.ssDeviations
    assert(rSquared === 0.2954 +- 0.005)

    // Assert that the p-value for Brain is correct (expectedPVal ~= 0.000855632)
    assert(regressionResult.pValue === 0.000855632 +- 0.00005)
  }

  // LinearSiteRegression.sumOfSquaredDeviations tests

  ignore("LinearSiteRegression.applyToSite should correctly calculate the relevant statistics.") {

  }

  // LinearSiteRegression.applyToSite input validation tests

  sparkTest("LinearSiteRegression.applyToSite should break on a singular matrix") {
    val genotypeStates = List(GenotypeState("sample1", "0"))
    val cv = CalledVariant(1, 1, "rs123456", "A", "C", genotypeStates)

    val phenoMap = Map("sample1" -> Phenotype("sample1", "pheno1", 1))

    intercept[MatrixSingularException] {
      val regressionResult = LinearSiteRegression.applyToSite(phenoMap, cv, "ADDITIVE")
    }
  }

  sparkTest("LinearSiteRegression.applyToSite should break when there is not overlap between sampleIDs in phenotypes and CalledVariant objects.") {
    val genotypeStates = List(GenotypeState("sample1", "0"))
    val cv = CalledVariant(1, 1, "rs123456", "A", "C", genotypeStates)

    val phenoMap = Map("sample2" -> Phenotype("sample2", "pheno1", 1))

    intercept[IllegalArgumentException] {
      val regressionResult = LinearSiteRegression.applyToSite(phenoMap, cv, "ADDITIVE")
    }
  }

  ignore("LinearSiteRegression.applyToSite should not break with missing covariates.") {

  }

  ignore("LinearSiteRegression.applyToSite should call `TDistribution.cumulativeProbability`") {

  }

  // LinearSiteRegression.prepareDesignMatrix tests
  ignore("LinearSiteRegression.prepareDesignMatrix should produce a matrix with missing values filtered out.") {
    val observations = new Array[(String, Array[Double])](10)
    observations(0) = ("1", Array[Double](1))
    observations(1) = ("2", Array[Double](2))
    observations(2) = ("3", Array[Double](3))
    observations(3) = ("4", Array[Double](4))
    observations(4) = ("5", Array[Double](5))
    observations(5) = (".", Array[Double](6))
    observations(6) = (".", Array[Double](7))
    observations(7) = (".", Array[Double](8))
    observations(8) = (".", Array[Double](9))
    observations(9) = (".", Array[Double](10))

    val (genotypes, phenotypes) = observations.unzip
    val genotypeStates = genotypes.toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1))
    val cv = CalledVariant(1, 1, "rs123456", "A", "C", genotypeStates)

    val phenoMap = phenotypes
      .toList
      .zipWithIndex
      .map(item => (item._2.toString, Phenotype(item._2.toString, "pheno1", item._1(0), item._1.slice(1, 3).toList)))
      .toMap

    val (x, y) = LinearSiteRegression.prepareDesignMatrix(cv, phenoMap, "ADDITIVE")
    // Verify length of X and Y matrices
    assert(x.rows === 5)
    assert(y.length === 5)

    // Verify contents of matrices, function should filter out both genotypes and phenotypes
    assert(x(::, 1).toArray === genotypes.filter(_ != ".").map(_.toDouble))
    assert(y.toArray === phenotypes.slice(0, 5).flatten)
  }

  ignore("LinearSiteRegression.prepareDesignMatrix should create a label vector filled with phenotype values.") {
    val observations = new Array[(Double, Array[Double])](5)
    observations(0) = (10, Array[Double](1))
    observations(1) = (20, Array[Double](2))
    observations(2) = (30, Array[Double](3))
    observations(3) = (40, Array[Double](4))
    observations(4) = (50, Array[Double](5))

    val (genotypes, phenotypes) = observations.unzip
    val genotypeStates = genotypes.toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cv = CalledVariant(1, 1, "rs123456", "A", "C", genotypeStates)

    val phenoMap = phenotypes
      .toList
      .zipWithIndex
      .map(item => (item._2.toString, Phenotype(item._2.toString, "pheno1", item._1(0), item._1.slice(1, 3).toList)))
      .toMap

    val (x, y) = LinearSiteRegression.prepareDesignMatrix(cv, phenoMap, "ADDITIVE")

    // Verify length of Y label vector
    assert(y.length === 5)

    // Verify contents of Y label vector
    assert(y.toArray === phenotypes.flatten)
  }

  ignore("LinearSiteRegression.prepareDesignMatrix should place the genotype value in the first column of the design matrix.") {
    val observations = new Array[(Double, Array[Double])](5)
    observations(0) = (10, Array[Double](1, 6, 11))
    observations(1) = (20, Array[Double](2, 7, 12))
    observations(2) = (30, Array[Double](3, 8, 13))
    observations(3) = (40, Array[Double](4, 9, 14))
    observations(4) = (50, Array[Double](5, 10, 15))

    val (genotypes, phenotypes) = observations.unzip
    val genotypeStates = genotypes.toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cv = CalledVariant(1, 1, "rs123456", "A", "C", genotypeStates)

    val phenoMap = phenotypes
      .toList
      .zipWithIndex
      .map(item => (item._2.toString, Phenotype(item._2.toString, "pheno1", item._1(0), item._1.slice(1, 3).toList)))
      .toMap

    val (x, y) = LinearSiteRegression.prepareDesignMatrix(cv, phenoMap, "ADDITIVE")

    // Verify length of X data matrix
    assert(x.rows === 5)

    // Verify contents of X first non-intercept column
    assert(x(::, 1).toArray === genotypes)
  }

  ignore("LinearSiteRegression.prepareDesignMatrix should place the covariates in columns 1-n in the design matrix") {
    val observations = new Array[(Double, Array[Double])](5)
    observations(0) = (10, Array[Double](1, 6, 11))
    observations(1) = (20, Array[Double](2, 7, 12))
    observations(2) = (30, Array[Double](3, 8, 13))
    observations(3) = (40, Array[Double](4, 9, 14))
    observations(4) = (50, Array[Double](5, 10, 15))

    val (genotypes, phenotypes) = observations.unzip
    val genotypeStates = genotypes.toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cv = CalledVariant(1, 1, "rs123456", "A", "C", genotypeStates)

    val phenoMap = phenotypes
      .toList
      .zipWithIndex
      .map(item => (item._2.toString, Phenotype(item._2.toString, "pheno1", item._1(0), item._1.slice(1, 3).toList)))
      .toMap

    val (x, y) = LinearSiteRegression.prepareDesignMatrix(cv, phenoMap, "ADDITIVE")

    // Verify length of X data matrix
    assert(x.rows === 5)

    // Verify contents of X last columns
    assert(x(::, 2 until 4).toArray.grouped(5).toArray.transpose === phenotypes.map(_.slice(1, 3)))
  }

  // AdditiveLinearRegression tests

  ignore("AdditiveLinearRegression.constructVM should call the clip or keep state from the `Additive` trait.") {

  }

  sparkTest("LinearSiteRegression should have regressionName set to `LinearSiteRegression`") {
    assert(LinearSiteRegression.regressionName === "LinearSiteRegression")
  }

  // DominantLinearRegression tests

  ignore("DominantLinearRegression.constructVM should call the clip or keep state from the `Dominant` trait.") {

  }
}
