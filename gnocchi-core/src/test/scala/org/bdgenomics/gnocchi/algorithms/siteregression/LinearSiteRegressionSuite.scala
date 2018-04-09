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

import breeze.linalg.{ DenseMatrix, DenseVector, MatrixSingularException }
import org.bdgenomics.gnocchi.primitives.genotype.GenotypeState
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.utils.GnocchiFunSuite
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
  sparkTest("LinearSiteRegression.applyToSite should calculate rsquared and p-value correctly for Anscombe I.") {
    // Note: This test checks if the two calls used to solve regression in LinearSiteRegression
    // actually produce the right result rather than calling applyToSite
    // load AnscombeI into an observations variable
    val anscombeI =
      List(
        (10.0, 8.04),
        (8.0, 6.95),
        (13.0, 7.58),
        (9.0, 8.81),
        (11.0, 8.33),
        (14.0, 9.96),
        (6.0, 7.24),
        (4.0, 4.26),
        (12.0, 10.84),
        (7.0, 4.82),
        (5.0, 5.68))

    val primitiveX = anscombeI.map(x => Array(1.0, x._1)).toArray
    val primitiveY = anscombeI.map(_._2).toArray

    val x = new DenseMatrix(primitiveX.length, primitiveX(0).length, primitiveX.transpose.flatten)
    val y = new DenseVector(primitiveY)

    val (xTx, xTy, beta) = LinearSiteRegression.solveRegression(x, y)

    val (genoSE, t, pValue, ssResiduals) = LinearSiteRegression.calculateSignificance(
      x: DenseMatrix[Double],
      y: DenseVector[Double],
      beta: DenseVector[Double],
      xTx: DenseMatrix[Double])

    val meanY = anscombeI.map(_._2).sum / anscombeI.size.toDouble
    val ssDeviations = anscombeI.map(x => Math.pow(x._2 - meanY, 2)).sum

    // Assert that the rsquared is in the right threshold.
    // R^2 = 1 - (SS_res / SS_tot)
    val rSquared = 1 - ssResiduals / ssDeviations
    assert(rSquared === 0.6665 +- 0.005)

    // Assert that the p-value for independent variable is correct (expectedPVal ~= 0.002169629)
    assert(pValue === 0.002169629 +- 0.00005)
  }

  sparkTest("LinearSiteRegression.applyToSite should calculate rsquared and p-value correctly for Anscombe II.") {
    // load AnscombeII into an observations variable
    val anscombeII =
      List(
        (10.0, 9.14),
        (8.0, 8.14),
        (13.0, 8.74),
        (9.0, 8.77),
        (11.0, 9.26),
        (14.0, 8.10),
        (6.0, 6.13),
        (4.0, 3.10),
        (12.0, 9.13),
        (7.0, 7.26),
        (5.0, 4.74))

    val primitiveX = anscombeII.map(x => Array(1.0, x._1)).toArray
    val primitiveY = anscombeII.map(_._2).toArray

    val x = new DenseMatrix(primitiveX.length, primitiveX(0).length, primitiveX.transpose.flatten)
    val y = new DenseVector(primitiveY)

    val (xTx, xTy, beta) = LinearSiteRegression.solveRegression(x, y)

    val (genoSE, t, pValue, ssResiduals) = LinearSiteRegression.calculateSignificance(
      x: DenseMatrix[Double],
      y: DenseVector[Double],
      beta: DenseVector[Double],
      xTx: DenseMatrix[Double])

    val meanY = anscombeII.map(_._2).sum / anscombeII.size.toDouble
    val ssDeviations = anscombeII.map(x => Math.pow(x._2 - meanY, 2)).sum

    // Assert that the rsquared is in the right threshold.
    // R^2 = 1 - (SS_res / SS_tot)
    val rSquared = 1 - ssResiduals / ssDeviations
    assert(rSquared === 0.6662 +- 0.005)

    // Assert that the p-value for independent variable is correct (expectedPVal ~= 0.002178816)
    assert(pValue === 0.002178816 +- 0.00005)
  }

  sparkTest("LinearSiteRegression.applyToSite should calculate rsquared and p-value correctly for Anscombe III.") {
    // load AnscombeIII into an observations variable
    val anscombeIII =
      List(
        (10.0, 7.46),
        (8.0, 6.77),
        (13.0, 12.74),
        (9.0, 7.11),
        (11.0, 7.81),
        (14.0, 8.84),
        (6.0, 6.08),
        (4.0, 5.39),
        (12.0, 8.15),
        (7.0, 6.42),
        (5.0, 5.73))

    val primitiveX = anscombeIII.map(x => Array(1.0, x._1)).toArray
    val primitiveY = anscombeIII.map(_._2).toArray

    val x = new DenseMatrix(primitiveX.length, primitiveX(0).length, primitiveX.transpose.flatten)
    val y = new DenseVector(primitiveY)

    val (xTx, xTy, beta) = LinearSiteRegression.solveRegression(x, y)

    val (genoSE, t, pValue, ssResiduals) = LinearSiteRegression.calculateSignificance(
      x: DenseMatrix[Double],
      y: DenseVector[Double],
      beta: DenseVector[Double],
      xTx: DenseMatrix[Double])

    val meanY = anscombeIII.map(_._2).sum / anscombeIII.size.toDouble
    val ssDeviations = anscombeIII.map(x => Math.pow(x._2 - meanY, 2)).sum

    // Assert that the rsquared is in the right threshold.
    // R^2 = 1 - (SS_res / SS_tot)
    val rSquared = 1 - ssResiduals / ssDeviations
    assert(rSquared === 0.6663 +- 0.005)

    // Assert that the p-value for independent variable is correct (expectedPVal ~= 0.002176305)
    assert(pValue === 0.002176305 +- 0.00005)
  }

  sparkTest("LinearSiteRegression.applyToSite should calculate rsquared and p-value correctly for Anscombe IV.") {
    //load AnscombeIV into an observations variable
    val anscombeIV =
      List(
        (8.0, 6.58),
        (8.0, 5.76),
        (8.0, 7.71),
        (8.0, 8.84),
        (8.0, 8.47),
        (8.0, 7.04),
        (8.0, 5.25),
        (19.0, 12.50),
        (8.0, 5.56),
        (8.0, 7.91),
        (8.0, 6.89))

    val primitiveX = anscombeIV.map(x => Array(1.0, x._1)).toArray
    val primitiveY = anscombeIV.map(_._2).toArray

    val x = new DenseMatrix(primitiveX.length, primitiveX(0).length, primitiveX.transpose.flatten)
    val y = new DenseVector(primitiveY)

    val (xTx, xTy, beta) = LinearSiteRegression.solveRegression(x, y)

    val (genoSE, t, pValue, ssResiduals) = LinearSiteRegression.calculateSignificance(
      x: DenseMatrix[Double],
      y: DenseVector[Double],
      beta: DenseVector[Double],
      xTx: DenseMatrix[Double])

    val meanY = anscombeIV.map(_._2).sum / anscombeIV.size.toDouble
    val ssDeviations = anscombeIV.map(x => Math.pow(x._2 - meanY, 2)).sum

    // Assert that the rsquared is in the right threshold.
    // R^2 = 1 - (SS_res / SS_tot)
    val rSquared = 1 - ssResiduals / ssDeviations
    assert(rSquared === 0.6667 +- 0.005)

    // Assert that the p-value for independent variable is correct (expectedPVal ~= 0.002164602)
    assert(pValue === 0.002164602 +- 0.00005)
  }

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
  sparkTest("LinearSiteRegression.applyToSite should work correctly for PIQ data.") {
    val observations =
      List(
        (124, Array[Double](1.0, 81.69, 64.5, 118)),
        (150, Array[Double](1.0, 103.84, 73.3, 143)),
        (128, Array[Double](1.0, 96.54, 68.8, 172)),
        (134, Array[Double](1.0, 95.15, 65.0, 147)),
        (110, Array[Double](1.0, 92.88, 69.0, 146)),
        (131, Array[Double](1.0, 99.13, 64.5, 138)),
        (98, Array[Double](1.0, 85.43, 66.0, 175)),
        (84, Array[Double](1.0, 90.49, 66.3, 134)),
        (147, Array[Double](1.0, 95.55, 68.8, 172)),
        (124, Array[Double](1.0, 83.39, 64.5, 118)),
        (128, Array[Double](1.0, 107.95, 70.0, 151)),
        (124, Array[Double](1.0, 92.41, 69.0, 155)),
        (147, Array[Double](1.0, 85.65, 70.5, 155)),
        (90, Array[Double](1.0, 87.89, 66.0, 146)),
        (96, Array[Double](1.0, 86.54, 68.0, 135)),
        (120, Array[Double](1.0, 85.22, 68.5, 127)),
        (102, Array[Double](1.0, 94.51, 73.5, 178)),
        (84, Array[Double](1.0, 80.80, 66.3, 136)),
        (86, Array[Double](1.0, 88.91, 70.0, 180)),
        (84, Array[Double](1.0, 90.59, 76.5, 186)),
        (134, Array[Double](1.0, 79.06, 62.0, 122)),
        (128, Array[Double](1.0, 95.50, 68.0, 132)),
        (102, Array[Double](1.0, 83.18, 63.0, 114)),
        (131, Array[Double](1.0, 93.55, 72.0, 171)),
        (84, Array[Double](1.0, 79.86, 68.0, 140)),
        (110, Array[Double](1.0, 106.25, 77.0, 187)),
        (72, Array[Double](1.0, 79.35, 63.0, 106)),
        (124, Array[Double](1.0, 86.67, 66.5, 159)),
        (132, Array[Double](1.0, 85.78, 62.5, 127)),
        (137, Array[Double](1.0, 94.96, 67.0, 191)),
        (110, Array[Double](1.0, 99.79, 75.5, 192)),
        (86, Array[Double](1.0, 88.00, 69.0, 181)),
        (81, Array[Double](1.0, 83.43, 66.5, 143)),
        (128, Array[Double](1.0, 94.81, 66.5, 153)),
        (124, Array[Double](1.0, 94.94, 70.5, 144)),
        (94, Array[Double](1.0, 89.40, 64.5, 139)),
        (74, Array[Double](1.0, 93.00, 74.0, 148)),
        (89, Array[Double](1.0, 93.59, 75.5, 179)))

    val primitiveX = observations.map(x => x._2).toArray
    val primitiveY = observations.map(_._1.toDouble).toArray

    val x = new DenseMatrix(primitiveX.length, primitiveX(0).length, primitiveX.transpose.flatten)
    val y = new DenseVector(primitiveY)

    val (xTx, xTy, beta) = LinearSiteRegression.solveRegression(x, y)

    val (genoSE, t, pValue, ssResiduals) = LinearSiteRegression.calculateSignificance(
      x: DenseMatrix[Double],
      y: DenseVector[Double],
      beta: DenseVector[Double],
      xTx: DenseMatrix[Double])

    val meanY = observations.map(_._1).sum / observations.size.toDouble
    val ssDeviations = observations.map(x => Math.pow(x._1 - meanY, 2)).sum

    // Assert that the rsquared is in the right threshold.
    // R^2 = 1 - (SS_res / SS_tot)
    val rSquared = 1 - ssResiduals / ssDeviations
    assert(rSquared === 0.2954 +- 0.005)

    // Assert that the p-value for Brain is correct (expectedPVal ~= 0.000855632)
    assert(pValue === 0.000855632 +- 0.00005)
  }

  /**
   * plink --vcf gnocchi/gnocchi-core/src/test/resources/10Variants.vcf --make-bed --out 10Variants
   * plink --bfile 10Variants --pheno 10PhenotypesLinear.txt --pheno-name pheno_1 --linear --adjust --out test --allow-no-sex --mind 0.1 --maf 0.1 --geno 0.1
   */
  sparkTest("LinearSiteRegression.applyToSite should match plink: Additive") {
    val variant = CalledVariant("rs8330247", 14, 21373362, "C", "T",
      List(
        GenotypeState("7677", 1, 1, 0),
        GenotypeState("5218", 2, 0, 0),
        GenotypeState("1939", 0, 2, 0),
        GenotypeState("5695", 1, 1, 0),
        GenotypeState("4626", 2, 0, 0),
        GenotypeState("1933", 1, 1, 0),
        GenotypeState("1076", 2, 0, 0),
        GenotypeState("1534", 0, 2, 0),
        GenotypeState("1615", 2, 0, 0)))
    val phenotypes =
      Map(
        "1939" -> Phenotype("1939", "pheno_1", 51.75673646004061, List()),
        "2792" -> Phenotype("2792", "pheno_1", 62.22934974654722, List()),
        "1534" -> Phenotype("1534", "pheno_1", 51.568591214841405, List()),
        "5218" -> Phenotype("5218", "pheno_1", 51.57128192897128, List()),
        "4626" -> Phenotype("4626", "pheno_1", 71.50143228329485, List()),
        "1933" -> Phenotype("1933", "pheno_1", 61.24800827007204, List()),
        "5695" -> Phenotype("5695", "pheno_1", 56.08340448063026, List()),
        "1076" -> Phenotype("1076", "pheno_1", 63.756223063707154, List()),
        "1615" -> Phenotype("1615", "pheno_1", 69.45757502327798, List()),
        "7677" -> Phenotype("7677", "pheno_1", 60.18207070928484, List()))

    val (model, association) = LinearSiteRegression.applyToSite(variant, phenotypes, "ADDITIVE")
    assert(association.pValue === 0.05148 +- 0.0001, "LinearSiteRegression.apply to site deviates significantly from plink!")
  }

  /**
   * plink --vcf gnocchi/gnocchi-core/src/test/resources/10Variants.vcf --make-bed --out 10Variants
   * plink --bfile 10Variants --pheno 10PhenotypesLinear.txt --pheno-name pheno_1 --linear dominant --adjust --out test --allow-no-sex --mind 0.1 --maf 0.1 --geno 0.1
   */
  sparkTest("LinearSiteRegression.applyToSite should match plink: Dominant") {
    val variant = CalledVariant("rs8330247", 14, 21373362, "C", "T",
      List(
        GenotypeState("7677", 1, 1, 0),
        GenotypeState("5218", 2, 0, 0),
        GenotypeState("1939", 0, 2, 0),
        GenotypeState("5695", 1, 1, 0),
        GenotypeState("4626", 2, 0, 0),
        GenotypeState("1933", 1, 1, 0),
        GenotypeState("1076", 2, 0, 0),
        GenotypeState("1534", 0, 2, 0),
        GenotypeState("1615", 2, 0, 0)))

    val phenotypes =
      Map(
        "1939" -> Phenotype("1939", "pheno_1", 51.75673646004061, List()),
        "2792" -> Phenotype("2792", "pheno_1", 62.22934974654722, List()),
        "1534" -> Phenotype("1534", "pheno_1", 51.568591214841405, List()),
        "5218" -> Phenotype("5218", "pheno_1", 51.57128192897128, List()),
        "4626" -> Phenotype("4626", "pheno_1", 71.50143228329485, List()),
        "1933" -> Phenotype("1933", "pheno_1", 61.24800827007204, List()),
        "5695" -> Phenotype("5695", "pheno_1", 56.08340448063026, List()),
        "1076" -> Phenotype("1076", "pheno_1", 63.756223063707154, List()),
        "1615" -> Phenotype("1615", "pheno_1", 69.45757502327798, List()),
        "7677" -> Phenotype("7677", "pheno_1", 60.18207070928484, List()))

    val (model, association) = LinearSiteRegression.applyToSite(variant, phenotypes, "DOMINANT")
    assert(association.pValue === 0.1265 +- 0.0001, "LinearSiteRegression.apply to site deviates significantly from plink!")
  }

  // LinearSiteRegression.applyToSite input validation tests
  sparkTest("LinearSiteRegression.applyToSite should break on a singular matrix") {
    val genotypeStates = List(GenotypeState("sample1", 1, 0, 0))
    val cv = CalledVariant("rs123456", 1, 1, "A", "C", genotypeStates)

    val phenoMap = Map("sample1" -> Phenotype("sample1", "pheno1", 1))

    intercept[MatrixSingularException] {
      LinearSiteRegression.applyToSite(cv, phenoMap, "ADDITIVE")
    }
  }

  sparkTest("LinearSiteRegression.applyToSite should break when there is not overlap between sampleIDs in phenotypes and CalledVariant objects.") {
    val genotypeStates = List(GenotypeState("sample1", 1, 0, 1))
    val cv = CalledVariant("rs123456", 1, 1, "A", "C", genotypeStates)

    val phenoMap = Map("sample2" -> Phenotype("sample2", "pheno1", 1))

    intercept[IllegalArgumentException] {
      LinearSiteRegression.applyToSite(cv, phenoMap, "ADDITIVE")
    }
  }

  // LinearSiteRegression.prepareDesignMatrix tests
  // This test is bulky, rewrite
  sparkTest("LinearSiteRegression.prepareDesignMatrix should produce a matrix with missing values filtered out.") {
    val observations = new Array[(String, Array[Double])](10)
    observations(0) = ("1/0", Array[Double](1, 1, 1, 1))
    observations(1) = ("0/1", Array[Double](2, 2, 2, 2))
    observations(2) = ("0/1", Array[Double](3, 3, 3, 3))
    observations(3) = ("0/0", Array[Double](4, 4, 4, 4))
    observations(4) = ("1/1", Array[Double](5, 5, 5, 5))
    observations(5) = ("1/.", Array[Double](6, 6, 6, 6))
    observations(6) = ("./.", Array[Double](7, 7, 7, 7))
    observations(7) = ("./0", Array[Double](8, 8, 8, 8))
    observations(8) = ("./.", Array[Double](9, 9, 9, 9))
    observations(9) = ("./1", Array[Double](10, 10, 10, 10))

    val (genotypes, phenotypes) = observations.unzip
    val genotypeStates =
      genotypes
        .toList
        .map(_.split("/|\\|"))
        .zipWithIndex
        .map(item =>
          GenotypeState(
            item._2.toString,
            item._1.count(_ == "0").toByte,
            item._1.count(_ == "1").toByte,
            item._1.count(_ == ".").toByte))
    val cv = CalledVariant("rs123456", 1, 1, "A", "C", genotypeStates)

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
    assert(x(::, 1).toArray === genotypeStates.filter(_.misses == 0).map(_.alts.toDouble))
    assert(y.toArray === phenotypes.slice(0, 5).map(_(0)))
  }

  // This test is bulky, rewrite
  sparkTest("LinearSiteRegression.prepareDesignMatrix should create a label vector filled with phenotype values.") {
    val observations = new Array[(Double, Array[Double])](5)
    observations(0) = (10, Array[Double](1))
    observations(1) = (20, Array[Double](2))
    observations(2) = (30, Array[Double](3))
    observations(3) = (40, Array[Double](4))
    observations(4) = (50, Array[Double](5))

    val (genotypes, phenotypes) = observations.unzip
    val genotypeStates = genotypes.toList.zipWithIndex.map(item => GenotypeState(item._2.toString, 1, 1, 0))
    val cv = CalledVariant("rs123456", 1, 1, "A", "C", genotypeStates)

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

  sparkTest("LinearSiteRegression.prepareDesignMatrix should place the genotype value in the first column of the design matrix.") {
    val observations = new Array[(String, Array[Double])](5)
    observations(0) = ("1/0", Array[Double](1, 2, 3))
    observations(1) = ("0/1", Array[Double](2, 3, 4))
    observations(2) = ("0/1", Array[Double](3, 4, 5))
    observations(3) = ("0/0", Array[Double](4, 5, 6))
    observations(4) = ("1/1", Array[Double](5, 6, 7))

    val (genotypes, phenotypes) = observations.unzip
    val genotypeStates =
      genotypes
        .toList
        .map(_.split("/|\\|"))
        .zipWithIndex.map(
          item => GenotypeState(
            item._2.toString,
            item._1.count(_ == "0").toByte,
            item._1.count(_ == "1").toByte,
            item._1.count(_ == ".").toByte))
    val cv = CalledVariant("rs123456", 1, 1, "A", "C", genotypeStates)

    val phenoMap = phenotypes
      .toList
      .zipWithIndex
      .map(item => (item._2.toString, Phenotype(item._2.toString, "pheno1", item._1(0), item._1.slice(1, 2).toList)))
      .toMap

    val (x, y) = LinearSiteRegression.prepareDesignMatrix(cv, phenoMap, "ADDITIVE")

    // Verify length of X data matrix
    assert(x.rows === 5)

    // Verify contents of X first non-intercept column
    assert(x(::, 1).toArray === genotypeStates.map(_.alts.toDouble).toArray)
  }

  sparkTest("LinearSiteRegression.prepareDesignMatrix should place the covariates in columns 1-n in the design matrix") {
    val observations = new Array[(String, Array[Double])](5)
    observations(0) = ("1/0", Array[Double](1, 2, 3))
    observations(1) = ("0/1", Array[Double](2, 3, 4))
    observations(2) = ("0/1", Array[Double](3, 4, 5))
    observations(3) = ("0/0", Array[Double](4, 5, 6))
    observations(4) = ("1/1", Array[Double](5, 6, 7))

    val (genotypes, phenotypes) = observations.unzip
    val genotypeStates =
      genotypes
        .toList
        .map(_.split("/|\\|"))
        .zipWithIndex.map(
          item => GenotypeState(
            item._2.toString,
            item._1.count(_ == "0").toByte,
            item._1.count(_ == "1").toByte,
            item._1.count(_ == ".").toByte))
    val cv = CalledVariant("rs123456", 1, 1, "A", "C", genotypeStates)

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

  sparkTest("LinearSiteRegression.prepareDesignMatrix should correctly take into account the allelic assumption: Additive") {
    val observations = new Array[(String, Array[Double])](5)
    observations(0) = ("1/0", Array[Double](1, 1, 1, 1))
    observations(1) = ("0/1", Array[Double](2, 2, 2, 2))
    observations(2) = ("0/1", Array[Double](3, 3, 3, 3))
    observations(3) = ("0/0", Array[Double](4, 4, 4, 4))
    observations(4) = ("1/1", Array[Double](5, 5, 5, 5))

    val (genotypes, phenotypes) = observations.unzip
    val genotypeStates =
      genotypes
        .toList
        .map(_.split("/|\\|"))
        .zipWithIndex
        .map(item =>
          GenotypeState(
            item._2.toString,
            item._1.count(_ == "0").toByte,
            item._1.count(_ == "1").toByte,
            item._1.count(_ == ".").toByte))
    val cv = CalledVariant("rs123456", 1, 1, "A", "C", genotypeStates)

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
    assert(x(::, 1).toArray === genotypeStates.filter(_.misses == 0).map(_.additive))
  }

  sparkTest("LinearSiteRegression.prepareDesignMatrix should correctly take into account the allelic assumption: Dominant") {
    val observations = new Array[(String, Array[Double])](5)
    observations(0) = ("1/0", Array[Double](1, 1, 1, 1))
    observations(1) = ("0/1", Array[Double](2, 2, 2, 2))
    observations(2) = ("0/1", Array[Double](3, 3, 3, 3))
    observations(3) = ("0/0", Array[Double](4, 4, 4, 4))
    observations(4) = ("1/1", Array[Double](5, 5, 5, 5))

    val (genotypes, phenotypes) = observations.unzip
    val genotypeStates =
      genotypes
        .toList
        .map(_.split("/|\\|"))
        .zipWithIndex
        .map(item =>
          GenotypeState(
            item._2.toString,
            item._1.count(_ == "0").toByte,
            item._1.count(_ == "1").toByte,
            item._1.count(_ == ".").toByte))
    val cv = CalledVariant("rs123456", 1, 1, "A", "C", genotypeStates)

    val phenoMap = phenotypes
      .toList
      .zipWithIndex
      .map(item => (item._2.toString, Phenotype(item._2.toString, "pheno1", item._1(0), item._1.slice(1, 3).toList)))
      .toMap

    val (x, y) = LinearSiteRegression.prepareDesignMatrix(cv, phenoMap, "DOMINANT")

    // Verify length of X and Y matrices
    assert(x.rows === 5)
    assert(y.length === 5)

    // Verify contents of matrices, function should filter out both genotypes and phenotypes
    assert(x(::, 1).toArray === genotypeStates.filter(_.misses == 0).map(_.dominant))
  }
}
