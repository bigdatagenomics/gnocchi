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

import breeze.linalg.{ DenseMatrix, DenseVector }
import org.apache.spark.sql.SparkSession
import org.bdgenomics.gnocchi.models.variant.LogisticVariantModel
import org.bdgenomics.gnocchi.primitives.genotype.GenotypeState
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.sql.{ GenotypeDataset, PhenotypesContainer }
import org.bdgenomics.gnocchi.utils.GnocchiFunSuite
import org.scalactic.Tolerance._

class LogisticSiteRegressionSuite extends GnocchiFunSuite {

  sparkTest("LogisticRegression.applyToSite works on binary UCLA grad school admissions data") {
    // read in the data from binary.csv
    // data comes from: https://stats.idre.ucla.edu/stat/data/binary.csv
    // results can be found here: https://stats.idre.ucla.edu/r/dae/logit-regression/
    val pathToFile = testFile("binary.csv")
    val csv = sc.textFile(pathToFile)
    val data = csv.map(line => line.split(",").map(elem => elem.toDouble)) //get rows

    // transform it into the right format
    val observations = data.map(row => {
      val x: Array[Double] = 1.0 +: row.slice(1, 4)
      val y: Double = row(0)
      (y, x)
    }).collect()

    val primitiveX = observations.map(x => x._2).toArray
    val primitiveY = observations.map(_._1.toDouble).toArray

    val x = new DenseMatrix(primitiveX.length, primitiveX(0).length, primitiveX.transpose.flatten)
    val y = new DenseVector(primitiveY)

    // feed it into logisitic regression and compare the Wald Chi Squared tests
    val (beta, hessian) = LogisticSiteRegression.solveRegression(x, y)

    // Assert that the weights are correct within a threshold. Note: Weights easily generated in R
    // mydata <- read.csv("https://stats.idre.ucla.edu/stat/data/binary.csv")
    // mylogit <- glm(admit ~ gre + gpa + rank, data = mydata, family = "binomial")
    // summary(mylogit)
    val compWeights = List(-3.449548, 0.002294, 0.777014, -0.560031)
    assert(beta(0) === compWeights(0) +- 0.00001, "Intercept incorrect")
    assert(beta(1) === compWeights(1) +- 0.00001, "Incorrect GRE weight")
    assert(beta(2) === compWeights(2) +- 0.00001, "Incorrect GPA weight")
    assert(beta(3) === compWeights(3) +- 0.00001, "Incorrect rank weight")

    val (genoSE, pValue) = LogisticSiteRegression.calculateSignificance(x, y, beta, hessian)

    //Assert that the Wald chi squared value is in the right threshold. Answer should be 0.0385
    assert(pValue === 0.03564 +- 0.0001, "p-Value calculation is incorrect.")
    assert(genoSE === 0.001092 +- 0.0001, "genoSE calculation is incorrect.")
  }

  // LogisticSiteRegression Correctness tests
  sparkTest("LogisticSiteRegression.applyToSite[Additive] should break gracefully on a singular matrix") {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    // This should produce a singular hessian matrix as all the rows will be identical
    val gs = createSampleGenotypeStates(num = 10, maf = 0.0, geno = 0.0, ploidy = 2)
    val cv = createSampleCalledVariant(samples = Option(gs))

    val cvDS = sparkSession.createDataset(List(cv))
    val genotypeDataset = GenotypeDataset(cvDS, "", "ADDITIVE", Set.empty)
    val phenos = sc.broadcast(createSamplePhenotype(calledVariant = Option(cv), phenoName = "pheno"))
    val phenotypesContainer = PhenotypesContainer(phenos, "pheno", None)

    val assoc = LogisticSiteRegression(genotypeDataset, phenotypesContainer).associations.collect

    // Note: due to lazy compute, the error won't actually
    // materialize till an action is called on the dataset, hence the collect

    assert(assoc.length == 0, "Singular Matrix test broken...(either a singular matrix was not created properly or it was not filtered out.)")
  }

  sparkTest("LogisticSiteRegression.applyToSite[Dominant] should break gracefully on a singular matrix") {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    // This should produce a singular hessian matrix as all the rows will be identical
    val gs = createSampleGenotypeStates(num = 10, maf = 0.0, geno = 0.0, ploidy = 2)
    val cv = createSampleCalledVariant(samples = Option(gs))

    val cvDS = sparkSession.createDataset(List(cv))
    val genotypeDataset = GenotypeDataset(cvDS, "", "DOMINANT", Set.empty)
    val phenos = sc.broadcast(createSamplePhenotype(calledVariant = Option(cv), phenoName = "pheno"))
    val phenotypesContainer = PhenotypesContainer(phenos, "pheno", None)

    val assoc = LogisticSiteRegression(genotypeDataset, phenotypesContainer).associations.collect

    // Note: due to lazy compute, the error won't actually
    // materialize till an action is called on the dataset, hence the collect

    assert(assoc.length == 0, "Singular Matrix test broken...(either a singular matrix was not created properly or it was not filtered out.)")
  }

  /**
   * plink --vcf gnocchi/gnocchi-core/src/test/resources/10Variants.vcf --make-bed --out 10Variants
   *
   * plink --bfile 10Variants
   *       --pheno 10PhenotypesLogistic.txt
   *       --pheno-name pheno_1
   *       --logistic
   *       --adjust
   *       --out test
   *       --allow-no-sex
   *       --mind 0.1
   *       --maf 0.1
   *       --geno 0.1
   *       --covar 10PhenotypesLogistic.txt
   *       --covar-name pheno_2,pheno_3
   *       --1
   */
  sparkTest("LogisticSiteRegression.applyToSite should match plink: Additive") {
    val variant = CalledVariant("rs8330247", 14, 21373362, "C", "T",
      Map(
        "7677" -> GenotypeState(1, 1, 0),
        "5218" -> GenotypeState(2, 0, 0),
        "1939" -> GenotypeState(0, 2, 0),
        "5695" -> GenotypeState(1, 1, 0),
        "4626" -> GenotypeState(2, 0, 0),
        "1933" -> GenotypeState(1, 1, 0),
        "1076" -> GenotypeState(2, 0, 0),
        "1534" -> GenotypeState(0, 2, 0),
        "1615" -> GenotypeState(2, 0, 0)))
    val phenotypes =
      Map("1939" -> Phenotype("1939", "pheno_1", 0.0, List(33.1311556631243, 17.335648977819975)),
        "1534" -> Phenotype("1534", "pheno_1", 1.0, List(43.5631372995055, 31.375377041144386)),
        "5218" -> Phenotype("5218", "pheno_1", 0.0, List(36.83233787900753, 15.335072581255679)),
        "4626" -> Phenotype("4626", "pheno_1", 0.0, List(24.718311412206525, 24.782686198847426)),
        "1933" -> Phenotype("1933", "pheno_1", 1.0, List(36.80339512174317, 25.749384943641015)),
        "5695" -> Phenotype("5695", "pheno_1", 0.0, List(39.47830201958979, 14.931122428970518)),
        "1076" -> Phenotype("1076", "pheno_1", 1.0, List(29.20731787959392, 23.113886863554768)),
        "1615" -> Phenotype("1615", "pheno_1", 1.0, List(36.086511107929894, 26.646338451664146)),
        "7677" -> Phenotype("7677", "pheno_1", 0.0, List(41.934512951913796, 28.422437596286894)))

    val (model, association) = LogisticSiteRegression.applyToSite(variant, phenotypes, "ADDITIVE")
    assert(association.pValue === 0.7090 +- 0.0001, "LinearSiteRegression.apply to site deviates significantly from plink!")
  }

  /**
   * plink --vcf gnocchi/gnocchi-core/src/test/resources/10Variants.vcf --make-bed --out 10Variants
   *
   * plink --bfile 10Variants
   *       --pheno 10PhenotypesLogistic.txt
   *       --pheno-name pheno_1
   *       --logistic dominant
   *       --adjust
   *       --out test
   *       --allow-no-sex
   *       --mind 0.1
   *       --maf 0.1
   *       --geno 0.1
   *       --covar 10PhenotypesLogistic.txt
   *       --covar-name pheno_2,pheno_3
   *       --1
   */
  sparkTest("LogisticSiteRegression.applyToSite should match plink: Dominant") {
    val variant = CalledVariant("rs8330247", 14, 21373362, "C", "T",
      Map(
        "7677" -> GenotypeState(1, 1, 0),
        "5218" -> GenotypeState(2, 0, 0),
        "1939" -> GenotypeState(0, 2, 0),
        "5695" -> GenotypeState(1, 1, 0),
        "4626" -> GenotypeState(2, 0, 0),
        "1933" -> GenotypeState(1, 1, 0),
        "1076" -> GenotypeState(2, 0, 0),
        "1534" -> GenotypeState(0, 2, 0),
        "1615" -> GenotypeState(2, 0, 0)))
    val phenotypes =
      Map("1939" -> Phenotype("1939", "pheno_1", 0.0, List(33.1311556631243, 17.335648977819975)),
        "1534" -> Phenotype("1534", "pheno_1", 1.0, List(43.5631372995055, 31.375377041144386)),
        "5218" -> Phenotype("5218", "pheno_1", 0.0, List(36.83233787900753, 15.335072581255679)),
        "4626" -> Phenotype("4626", "pheno_1", 0.0, List(24.718311412206525, 24.782686198847426)),
        "1933" -> Phenotype("1933", "pheno_1", 1.0, List(36.80339512174317, 25.749384943641015)),
        "5695" -> Phenotype("5695", "pheno_1", 0.0, List(39.47830201958979, 14.931122428970518)),
        "1076" -> Phenotype("1076", "pheno_1", 1.0, List(29.20731787959392, 23.113886863554768)),
        "1615" -> Phenotype("1615", "pheno_1", 1.0, List(36.086511107929894, 26.646338451664146)),
        "7677" -> Phenotype("7677", "pheno_1", 0.0, List(41.934512951913796, 28.422437596286894)))

    val (model, association) = LogisticSiteRegression.applyToSite(variant, phenotypes, "DOMINANT")
    assert(association.pValue === 0.5010 +- 0.0001, "LinearSiteRegression.apply to site deviates significantly from plink!")
  }

  // LogisticSiteRegression.applyToSite input validation tests
  sparkTest("LogisticSiteRegression.applyToSite should break when there is not overlap between sampleIDs in phenotypes and CalledVariant objects.") {
    val variant = CalledVariant("rs8330247", 14, 21373362, "C", "T",
      Map(
        "1" -> GenotypeState(1, 1, 0),
        "2" -> GenotypeState(2, 0, 0),
        "3" -> GenotypeState(0, 2, 0),
        "4" -> GenotypeState(1, 1, 0),
        "5" -> GenotypeState(2, 0, 0),
        "6" -> GenotypeState(1, 1, 0),
        "7" -> GenotypeState(2, 0, 0),
        "8" -> GenotypeState(0, 2, 0),
        "9" -> GenotypeState(2, 0, 0)))

    val phenotypes =
      Map("1939" -> Phenotype("1939", "pheno_1", 0.0, List(33.1311556631243, 17.335648977819975)),
        "1534" -> Phenotype("1534", "pheno_1", 1.0, List(43.5631372995055, 31.375377041144386)),
        "5218" -> Phenotype("5218", "pheno_1", 0.0, List(36.83233787900753, 15.335072581255679)),
        "4626" -> Phenotype("4626", "pheno_1", 0.0, List(24.718311412206525, 24.782686198847426)),
        "1933" -> Phenotype("1933", "pheno_1", 1.0, List(36.80339512174317, 25.749384943641015)),
        "5695" -> Phenotype("5695", "pheno_1", 0.0, List(39.47830201958979, 14.931122428970518)),
        "1076" -> Phenotype("1076", "pheno_1", 1.0, List(29.20731787959392, 23.113886863554768)),
        "1615" -> Phenotype("1615", "pheno_1", 1.0, List(36.086511107929894, 26.646338451664146)),
        "7677" -> Phenotype("7677", "pheno_1", 0.0, List(41.934512951913796, 28.422437596286894)))

    try {
      LogisticSiteRegression.applyToSite(variant, phenotypes, "ADDITIVE")
      fail("No overlap between phenotype sample IDs and genotype sample IDs did not cause a break.")
    } catch {
      case _: IllegalArgumentException =>
      case e: Throwable                => { print(e); fail("exception thrown") }
    }
  }

  sparkTest("LogisticSiteRegression.applyToSite should not break with missing covariates.") {
    val variant = CalledVariant("rs8330247", 14, 21373362, "C", "T",
      Map(
        "7677" -> GenotypeState(1, 1, 0),
        "5218" -> GenotypeState(2, 0, 0),
        "1939" -> GenotypeState(0, 2, 0),
        "5695" -> GenotypeState(1, 1, 0),
        "4626" -> GenotypeState(2, 0, 0),
        "1933" -> GenotypeState(1, 1, 0),
        "1076" -> GenotypeState(2, 0, 0),
        "1534" -> GenotypeState(0, 2, 0),
        "1615" -> GenotypeState(2, 0, 0)))

    val phenotypes =
      Map("1939" -> Phenotype("1939", "pheno_1", 0.0, List()),
        "1534" -> Phenotype("1534", "pheno_1", 1.0, List()),
        "5218" -> Phenotype("5218", "pheno_1", 0.0, List()),
        "4626" -> Phenotype("4626", "pheno_1", 0.0, List()),
        "1933" -> Phenotype("1933", "pheno_1", 1.0, List()),
        "5695" -> Phenotype("5695", "pheno_1", 0.0, List()),
        "1076" -> Phenotype("1076", "pheno_1", 1.0, List()),
        "1615" -> Phenotype("1615", "pheno_1", 1.0, List()),
        "7677" -> Phenotype("7677", "pheno_1", 0.0, List()))

    try {
      LogisticSiteRegression.applyToSite(variant, phenotypes, "ADDITIVE")
    } catch {
      case e: Throwable => { print(e); fail("exception thrown") }
    }
  }

  // LogisticSiteRegression.findBeta tests

  sparkTest("LogisticSiteRegression.findBeta should throw a singular matrix exception when hessian is noninvertible.") {
    val gs = createSampleGenotypeStates(num = 10, maf = 0.0, geno = 0.0, ploidy = 2)
    val cv = createSampleCalledVariant(samples = Option(gs))
    val phenos = createSamplePhenotype(calledVariant = Option(cv))

    val (data, label) = LogisticSiteRegression.prepareDesignMatrix(cv, phenos, "ADDITIVE")
    val beta = DenseVector.zeros[Double](data.cols)

    try {
      LogisticSiteRegression.findBeta(data, label, beta)
      fail("LogisticSiteRegression.findBeta does not break on singular hessian.")
    } catch {
      case e: breeze.linalg.MatrixSingularException =>
    }
  }
}
