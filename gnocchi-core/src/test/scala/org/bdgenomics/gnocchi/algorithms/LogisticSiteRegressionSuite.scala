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
package org.bdgenomics.gnocchi.algorithms

import breeze.linalg
import breeze.linalg.{ DenseMatrix, DenseVector, MatrixSingularException }
import org.bdgenomics.gnocchi.algorithms.siteregression.LogisticSiteRegression
import org.bdgenomics.gnocchi.primitives.association.LogisticAssociation
import org.apache.spark.sql.SparkSession
import org.bdgenomics.gnocchi.GnocchiFunSuite
import org.bdgenomics.gnocchi.models.variant.LogisticVariantModel
import org.mockito.{ ArgumentMatchers, Mockito }

class LogisticSiteRegressionSuite extends GnocchiFunSuite {

  //
  //  sparkTest("Test logisticRegression on binary data") {
  //    // read in the data from binary.csv
  //    // data comes from: http://www.ats.ucla.edu/stat/sas/dae/binary.sas7bdat
  //    // results can be found here: http://www.ats.ucla.edu/stat/sas/dae/logit.htm
  //    val pathToFile = ClassLoader.getSystemClassLoader.getResource("binary.csv").getFile
  //    val csv = sc.textFile(pathToFile)
  //    val data = csv.map(line => line.split(",").map(elem => elem.toDouble)) //get rows
  //
  //    // transform it into the right format
  //    val observations = data.map(row => {
  //      val geno: Double = row(0)
  //      val covars: Array[Double] = row.slice(1, 3)
  //      val phenos: Array[Double] = Array(row(3)) ++ covars
  //      (geno, phenos)
  //    }).collect()
  //    val altAllele = "No allele"
  //    val phenotype = "acceptance"
  //    val locus = ReferenceRegion("Name", 1, 2)
  //    val scOption = Option(sc)
  //    val variant = new Variant
  //    //    val contig = new Contig()
  //    //    contig.setContigName(locus.referenceName)
  //    variant.setContigName(locus.referenceName)
  //    variant.setStart(locus.start)
  //    variant.setEnd(locus.end)
  //    variant.setAlternateAllele(altAllele)
  //    val phaseSetId = 0
  //
  //    // feed it into logisitic regression and compare the Wald Chi Squared tests
  //    val regressionResult = AdditiveLogisticRegression.applyToSite(observations, variant, phenotype, phaseSetId)
  //
  //    // Assert that the weights are correct within a threshold.
  //    val estWeights: Array[Double] = regressionResult.statistics("weights").asInstanceOf[Array[Double]] :+ regressionResult.statistics("intercept").asInstanceOf[Double]
  //    val compWeights = Array(-3.4495484, .0022939, .77701357, -0.5600314)
  //    for (i <- 0 until 3) {
  //      assert(estWeights(i) <= (compWeights(i) + 1), s"Weight $i incorrect")
  //      assert(estWeights(i) >= (compWeights(i) - 1), s"Weight $i incorrect")
  //    }
  //    //Assert that the Wald chi squared value is in the right threshold. Answer should be 0.0385
  //    val pval: Array[Double] = regressionResult.statistics("'P Values' aka Wald Tests").asInstanceOf[DenseVector[Double]].toArray
  //    assert(pval(1) <= 0.0385 + 0.01, "'P Values' aka Wald Tests = " + pval)
  //    assert(pval(1) >= 0.0385 - 0.01, "'P Values' aka Wald Tests = " + pval)
  //  }

  // LogisticSiteRegression Correctness tests

  sparkTest("LogisticSiteRegression.applyToSite[Additive] should break gracefully on a singular matrix") {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    // This should produce a singular hessian matrix as all the rows will be identical
    val gs = createSampleGenotypeStates(num = 10, maf = 0.0, geno = 0.0, ploidy = 2)
    val cv = createSampleCalledVariant(samples = Option(gs))

    val cvDS = sparkSession.createDataset(List(cv))
    val phenos = sc.broadcast(createSamplePhenotype(calledVariant = Option(cv)))

    val assoc = LogisticSiteRegression(cvDS, phenos).collect

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
    val phenos = sc.broadcast(createSamplePhenotype(calledVariant = Option(cv)))

    val assoc = LogisticSiteRegression(cvDS, phenos, "DOMINANT").collect

    // Note: due to lazy compute, the error won't actually
    // materialize till an action is called on the dataset, hence the collect

    assert(assoc.length == 0, "Singular Matrix test broken...(either a singular matrix was not created properly or it was not filtered out.)")
  }

  // LogisticSiteRegression.applyToSite input validation tests

  ignore("LogisticSiteRegression.applyToSite should break when there is not overlap between sampleIDs in phenotypes and CalledVariant objects.") {

  }

  // currently breaks due to an unwanted singular matrix, that filters out the sample.
  ignore("LogisticSiteRegression.applyToSite should not break with missing covariates.") {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    // This should produce a singular hessian matrix as all the rows will be identical
    val gs = createSampleGenotypeStates(maf = 0.35, num = 2)
    val cv = createSampleCalledVariant(samples = Option(gs))

    val phenos = sc.broadcast(createSamplePhenotype(calledVariant = Option(cv), numCovariate = 0))
    val cvDS = sparkSession.createDataset(List(cv))
    val assoc = LogisticSiteRegression(cvDS, phenos).collect

    assert(assoc.length != 0, "LogisticSiteRegression.applyToSite breaks on missing covariates.")
  }

  // currently breaks due to an unwanted singular matrix, that filters out the sample.
  ignore("LogisticSiteRegression.applyToSite should return an AdditiveLogisticVariantModel") {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    val gs = createSampleGenotypeStates(num = 10, maf = 0.35, geno = 0.0, ploidy = 2)
    val cv = createSampleCalledVariant(samples = Option(gs))

    val cvDS = sparkSession.createDataset(List(cv))
    val phenos = sc.broadcast(createSamplePhenotype(calledVariant = Option(cv), numCovariate = 10))

    val assoc = LogisticSiteRegression(cvDS, phenos).collect

    assert(assoc.head.isInstanceOf[LogisticVariantModel], "LogisticSiteRegression.applyToSite does not return a AdditiveLogisticVariantModel")
  }

  // LogisticSiteRegression.prepareDesignMatrix tests

  sparkTest("LogisticSiteRegression.prepareDesignMatrix should filter out missing values and produce a label vector and a design matrix.") {
    // This should produce a singular hessian matrix as all the rows will be identical
    val gs = createSampleGenotypeStates(num = 10, maf = 0.25, geno = 0.1, ploidy = 2)
    val cv = createSampleCalledVariant(samples = Option(gs))
    val phenos = createSamplePhenotype(calledVariant = Option(cv))

    val (data, label) = LogisticSiteRegression.prepareDesignMatrix(phenos, cv, "ADDITIVE")

    assert(data.rows == cv.numValidSamples, "LogisticSiteRegression.prepareDesignMatrix doesn't filter out missing values properly, design matrix.")
    assert(data.isInstanceOf[DenseMatrix[Double]], "LogisticSiteRegression.prepareDesignMatrix doesn't produce a `breeze.linalg.DenseMatrix[Double]`.")
    assert(label.length == cv.numValidSamples, "LogisticSiteRegression.prepareDesignMatrix doesn't filter out missing values properly, labels.")
    assert(label.isInstanceOf[DenseVector[Double]], "LogisticSiteRegression.prepareDesignMatrix doesn't produce a `breeze.linalg.DenseVector[Double]`.")
  }

  sparkTest("LogisticSiteRegression.prepareDesignMatrix should place the genotype value in the second column of the design matrix.") {
    val gs = createSampleGenotypeStates(num = 10, maf = 0.25, geno = 0.1, ploidy = 2)
    val cv = createSampleCalledVariant(samples = Option(gs))
    val phenos = createSamplePhenotype(calledVariant = Option(cv))

    val (data, label) = LogisticSiteRegression.prepareDesignMatrix(phenos, cv, "ADDITIVE")

    val genos = DenseVector(cv.samples.filter(!_.toList.contains(".")).map(_.toDouble): _*)
    assert(data(::, 1) == genos, "LogisticSiteRegression.prepareDesignMatrix places genos in the wrong place")
  }

  sparkTest("LogisticSiteRegression.prepareDesignMatrix should place the covariates in columns 1 through n in the design matrix") {
    val gs = createSampleGenotypeStates(num = 10, maf = 0.25, geno = 0.1, ploidy = 2)
    val cv = createSampleCalledVariant(samples = Option(gs))
    val phenos = createSamplePhenotype(calledVariant = Option(cv), numCovariate = 3)

    val (data, label) = LogisticSiteRegression.prepareDesignMatrix(phenos, cv, "ADDITIVE")

    val covs = data(::, 2 to -1)
    val rows = phenos.filter(x => cv.samples.filter(!_.toList.contains(".")).map(_.sampleID).contains(x._1))
      .map(_._2.covariates)
      .toList
    val otherCovs = DenseMatrix(rows: _*)

    for (i <- 0 until covs.cols) assert(covs(::, i).toArray.toList.sorted == otherCovs(::, i).toArray.toList.sorted, "Covariates are wrong.")
  }

  sparkTest("LogisticSiteRegression.prepareDesignMatrix should produce a `(DenseMatrix[Double], DenseVector[Double])`") {
    val gs = createSampleGenotypeStates(num = 10, maf = 0.25, geno = 0.1, ploidy = 2)
    val cv = createSampleCalledVariant(samples = Option(gs))
    val phenos = createSamplePhenotype(calledVariant = Option(cv), numCovariate = 3)

    val XandY = LogisticSiteRegression.prepareDesignMatrix(phenos, cv, "ADDITIVE")

    assert(XandY.isInstanceOf[(DenseMatrix[Double], DenseVector[Double])], "LogisticSiteRegression.prepareDesignMatrix returned an incorrect type.")
  }

  // LogisticSiteRegression.findBeta tests

  ignore("LogisticSiteRegression.findBeta should break after the correct number of iterations.") {

  }

  sparkTest("LogisticSiteRegression.findBeta should throw a singular matrix exception when hessian is noninvertible.") {
    val gs = createSampleGenotypeStates(num = 10, maf = 0.0, geno = 0.0, ploidy = 2)
    val cv = createSampleCalledVariant(samples = Option(gs))
    val phenos = createSamplePhenotype(calledVariant = Option(cv))

    val (data, label) = LogisticSiteRegression.prepareDesignMatrix(phenos, cv, "ADDITIVE")
    val beta = DenseVector.zeros[Double](data.cols)

    try {
      LogisticSiteRegression.findBeta(data, label, beta)
      fail("LogisticSiteRegression.findBeta does not break on singular hessian.")
    } catch {
      case e: breeze.linalg.MatrixSingularException =>
    }
  }
}
