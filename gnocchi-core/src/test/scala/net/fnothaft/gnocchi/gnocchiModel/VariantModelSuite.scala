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
package net.fnothaft.gnocchi.gnocchiModel

import breeze.linalg.DenseMatrix
import net.fnothaft.gnocchi.GnocchiFunSuite
import net.fnothaft.gnocchi.models.{ AdditiveLinearVariantModel, AdditiveLogisticVariantModel }
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{ Contig, Variant }

class VariantModelSuite extends GnocchiFunSuite {

  // Load data from binary.csv
  val pathToFile = ClassLoader.getSystemClassLoader.getResource("IncrementalBinary.csv").getFile
  val csv = sc.textFile(pathToFile)
  val data = csv.map(line => line.split(",").map(elem => elem.toDouble))

  /* transforms it into the right format
  *  0th column is Additive genotype.
  *  1st and 2nd columns are covariates
  *  3rd column is linear, ordinal phenotype
  *  4th column is binary phenotype
  */
  val linearObservations = data.map(row => {
    val geno: Double = row(0)
    val covars: Array[Double] = row.slice(1, 3)
    val phenos: Array[Double] = Array(row(3)) ++ covars
    (geno, phenos)
  }).collect
  val altAllele = "No allele"
  val phenotype = "acceptance"
  val locus = ReferenceRegion("Name", 1, 2)
  val scOption = Option(sc)
  val variant = new Variant()
  val contig = new Contig()
  contig.setContigName(locus.referenceName)
  variant.setContig(contig)
  variant.setStart(locus.start)
  variant.setEnd(locus.end)
  variant.setAlternateAllele(altAllele)

  // break observations into initial group and update group
  val (linearInitialGroup, linearUpdateGroup) = linearObservations.slice(0, linearObservations.length / 2)

  // build a VariantModel using initial data and update with update data
  val incrementalLinearVariantModel: AdditiveLinearVariantModel = BuildAdditiveLinearVariantModel(linearInitialGroup, variant, phenotype)
  incrementalLinearVariantModel.update(linearUpdateGroup, locus, altAllele, phenotype)

  ignore("Check Linear Regression Model weights") {
    assert(incrementalLinearVariantModel.weights sameElements Array(0.753, 1.498, -0.000438, 0.1382))
  }

  ignore("Check tracked number of samples in linear model") {
    assert(incrementalLinearVariantModel.numSamples == 400)
  }

  ignore("Check updated residual for SE calculation") {
    assert(incrementalLinearVariantModel.ssResiduals == 32.51447333)
  }

  ignore("Check updated sum of squared deviations from mean for SE calculation") {
    assert(incrementalLinearVariantModel.ssDeviations == 127.27)
  }

  ignore("Check updated standard error of genetic parameter") {
    assert(incrementalLinearVariantModel.geneticParameterStandardError == 0.035875262)
  }

  ignore("Check updated degrees of freedom for linear model") {
    assert(incrementalLinearVariantModel.residualDegreesOfFreedom == 396)
  }

  ignore("Check updated t-Statistic for genotype component in linear model") {
    assert(incrementalLinearVariantModel.tStatistic == 41.82036378)
  }

  ignore("Check updated p-value for the genotype component in linear model") {
    assert(incrementalLinearVariantModel.pValue == 2.3454E-147)
  }

  /*
   * transforms it into the right format
   *  0th column is Additive genotype.
   *  1st and 2nd columns are covariates
   *  3rd column is linear, ordinal phenotype
   *  4th column is binary phenotype
   */
  val logisticObservations = data.map(row => {
    val geno: Double = row(0)
    val covars: Array[Double] = row.slice(1, 3)
    val phenos: Array[Double] = Array(row(3)) ++ covars
    (geno, phenos)
  }).collect

  // break observations into initial group and update group
  val (logisticInitialGroup, logisticUpdateGroup) = logisticObservations.slice(0, logisticObservations.length / 2)

  // Build a logistic model with initial group
  val incrementalLogisticVariantModel: AdditiveLogisticVariantModel = BuildAdditiveLogisticVariantModel(logisticInitialGroup, variant, phenotype)

  // Update logistic model with update group
  incrementalLogisticVariantModel.update(logisticUpdateGroup, locus, altAllele, phenotype)

  ignore("Check Logistic Regression Model weights") {
    assert(incrementalLogisticVariantModel.weights sameElements Array(-48.24384529, -8.901532761, 0.024694189, 9.863257964))
  }

  ignore("Check tracked number of samples in logistic model") {
    assert(incrementalLogisticVariantModel.numSamples == 400)
  }

  /*
   * Averaging standard errors becuase unlink in linear regression,
   * there is no obvious way to combine intermediate values
   */
  ignore("Check updated standard error in logistic model") {
    assert(incrementalLogisticVariantModel.geneticParameterStandardError == 2.398984896)
  }

  ignore("Check updated p-value in logistic model") {
    assert(incrementalLogisticVariantModel.pValue == 0.000206816)
  }
}

