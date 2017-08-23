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
package net.fnothaft.gnocchi.models

import net.fnothaft.gnocchi.GnocchiFunSuite
import net.fnothaft.gnocchi.algorithms.siteregression.{ AdditiveLinearRegression, AdditiveLogisticRegression }
import net.fnothaft.gnocchi.models.variant.linear.AdditiveLinearVariantModel
import net.fnothaft.gnocchi.models.variant.logistic.AdditiveLogisticVariantModel
import org.apache.commons.math3.distribution.{ ChiSquaredDistribution, TDistribution }
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{ Contig, Variant }

import scala.io.Source

class VariantModelSuite extends GnocchiFunSuite {

  val altAllele = "No allele"
  val phenotype = "acceptance"
  val locus = ReferenceRegion("Name", 1, 2)
  val scOption = Option(sc)
  val variant = new Variant()
  //  val contig = new Contig()
  //  contig.setContigName(locus.referenceName)
  variant.setContigName(locus.referenceName)
  variant.setStart(locus.start)
  variant.setEnd(locus.end)
  variant.setAlternateAllele(altAllele)
  val variantModel = AdditiveLinearVariantModel(variant.getContigName,
    22.0, //ssDeviations
    12.0, //ssResiduals
    .001, //geneticParameterStandardError
    0.5, //tStatistic
    10, //residualDegreesOfFreedom
    0.0001, //pValue
    variant,
    List(1.0, 2.0, 3.0), //weights
    8,
    "phenotype") //numSamples

  test("updateWeights should take weighted average of old weights and batch weights") {
    val batchWeights = List(2.0, 3.0, 4.0)
    val batchNumSamples = 20
    val updatedWeights = variantModel.updateWeights(batchWeights, batchNumSamples)
    val comparisonWeight0 = (batchWeights(0) * batchNumSamples + variantModel.weights(0) * variantModel.numSamples) / (batchNumSamples + variantModel.numSamples)
    val comparisonWeight1 = (batchWeights(1) * batchNumSamples + variantModel.weights(1) * variantModel.numSamples) / (batchNumSamples + variantModel.numSamples)
    val comparisonWeight2 = (batchWeights(2) * batchNumSamples + variantModel.weights(2) * variantModel.numSamples) / (batchNumSamples + variantModel.numSamples)
    assert(updatedWeights == List(comparisonWeight0, comparisonWeight1, comparisonWeight2),
      "Updated weights did not match expected.")
  }

  test("updateNumSamples should add number of samples in batch to total number of samples") {
    val batchNumSamples = 10
    val updatedNumSamples = variantModel.updateNumSamples(batchNumSamples)
    val comparisonNumSamples = batchNumSamples + variantModel.numSamples
    assert(updatedNumSamples === comparisonNumSamples, "Updated numSamples did not match expected ")
  }

  test("updateSsDeviations should add batch sum of squared deviations to model total") {
    val batchSsDeviations = 22.0
    val updatedSsDeviations = variantModel.updateSsDeviations(batchSsDeviations)
    val comparisonSsDeviations = batchSsDeviations + variantModel.ssDeviations
    assert(updatedSsDeviations === comparisonSsDeviations, "Updated sum of squared deviations did not match expected")
  }

  test("updateSsResiduals should add batch sum of squared deviations to model total") {
    val batchSsResiduals = 40.1
    val updatedSsResiduals = variantModel.updateSsResiduals(batchSsResiduals)
    val comparisonSsResiduals = batchSsResiduals + variantModel.ssResiduals
    assert(updatedSsResiduals === comparisonSsResiduals, "Updated sum of squared residuals did not match expected.")
  }

  test("computeGeneticParameterStandardError should take sqrt((1/(n-p)sum(eps^2)/sum(dev^2)) where eps is the residual and dev is the deviation") {
    val updatedSsResiduals = 23.2
    val updatedSsDeviations = 22.1
    val updatedNumSamples = 30
    val comparisonGeneticParameterStandardError = math.sqrt(((1.0 / (updatedNumSamples - variantModel.weights.length))
      * updatedSsResiduals) / updatedSsDeviations)
    val updatedGeneticParameterStandardError = variantModel.computeGeneticParameterStandardError(updatedSsResiduals,
      updatedSsDeviations,
      updatedNumSamples)
    assert(updatedGeneticParameterStandardError === comparisonGeneticParameterStandardError,
      "Updated genetic parameter standard error did not match expected.")
  }

  test("updateResidualDegreesOfFreedom should add batch residualDegreesOfFreedom") {
    val batchNumSamples = 20
    val comparisonResidualDegreesOfFreedom = variantModel.residualDegreesOfFreedom + batchNumSamples
    val updatedResidualDegreesOfFreedom = variantModel.updateResidualDegreesOfFreedom(batchNumSamples)
    assert(updatedResidualDegreesOfFreedom === comparisonResidualDegreesOfFreedom,
      "Updated residual degrees of freedom did not match expected.")
  }

  test("calculateTStatistic should take averate of genetic parameter weight to genetic parameter standard error") {
    val weights = variantModel.weights
    val geneticParameterStandardError = variantModel.geneticParameterStandardError
    val comparisonTStatistic = weights(1) / geneticParameterStandardError
    val tStatistic = variantModel.calculateTStatistic(weights, geneticParameterStandardError)
    assert(tStatistic === comparisonTStatistic, "Calculated t statistic did not match expected.")
  }

  test("calculatePValue should run the tStatistic through a t-distribution with the specificed degrees of freedom") {
    val tStatistic = 2
    val residualDegreesOfFreedom = 100
    val tDist = new TDistribution(residualDegreesOfFreedom)
    val comparisonPValue = 2 * tDist.cumulativeProbability(-math.abs(tStatistic))
    val updatedPValue = variantModel.calculatePValue(tStatistic, residualDegreesOfFreedom)
    assert(comparisonPValue === updatedPValue, "Calculated P value did not match expected.")
  }

  test("constructVariantModel should copy relevant fields from original variantModel and update the rest.") {
    val variantID = variantModel.variantId
    val updatedWeights = List(1.0, 2.0, 3.0)
    val updatedNumSamples = 5
    val updatedSsDeviations = 22.0
    val updatedSsResiduals = 13.1
    val updatedGeneticParameterStandardError = 2
    val updatedResidualDegreesOfFreedom = 8
    val updatedTStatistic = 2.0
    val updatedPValue = 0.05
    val updatedVariantModel = variantModel.constructVariantModel(variantID, updatedSsDeviations,
      updatedSsResiduals, updatedGeneticParameterStandardError, updatedTStatistic, updatedResidualDegreesOfFreedom,
      updatedPValue, variantModel.variant, updatedWeights, updatedNumSamples)
    val comparisonVariantModel = AdditiveLinearVariantModel(variantID, updatedSsDeviations, updatedSsResiduals,
      updatedGeneticParameterStandardError, updatedTStatistic, updatedResidualDegreesOfFreedom, updatedPValue,
      variantModel.variant, updatedWeights, updatedNumSamples, variantModel.phenotype)
    assert(updatedVariantModel === comparisonVariantModel, "Constructed variant model did not match expected.")
  }

  // Load data from binary.csv
  val pathToFile = ClassLoader.getSystemClassLoader.getResource("IncrementalBinary.csv").getFile
  val csv = Source.fromFile(pathToFile)
  val data = csv.getLines.toList.map(line => line.split(",").map(elem => elem.toDouble))
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
  })

  // break observations into initial group and update group
  val linearInitialGroup = linearObservations.slice(0, linearObservations.length / 2).toArray
  val linearUpdateGroup = linearObservations.slice(linearObservations.length / 2, linearObservations.length).toArray
  val phaseSetId = 0

  // build a VariantModel using initial data and update with update data
  val incrementalLinearVariantModel: AdditiveLinearVariantModel = AdditiveLinearRegression.applyToSite(linearInitialGroup, variant, phenotype, phaseSetId)
    .toVariantModel
  println(incrementalLinearVariantModel.weights.toList)
  val updatedLinearModel = incrementalLinearVariantModel.update(linearUpdateGroup)

  test("weights in updated linear model should be consistent with batch-merged excel results") {
    // the weights that result from running the linear regression in excel is:
    // Array(0.752994942, 1.497900361, -0.000437889, 0.138199142)
    val intercept = 0.752994942
    val b1 = 1.497900361
    val b2 = -0.000437889
    val b3 = 0.138199142
    val w = updatedLinearModel.weights
    assert(w(0) <= intercept + 0.0000000005)
    assert(w(0) >= intercept - 0.0000000005)
    assert(w(1) <= b1 + 0.0000000005)
    assert(w(1) >= b1 - 0.0000000005)
    assert(w(2) <= b2 + 0.0000000005)
    assert(w(2) >= b2 - 0.0000000005)
    assert(w(3) <= b3 + 0.0000000005)
    assert(w(3) >= b3 - 0.0000000005)
  }

  test("number of samples in updated linear model should be same as total number of observations") {
    assert(updatedLinearModel.numSamples == linearObservations.length)
  }

  test("residual for SE calculation in updated model should be sum of ssResiduals from original and updated model") {
    val ssResidualsFromOriginalModel = incrementalLinearVariantModel.ssResiduals
    // sum of squared residuals from the update batch comes from excel run.
    val ssResidualsFromBatch = 34.07839038
    val sumOfResiduals = ssResidualsFromOriginalModel + ssResidualsFromBatch
    assert(updatedLinearModel.ssResiduals <= sumOfResiduals + 0.000000005)
    assert(updatedLinearModel.ssResiduals >= sumOfResiduals - 0.000000005)
  }

  test("sum of squared deviations from mean for SE calculation should be sum of ssDeviations from original and updated model") {
    val ssDeviationsFromOriginalModel = incrementalLinearVariantModel.ssDeviations
    // sum of squared deviations from the update batch comes from excel run.
    val ssDeviationsFromBatch = 60.875
    val sumOfDeviations = ssDeviationsFromOriginalModel + ssDeviationsFromBatch
    assert(updatedLinearModel.ssDeviations <= sumOfDeviations + .0005)
    assert(updatedLinearModel.ssDeviations >= sumOfDeviations - .0005)
  }

  test("standard error of genetic parameter in updated model should be the same as output of computeGeneticParameterStandardError") {
    val updatedGeneticParameterStandardError = updatedLinearModel.computeGeneticParameterStandardError(updatedLinearModel.ssResiduals, updatedLinearModel.ssDeviations, updatedLinearModel.numSamples)
    assert(updatedLinearModel.geneticParameterStandardError === updatedGeneticParameterStandardError)
    // test that it matches excel results
    assert(updatedLinearModel.geneticParameterStandardError <= 0.035875262 + .0001)
    assert(updatedLinearModel.geneticParameterStandardError >= 0.035875262 - .0001)
  }

  test("degrees of freedom for updated linear model should be same as output of updateResidualDegreesOfFreedom") {
    val degOfFreedom = incrementalLinearVariantModel.updateResidualDegreesOfFreedom(incrementalLinearVariantModel.numSamples)
    assert(updatedLinearModel.residualDegreesOfFreedom == degOfFreedom)
    assert(updatedLinearModel.residualDegreesOfFreedom == 396)
  }

  test("t-Statistic for genotype component in updated linear model should be same as output of computeTStatistic using updated parameters") {
    val tStat = updatedLinearModel.calculateTStatistic(updatedLinearModel.weights, updatedLinearModel.geneticParameterStandardError)
    assert(updatedLinearModel.tStatistic === tStat)
    // test that it matches excel results
    assert(updatedLinearModel.tStatistic >= 41.82036378 - 0.15)
    assert(updatedLinearModel.tStatistic <= 41.82036378 + 0.15)
  }

  test("p-value for the genotype component in updated linear model should match result of caclulatePValue and excel results") {
    val pVal = updatedLinearModel.calculatePValue(updatedLinearModel.tStatistic, updatedLinearModel.residualDegreesOfFreedom)
    assert(updatedLinearModel.pValue === pVal)
    // test that it matches excel results
    assert(updatedLinearModel.pValue <= 2.3454E-147 + 1E-145)
    assert(updatedLinearModel.pValue >= 2.3454E-147 - 1E-145)
  }

  /*
   * transforms data in incrementalBinary.csv into the right format
   *  0th column is Additive genotype.
   *  1st and 2nd columns are covariates
   *  3rd column is linear, ordinal phenotype
   *  4th column is binary phenotype
   */
  val logisticObservations = data.map(row => {
    val geno: Double = row(0)
    val covars: Array[Double] = row.slice(1, 3)
    val phenos: Array[Double] = Array(row(4)) ++ covars
    (geno, phenos)
  })

  // break observations into initial group and update group
  val logisticInitialGroup = logisticObservations.slice(0, logisticObservations.length / 2).toArray
  val logisticUpdateGroup = logisticObservations.slice(logisticObservations.length / 2, logisticObservations.length).toArray

  // Build a logistic model with initial group
  val incrementalLogisticVariantModel: AdditiveLogisticVariantModel = AdditiveLogisticRegression.applyToSite(logisticInitialGroup, variant, phenotype, phaseSetId)
    .toVariantModel

  // Update logistic model with update group
  val updatedLogisticModel = incrementalLogisticVariantModel.update(logisticUpdateGroup)

  test("weights in updated logistic model should match batch-merged excel results") {
    // excel results are:
    val intercept = -48.24384529
    val b1 = -8.901532761
    val b2 = 0.024694189
    val b3 = 9.863257964
    val w = updatedLogisticModel.weights
    assert(w(0) <= intercept + 0.000000005)
    assert(w(0) >= intercept - 0.000000005)
    assert(w(1) <= b1 + 0.0000000005)
    assert(w(1) >= b1 - 0.0000000005)
    assert(w(2) <= b2 + 0.0000000005)
    assert(w(2) >= b2 - 0.0000000005)
    assert(w(3) <= b3 + 0.0000000005)
    assert(w(3) >= b3 - 0.0000000005)
  }

  val batchStandardError = 3.145755632
  val batchNumSamples = 200

  /*
   * Averaging standard errors because unlike in linear regression,
   * there is no obvious way to combine intermediate values
   */
  test("computeGeneticParameterStandardError should take the weighted average of old standard error " +
    "and batch standard error and should match excel results") {
    val oldStandardError = incrementalLogisticVariantModel.geneticParameterStandardError
    val oldNumSamples = incrementalLogisticVariantModel.numSamples
    val averagedError = (batchStandardError * batchNumSamples.toDouble + oldStandardError * oldNumSamples.toDouble) / (batchNumSamples.toDouble + oldNumSamples.toDouble)
    val res = incrementalLogisticVariantModel.computeGeneticParameterStandardError(batchStandardError, batchNumSamples)
    assert(res === averagedError)

    // check against excel results
    assert(res <= 2.398984896 + 0.00000001)
    assert(res >= 2.398984896 - 0.00000001)
  }

  test("calculateWaldStatistic should take square of the ratio of genetic parameter and genetic parameter standard error") {
    val genParamStandardError = 2.398984896
    val weights = List(0.0, -8.901532761, 2.0, 3.0)
    val comparisonSE = math.pow(weights(1) / genParamStandardError, 2)
    assert(updatedLogisticModel.calculateWaldStatistic(genParamStandardError, weights) === comparisonSE)

    // check against excel results
    val excelWaldStat = 13.76811744
    assert(updatedLogisticModel.calculateWaldStatistic(genParamStandardError, weights) <= excelWaldStat + 0.00000005)
    assert(updatedLogisticModel.calculateWaldStatistic(genParamStandardError, weights) >= excelWaldStat - 0.00000005)
  }

  // excel values from averaged logistic models
  val waldStatistic = 13.768117439096898
  val pValue = 0.000206816

  test("calculatePValue in logistic model should run waldStatistic through a chi-distribution with 1 degree of freedom and be consistent with excel") {

    // manually generated values
    val chiDist = new ChiSquaredDistribution(1)
    val comparisonPValue = 1.0 - chiDist.cumulativeProbability(waldStatistic)

    // check against manually generated values
    assert(updatedLogisticModel.calculatePvalue(waldStatistic) === comparisonPValue)

    // check against excel
    assert(updatedLogisticModel.calculatePvalue(waldStatistic) <= pValue + 0.0000000005)
    assert(updatedLogisticModel.calculatePvalue(waldStatistic) >= pValue - 0.0000000005)
  }

  test("number of samples in updated logistic model should match results of updateNumSamples ") {
    val numSamples = incrementalLogisticVariantModel.updateNumSamples(logisticUpdateGroup.length)
    assert(updatedLogisticModel.numSamples === numSamples)
  }

  test("geneticParameterStandardError in updated logistic model should match restuls of calculateWaldStatistic") {
    assert(updatedLogisticModel.geneticParameterStandardError <= incrementalLogisticVariantModel.computeGeneticParameterStandardError(batchStandardError, batchNumSamples) + 0.00000005)
    assert(updatedLogisticModel.geneticParameterStandardError >= incrementalLogisticVariantModel.computeGeneticParameterStandardError(batchStandardError, batchNumSamples) - 0.00000005)
  }

  test("pValue in updated logistic model should match restuls of calculatePValue") {
    assert(updatedLogisticModel.pValue <= incrementalLogisticVariantModel.calculatePvalue(waldStatistic) + 0.00000000005)
    assert(updatedLogisticModel.pValue >= incrementalLogisticVariantModel.calculatePvalue(waldStatistic) - 0.00000000005)
  }

}

