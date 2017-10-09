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
package net.fnothaft.gnocchi.algorithms.siteregression

import net.fnothaft.gnocchi.models.variant.linear.{ AdditiveLinearVariantModel, DominantLinearVariantModel, LinearVariantModel }
import net.fnothaft.gnocchi.primitives.association.LinearAssociation
import net.fnothaft.gnocchi.primitives.phenotype.Phenotype
import net.fnothaft.gnocchi.primitives.variants.CalledVariant
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.linear.SingularMatrixException
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{ Dataset, SparkSession }

import scala.collection.immutable.Map
import scala.math.log10

trait LinearSiteRegression[VM <: LinearVariantModel[VM]] extends SiteRegression[VM] {

  def apply(genotypes: Dataset[CalledVariant],
            phenotypes: Broadcast[Map[String, Phenotype]],
            validationStringency: String = "STRICT"): Dataset[VM]

  def applyToSite(phenotypes: Map[String, Phenotype],
                  genotypes: CalledVariant): LinearAssociation = {

    val XandY = prepareDesignMatrix(phenotypes, genotypes)
    val x = XandY.map(_._1.toArray).toArray
    val y = XandY.map(_._2).toArray

    val phenotypesLength = phenotypes.head._2.covariates.length + 1

    try {
      // create linear model
      val ols = new OLSMultipleLinearRegression()

      // input sample data
      ols.newSampleData(y, x)

      // calculate coefficients
      val beta = ols.estimateRegressionParameters()

      // calculate sum of squared residuals
      val ssResiduals = ols.calculateResidualSumOfSquares()

      // calculate sum of squared deviations
      val ssDeviations = sumOfSquaredDeviations(genotypes)

      // compute the regression parameters standard errors
      val standardErrors = ols.estimateRegressionParametersStandardErrors()

      // get standard error for genotype parameter (for p value calculation)
      val genoSE = standardErrors(1)

      // test statistic t for jth parameter is equal to bj/SEbj, the parameter estimate divided by its standard error
      val t = beta(1) / genoSE

      /* calculate p-value and report:
        Under null hypothesis (i.e. the j'th element of weight vector is 0) the relevant distribution is
        a t-distribution with N-p-1 degrees of freedom. (N = number of samples, p = number of regressors i.e. genotype+covariates+intercept)
        https://en.wikipedia.org/wiki/T-statistic
      */
      val residualDegreesOfFreedom = genotypes.numValidSamples - phenotypesLength - 1
      val tDist = new TDistribution(residualDegreesOfFreedom)
      val pvalue = 2 * tDist.cumulativeProbability(-math.abs(t))
      val logPValue = log10(pvalue)

      LinearAssociation(
        ssDeviations,
        ssResiduals,
        genoSE,
        t,
        residualDegreesOfFreedom,
        pvalue,
        beta.toList,
        genotypes.numValidSamples)
    } catch {
      case _: breeze.linalg.MatrixSingularException => {
        throw new SingularMatrixException()
      }
    }
  }

  private def prepareDesignMatrix(phenotypes: Map[String, Phenotype],
                                  genotypes: CalledVariant): List[(List[Double], Double)] = {

    // class for ols: org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
    // see http://commons.apache.org/proper/commons-math/javadocs/api-3.6.1/org/apache/commons/math3/stat/regression/OLSMultipleLinearRegression.html
    val samplesGenotypes = genotypes.samples.filter(x => !x.value.contains(".")).map(x => (x.sampleID, List(clipOrKeepState(x.toDouble))))
    val samplesCovariates = phenotypes.map(x => (x._1, x._2.covariates)).toMap
    val cleanedSampleVector = samplesGenotypes.map(x => (x._1, (x._2 ++ samplesCovariates(x._1)).toList)).toMap

    cleanedSampleVector.toList.map(x => (x._2, phenotypes(x._1).phenotype.toDouble))
  }

  protected def sumOfSquaredDeviations(genotypes: CalledVariant): Double = {
    val sum = genotypes.samples.filter(x => !x.value.contains(".")).map(x => clipOrKeepState(x.toDouble)).sum
    val mean = sum / genotypes.numValidSamples
    val squaredDeviations = genotypes.samples.map(x => math.pow(x.toDouble - mean, 2))
    squaredDeviations.sum
  }

  protected def constructVM(variant: CalledVariant,
                            phenotype: Phenotype,
                            association: LinearAssociation): VM
}

object AdditiveLinearRegression extends AdditiveLinearRegression {
  val regressionName = "additiveLinearRegression"
}

trait AdditiveLinearRegression extends LinearSiteRegression[AdditiveLinearVariantModel] with Additive {
  val sparkSession = SparkSession.builder().getOrCreate()
  import sparkSession.implicits._

  def apply(genotypes: Dataset[CalledVariant],
            phenotypes: Broadcast[Map[String, Phenotype]],
            validationStringency: String = "STRICT"): Dataset[AdditiveLinearVariantModel] = {

    //ToDo: Singular Matrix Exceptions
    genotypes.map((genos: CalledVariant) => {
      val association = applyToSite(phenotypes.value, genos)
      constructVM(genos, phenotypes.value.head._2, association)
    })
  }

  protected def constructVM(variant: CalledVariant,
                            phenotype: Phenotype,
                            association: LinearAssociation): AdditiveLinearVariantModel = {
    AdditiveLinearVariantModel(variant.uniqueID,
      association,
      phenotype.phenoName,
      variant.chromosome,
      variant.position,
      variant.referenceAllele,
      variant.alternateAllele,
      phaseSetId = 0)
  }
}

object DominantLinearRegression extends DominantLinearRegression {
  val regressionName = "dominantLinearRegression"
}

trait DominantLinearRegression extends LinearSiteRegression[DominantLinearVariantModel] with Dominant {
  val sparkSession = SparkSession.builder().getOrCreate()
  import sparkSession.implicits._

  def apply(genotypes: Dataset[CalledVariant],
            phenotypes: Broadcast[Map[String, Phenotype]],
            validationStringency: String = "STRICT"): Dataset[DominantLinearVariantModel] = {

    //ToDo: Singular Matrix Exceptions
    genotypes.map((genos: CalledVariant) => {
      val association = applyToSite(phenotypes.value, genos)
      constructVM(genos, phenotypes.value.head._2, association)
    })
  }

  protected def constructVM(variant: CalledVariant,
                            phenotype: Phenotype,
                            association: LinearAssociation): DominantLinearVariantModel = {
    DominantLinearVariantModel(variant.uniqueID,
      association,
      phenotype.phenoName,
      variant.chromosome,
      variant.position,
      variant.referenceAllele,
      variant.alternateAllele,
      phaseSetId = 0)
  }
}
