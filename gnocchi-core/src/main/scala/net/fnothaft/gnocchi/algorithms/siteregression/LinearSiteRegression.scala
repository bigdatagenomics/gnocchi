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

import net.fnothaft.gnocchi.models.{ GnocchiModel, GnocchiModelMetaData }
import net.fnothaft.gnocchi.models.linear.{ AdditiveLinearGnocchiModel, DominantLinearGnocchiModel }
import net.fnothaft.gnocchi.models.variant.{ QualityControlVariantModel, VariantModel }
import net.fnothaft.gnocchi.models.variant.linear.{ AdditiveLinearVariantModel, DominantLinearVariantModel, LinearVariantModel }
//import net.fnothaft.gnocchi.models.variant.linear.{ AdditiveLinearVariantModel, DominantLinearVariantModel }
import net.fnothaft.gnocchi.primitives.association.LinearAssociation
import net.fnothaft.gnocchi.primitives.phenotype.Phenotype
import net.fnothaft.gnocchi.primitives.variants.CalledVariant
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.linear.SingularMatrixException
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.bdgenomics.formats.avro.Variant

import scala.collection.immutable.Map
import scala.math.log10

trait LinearSiteRegression[VM <: LinearVariantModel[VM]] extends SiteRegression[VM] {

  def apply(genotypes: Dataset[CalledVariant],
            phenotypes: Broadcast[Map[String, Phenotype]],
            validationStringency: String = "STRICT"): Dataset[VM]

  def applyToSite(phenotypes: Map[String, Phenotype],
                  genotypes: CalledVariant): LinearAssociation = {
    // class for ols: org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
    // see http://commons.apache.org/proper/commons-math/javadocs/api-3.6.1/org/apache/commons/math3/stat/regression/OLSMultipleLinearRegression.html

    val samplesGenotypes = genotypes.samples.map(x => (x.sampleID, List(x.toDouble)))
    val samplesCovariates = phenotypes.map(x => (x._1, x._2.covariates))
    val mergedSampleVector = samplesGenotypes ++ samplesCovariates
    val groupedSampleVector = mergedSampleVector.groupBy(_._1)
    val cleanedSampleVector = groupedSampleVector.mapValues(_.map(_._2).toList.flatten)

    // transform the data in to design matrix and y matrix compatible with OLSMultipleLinearRegression
    val phenotypesLength = phenotypes.head._2.covariates.length
    val numObservations = genotypes.samples.length
    val x = cleanedSampleVector.map(x => x._2.toArray).toArray
    val y = cleanedSampleVector.map(x => phenotypes(x._1).phenotype.toDouble).toArray
    val sum = genotypes.samples.map(x => x.toDouble).reduce(_ + _)

    val mean = sum / numObservations.toDouble

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
      val ssDeviations = sumOfSquaredDeviations(genotypes, mean)

      // calculate Rsquared
      val rSquared = ols.calculateRSquared()

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
      val residualDegreesOfFreedom = numObservations - phenotypesLength
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
        numObservations)
    } catch {
      case _: breeze.linalg.MatrixSingularException => {
        throw new SingularMatrixException()
      }
    }
  }

  protected def sumOfSquaredDeviations(genotypes: CalledVariant, mean: Double): Double = {
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
