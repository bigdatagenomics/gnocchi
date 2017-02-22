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
package net.fnothaft.gnocchi.association

import net.fnothaft.gnocchi.models.Association
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.apache.commons.math3.linear.SingularMatrixException
import scala.math.log10
import org.apache.commons.math3.distribution.TDistribution
import org.bdgenomics.formats.avro.Variant

trait LinearSiteRegression extends SiteRegression {

  /**
   * Returns Association object with solution to linear regression.
   *
   * Implementation of RegressSite method from SiteRegression trait. Performs linear regression on a single site.
   * A site in this context is the unique pairing of a [[org.bdgenomics.formats.avro.Variant]] object and a
   * [[net.fnothaft.gnocchi.models.Phenotype]] name. [[org.bdgenomics.formats.avro.Variant]] objects in this context
   * have contigs defined as CHROM_POS_ALT, which uniquely identify a single base.
   *
   * For calculation of the p-value this uses a t-distribution with N-p-1 degrees of freedom. (N = number of samples,
   * p = number of regressors i.e. genotype+covariates+intercept).
   *
   * @param observations Array of tuples. The first element is a coded genotype taken from
   *                     [[net.fnothaft.gnocchi.models.GenotypeState]]. The second is an array of phenotype values
   *                     taken from [[net.fnothaft.gnocchi.models.Phenotype]] objects. All genotypes are of the same
   *                     site and therefore reference the same contig value i.e. all have the same CHROM_POS_ALT
   *                     identifier. Array of phenotypes has primary phenotype first then covariates.
   * @param variant [[org.bdgenomics.formats.avro.Variant]] being regressed
   * @param phenotype [[net.fnothaft.gnocchi.models.Phenotype.phenotype]], The name of the phenotype being regressed.
   * @return [[net.fnothaft.gnocchi.models.Association]] object containing statistic result for Logistic Regression.
   */
  def regressSite(observations: Array[(Double, Array[Double])], variant: Variant, phenotype: String): Association = {
    val phenotypesLength = observations(0)._2.length
    val numObservations = observations.length
    val x = new Array[Array[Double]](numObservations)
    val y = new Array[Double](numObservations)

    var sample = new Array[Double](phenotypesLength)
    for (i <- 0 until numObservations) {
      sample = new Array[Double](phenotypesLength)
      sample(0) = observations(i)._1.toDouble
      observations(i)._2.slice(1, phenotypesLength).copyToArray(sample, 1)
      x(i) = sample
      y(i) = observations(i)._2(0)
    }

    var associationObject = new Association(null, null, -9.0, null)
    var matrixSingular = false
    try {
      // create linear model
      val ols = new OLSMultipleLinearRegression()

      // input sample data
      ols.newSampleData(y, x)

      // calculate coefficients
      val beta = ols.estimateRegressionParameters()

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
        a t-distribution with N-p-1 degrees of freedom.
      */
      val tDist = new TDistribution(numObservations - observationLength - 1)
      val pvalue = 1.0 - tDist.cumulativeProbability(t)
      val logPValue = log10(pvalue)

      // pack up the information into an Association object
      //    val variant = new Variant()
      //    val contig = new Contig()
      //    contig.setContigName(locus.referenceName)
      //    variant.setContig(contig)
      //    variant.setStart(locus.start)
      //    variant.setEnd(locus.end)
      //    variant.setAlternateAllele(altAllele)
      val statistics = Map("rSquared" -> rSquared,
        "weights" -> beta,
        "intercept" -> beta(0))
      associationObject = new Association(variant, phenotype, logPValue, statistics)
    }
    catch {
      case error: breeze.linalg.MatrixSingularException => matrixSingular = true
    }
    if (matrixSingular) {
      val statistics = Map()
      associationObject = Association(variant, phenotype, 0.0, Map())
      println("Caught a singular matrix error!")
    }

    return associationObject
  }
}

object AdditiveLinearAssociation extends LinearSiteRegression with Additive {
  val regressionName = "additiveLinearRegression"
}

object DominantLinearAssociation extends LinearSiteRegression with Dominant {
  val regressionName = "dominantLinearRegression"
}
