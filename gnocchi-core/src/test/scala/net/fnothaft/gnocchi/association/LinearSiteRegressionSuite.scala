/**
 * Copyright 2016 Frank Austin Nothaft, Taner Dagdelen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import net.fnothaft.gnocchi.GnocchiFunSuite
import net.fnothaft.gnocchi.models.{ Association, GenotypeState }
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression

class LinearSiteRegressionSuite extends GnocchiFunSuite {

  test("rsquared should be 1 if all the points lie on a line") {

    val observations = new Array[(Double, Array[Double])](10)
    observations(0) = (0, Array[Double](1))
    observations(1) = (0, Array[Double](1))
    observations(2) = (0, Array[Double](1))
    observations(3) = (0, Array[Double](1))
    observations(4) = (1, Array[Double](2))
    observations(5) = (1, Array[Double](2))
    observations(6) = (1, Array[Double](2))
    observations(7) = (2, Array[Double](3))
    observations(8) = (2, Array[Double](3))
    observations(9) = (2, Array[Double](3))
    val locus = null
    val altAllele = null
    val phenotype = null

    //use additiveLinearRegression to regress on Ascombe1
    var regressionResult = AdditiveLinearAssociation.regressSite(observations, locus, altAllele, phenotype)

    //Assert that the rsquared is in the right threshold. 
    assert(regressionResult.statistics("rSquared") == 1.0, "rSquared = " + regressionResult.statistics("rSquared"))
  }

  /* generate Ascombe's quartet for linear regression 
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

    The full description of Ascombe's Quartet can be found here: https://en.wikipedia.org/wiki/Anscombe%27s_quartet
    Each of the members of the quartet should have roughly the same R^2 value, although the shapes of the graph are different shapes.
    Target R^2 values verified values produced here https://rstudio-pubs-static.s3.amazonaws.com/52381_36ec82827e4b476fb968d9143aec7c4f.html.  
    */

  test("The rsquared for Ascombe I should be within .001 of expected results 0.6665") {

    //load Ascombe1 into an observations variable
    val observations = new Array[(Double, Array[Double])](11)
    observations(0) = (10.0, Array[Double](8.04))
    observations(1) = (8.0, Array[Double](6.95))
    observations(2) = (13.0, Array[Double](7.58))
    observations(3) = (9.0, Array[Double](8.81))
    observations(4) = (11.0, Array[Double](8.33))
    observations(5) = (14.0, Array[Double](9.96))
    observations(6) = (6.0, Array[Double](7.24))
    observations(7) = (4.0, Array[Double](4.26))
    observations(8) = (12.0, Array[Double](10.84))
    observations(9) = (7.0, Array[Double](4.82))
    observations(10) = (5.0, Array[Double](5.68))
    val locus = null
    val altAllele = null
    val phenotype = null

    //use additiveLinearRegression to regress on Ascombe1
    var regressionResult = AdditiveLinearAssociation.regressSite(observations, locus, altAllele, phenotype)

    //Assert that the rsquared is in the right threshold. 
    assert(regressionResult.statistics("rSquared").asInstanceOf[Double] <= 0.6670)
    assert(regressionResult.statistics("rSquared").asInstanceOf[Double] >= 0.6660)
  }

  test("The rsquared for Ascombe II should be within .001 of expected results 0.6662") {

    //load AscombeII into an observations variable
    val observations = new Array[(Double, Array[Double])](11)
    observations(0) = (10.0, Array[Double](9.14))
    observations(1) = (8.0, Array[Double](8.14))
    observations(2) = (13.0, Array[Double](8.74))
    observations(3) = (9.0, Array[Double](8.77))
    observations(4) = (11.0, Array[Double](9.26))
    observations(5) = (14.0, Array[Double](8.10))
    observations(6) = (6.0, Array[Double](6.13))
    observations(7) = (4.0, Array[Double](3.10))
    observations(8) = (12.0, Array[Double](9.13))
    observations(9) = (7.0, Array[Double](7.26))
    observations(10) = (5.0, Array[Double](4.74))
    val locus = null
    val altAllele = null
    val phenotype = null

    //use additiveLinearRegression to regress on Ascombe1
    var regressionResult = AdditiveLinearAssociation.regressSite(observations, locus, altAllele, phenotype)

    //Assert that the rsquared is in the right threshold. 
    assert(regressionResult.statistics("rSquared").asInstanceOf[Double] <= 0.6670)
    assert(regressionResult.statistics("rSquared").asInstanceOf[Double] >= 0.6660)
  }

  test("The rsquared for Ascombe III should be within .001 of expected results 0.6663") {

    //load AscombeIII into an observations variable
    val observations = new Array[(Double, Array[Double])](11)
    observations(0) = (10.0, Array[Double](7.46))
    observations(1) = (8.0, Array[Double](6.77))
    observations(2) = (13.0, Array[Double](12.74))
    observations(3) = (9.0, Array[Double](7.11))
    observations(4) = (11.0, Array[Double](7.81))
    observations(5) = (14.0, Array[Double](8.84))
    observations(6) = (6.0, Array[Double](6.08))
    observations(7) = (4.0, Array[Double](5.39))
    observations(8) = (12.0, Array[Double](8.15))
    observations(9) = (7.0, Array[Double](6.42))
    observations(10) = (5.0, Array[Double](5.73))
    val locus = null
    val altAllele = null
    val phenotype = null

    //use additiveLinearRegression to regress on Ascombe1
    var regressionResult = AdditiveLinearAssociation.regressSite(observations, locus, altAllele, phenotype)

    //Assert that the rsquared is in the right threshold. 
    assert(regressionResult.statistics("rSquared").asInstanceOf[Double] <= 0.6670)
    assert(regressionResult.statistics("rSquared").asInstanceOf[Double] >= 0.6660)
  }

  test("The rsquared for Ascombe IV should be within .001 of expected results 0.6667") {

    //load AscombeIV into an observations variable
    val observations = new Array[(Double, Array[Double])](11)
    observations(0) = (8.0, Array[Double](6.58))
    observations(1) = (8.0, Array[Double](5.76))
    observations(2) = (8.0, Array[Double](7.71))
    observations(3) = (8.0, Array[Double](8.84))
    observations(4) = (8.0, Array[Double](8.47))
    observations(5) = (8.0, Array[Double](7.04))
    observations(6) = (8.0, Array[Double](5.25))
    observations(7) = (19.0, Array[Double](12.50))
    observations(8) = (8.0, Array[Double](5.56))
    observations(9) = (8.0, Array[Double](7.91))
    observations(10) = (8.0, Array[Double](6.89))
    val locus = null
    val altAllele = null
    val phenotype = null

    //use additiveLinearRegression to regress on Ascombe1
    var regressionResult = AdditiveLinearAssociation.regressSite(observations, locus, altAllele, phenotype)

    //Assert that the rsquared is in the right threshold. 
    assert(regressionResult.statistics("rSquared").asInstanceOf[Double] <= 0.6670)
    assert(regressionResult.statistics("rSquared").asInstanceOf[Double] >= 0.6660)
  }

  test("Test multiple regression on PIQ data") {
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
    val locus = null
    val altAllele = null
    val phenotype = null

    // use additiveLinearAssociation to regress on PIQ data
    var regressionResult = AdditiveLinearAssociation.regressSite(observations, locus, altAllele, phenotype)

    // check that the rsquared value is correct (correct is 0.2949)
    assert(regressionResult.statistics("rSquared").asInstanceOf[Double] <= 0.2954)
    assert(regressionResult.statistics("rSquared").asInstanceOf[Double] >= 0.2944)

    // check that the p-value for Brain is correct (correct is ~0.001)
    assert(regressionResult.logPValue <= -2.721)
    assert(regressionResult.logPValue >= -4.0)

  }
}
