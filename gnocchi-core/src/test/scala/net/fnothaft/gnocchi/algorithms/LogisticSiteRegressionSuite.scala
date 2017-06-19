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
package net.fnothaft.gnocchi.algorithms

import breeze.linalg.DenseVector
import net.fnothaft.gnocchi.GnocchiFunSuite
import net.fnothaft.gnocchi.algorithms.siteregression.AdditiveLogisticRegression
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{ Contig, Variant }

class LogisticSiteRegressionSuite extends GnocchiFunSuite {

  sparkTest("Test logisticRegression on binary data") {
    // read in the data from binary.csv
    // data comes from: http://www.ats.ucla.edu/stat/sas/dae/binary.sas7bdat
    // results can be found here: http://www.ats.ucla.edu/stat/sas/dae/logit.htm
    val pathToFile = ClassLoader.getSystemClassLoader.getResource("binary.csv").getFile
    val csv = sc.textFile(pathToFile)
    val data = csv.map(line => line.split(",").map(elem => elem.toDouble)) //get rows

    // transform it into the right format
    val observations = data.map(row => {
      val geno: Double = row(0)
      val covars: Array[Double] = row.slice(1, 3)
      val phenos: Array[Double] = Array(row(3)) ++ covars
      (geno, phenos)
    }).collect()
    val altAllele = "No allele"
    val phenotype = "acceptance"
    val locus = ReferenceRegion("Name", 1, 2)
    val scOption = Option(sc)
    val variant = new Variant
    //    val contig = new Contig()
    //    contig.setContigName(locus.referenceName)
    variant.setContigName(locus.referenceName)
    variant.setStart(locus.start)
    variant.setEnd(locus.end)
    variant.setAlternateAllele(altAllele)
    val phaseSetId = 0

    // feed it into logisitic regression and compare the Wald Chi Squared tests
    val regressionResult = AdditiveLogisticRegression.applyToSite(observations, variant, phenotype, phaseSetId)

    // Assert that the weights are correct within a threshold.
    val estWeights: Array[Double] = regressionResult.statistics("weights").asInstanceOf[Array[Double]] :+ regressionResult.statistics("intercept").asInstanceOf[Double]
    val compWeights = Array(-3.4495484, .0022939, .77701357, -0.5600314)
    for (i <- 0 until 3) {
      assert(estWeights(i) <= (compWeights(i) + 1), s"Weight $i incorrect")
      assert(estWeights(i) >= (compWeights(i) - 1), s"Weight $i incorrect")
    }
    //Assert that the Wald chi squared value is in the right threshold. Answer should be 0.0385
    val pval: Array[Double] = regressionResult.statistics("'P Values' aka Wald Tests").asInstanceOf[DenseVector[Double]].toArray
    assert(pval(1) <= 0.0385 + 0.01, "'P Values' aka Wald Tests = " + pval)
    assert(pval(1) >= 0.0385 - 0.01, "'P Values' aka Wald Tests = " + pval)
  }
}
