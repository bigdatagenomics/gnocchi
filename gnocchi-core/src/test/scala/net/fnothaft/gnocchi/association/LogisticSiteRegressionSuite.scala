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
import org.apache.spark.SparkContext
import org.bdgenomics.adam.models.ReferenceRegion

class LogisticSiteRegressionSuite extends GnocchiFunSuite {

  test("Test logisticRegression on binary data") {

    // read in the data from binary.csv
    // data comes from: http://www.ats.ucla.edu/stat/sas/dae/binary.sas7bdat
    val pathToFile = ClassLoader.getSystemClassLoader.getResource("binary.csv").getFile
    println(ClassLoader.getSystemClassLoader.getResource("binary.csv").getFile)
    println(pathToFile.getClass)
    println(sc.getClass)
    val csv = sc.textFile(pathToFile)
    val data = csv.map(line => line.split(",").map(elem => elem.toDouble)) //get rows

    // transform it into the right format
    val observations = data.map(row => {
      val geno = row(0)
      val covars = row.slice(0, row.length - 1)
      val phenos = Array(row(row.length - 1)) ++ covars
      println(geno)
      println(phenos)
      (geno, phenos)
    }).collect()
    val altAllele = "No allele"
    val phenotype = "acceptance"
    val locus = ReferenceRegion("Name", 1, 2)
    val scOption = Option(sc)

    // feed it into logisitic regression and compare the Wald Chi Squared tests
    val regressionResult = AdditiveLogisticAssociation.regressSite(observations, locus, altAllele, phenotype, scOption)

    //Assert that the Wald chi squared value is in the right threshold. Answer should be 0.0385
    assert(regressionResult.statistics("'P Value' aka Wald Chi Squared") == 0.0385, "'P Value' aka Wald Chi Squared = " + regressionResult.statistics("'P Value' aka Wald Chi Squared"))
  }
}
