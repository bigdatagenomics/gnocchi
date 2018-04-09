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
package org.bdgenomics.gnocchi.cli

import org.apache.spark.SparkContext
import org.bdgenomics.gnocchi.primitives.association.LinearAssociation
import org.bdgenomics.gnocchi.utils.GnocchiFunSuite
import org.bdgenomics.utils.cli.Args4j
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.bdgenomics.gnocchi.sql.GnocchiSession._
import org.mockito.{ ArgumentCaptor, Mockito }
import org.apache.spark.sql.Dataset

class RegressPhenotypesSuite extends GnocchiFunSuite {

  /**
   * plink command:
   * plink --vcf resources/RegressionIntegrationTestData_genotypes.vcf --make-bed --out regressionIntegrationTestData
   * plink --bfile regressionIntegrationTestData --pheno resources/LinearRegressionIntegrationTestData_phenotypes.txt --pheno-name pheno_1 --linear --adjust --out test --allow-no-sex --mind 0.1 --maf 0.1 --geno 0.1
   *
   * If possible it would be preferable to move this over to a mockito ArgumentCaptor test on the
   * saveAssociations call in the gnocchiContext.
   *
   *    val associationArgCaptor: ArgumentCaptor[Dataset[LinearAssociation]] = ArgumentCaptor.forClass(classOf[Dataset[LinearAssociation]])
   *    val outputArgCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
   *    val forceSaveArgCaptor: ArgumentCaptor[Boolean] = ArgumentCaptor.forClass(classOf[Boolean])
   *    val saveAsTextArgCaptor: ArgumentCaptor[Boolean] = ArgumentCaptor.forClass(classOf[Boolean])
   *
   *    val spySC = Mockito.spy(sc)
   *    when(spySC.saveAssociations(any(), anyString(), anyBoolean(), anyBoolean())).then(_)
   *
   *    Mockito.verify(spySC).saveAssociations(associationArgCaptor.capture(),
   *      outputArgCaptor.capture(),
   *      forceSaveArgCaptor.capture(),
   *      saveAsTextArgCaptor.capture())
   */
  sparkTest("RegressPhenotypes results should match plink output: Linear Additive.") {
    val sparkSession = sc.sparkSession
    import sparkSession.implicits._

    val plinkResults = Map(("rs7717145", 0.06202),
      ("rs3144742", 0.1041),
      ("rs5133591", 0.1266),
      ("rs891382", 0.1268),
      ("rs3042096", 0.167),
      ("rs9715474", 0.1868),
      ("rs8330247", 0.262),
      ("rs8717426", 0.2819),
      ("rs1567086", 0.2835),
      ("rs9101921", 0.3085),
      ("rs5373017", 0.311),
      ("rs8868170", 0.3568),
      ("rs8068892", 0.3895),
      ("rs4795741", 0.4238),
      ("rs8451144", 0.5036),
      ("rs6071519", 0.5693),
      ("rs3257620", 0.5752),
      ("rs919086", 0.6313),
      ("rs5481294", 0.6436),
      ("rs2075136", 0.6532),
      ("rs9221689", 0.7147),
      ("rs205038", 0.7415),
      ("rs5395585", 0.7874),
      ("rs3685511", 0.8204),
      ("rs7349598", 0.9693),
      ("rs4046230", 0.9746),
      ("rs3646124", 0.9823),
      ("rs707848", 0.9944),
      ("rs9175124", 0.9996))

    val genotypes = testFile("RegressionIntegrationTestData_genotypes.vcf")
    val phenotypes = testFile("LinearRegressionIntegrationTestData_phenotypes.txt")

    val resultsPath = tmpFile("out/")

    val gnocchiCommand =
      genotypes + " " +
        phenotypes + " " +
        "ADDITIVE_LINEAR " +
        resultsPath + " " +
        "-mind 0.1 " +
        "-maf 0.1 " +
        "-geno 0.1 " +
        "-phenoName pheno_1 " +
        "-sampleIDName IID " +
        "-saveAsText"

    RegressPhenotypes(gnocchiCommand.split(" ")).run(sc)

    val gnocchiResults = sparkSession.read.format("csv").option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .load(resultsPath)
      .select($"uniqueID", $"pValue")
      .as[(String, Double)]
      .collect()
      .toMap

    assert((plinkResults.keys.toSet diff gnocchiResults.keys.toSet).isEmpty, "Plink filters different!")
    assert((gnocchiResults.keys.toSet diff plinkResults.keys.toSet).isEmpty, "Plink filters different!")
    gnocchiResults.foreach { case (snp, pvalue) => assert(pvalue - plinkResults(snp) < 0.0001, s"Difference between plink and gnocchi results for SNP $snp") }
  }

  /**
   * plink command:
   * plink --vcf resources/RegressionIntegrationTestData_genotypes.vcf --make-bed --out regressionIntegrationTestData
   * plink --bfile regressionIntegrationTestData --pheno resources/LinearRegressionIntegrationTestData_phenotypes.txt --pheno-name pheno_1 --linear dominant --adjust --out test --allow-no-sex --mind 0.1 --maf 0.1 --geno 0.1
   */
  sparkTest("RegressPhenotypes results should match plink output: Linear Dominant.") {
    val sparkSession = sc.sparkSession
    import sparkSession.implicits._

    val plinkResults = Map(
      ("rs7717145", 0.02997),
      ("rs891382", 0.05757),
      ("rs919086", 0.06957),
      ("rs3042096", 0.08001),
      ("rs8868170", 0.114),
      ("rs3144742", 0.1164),
      ("rs9715474", 0.1274),
      ("rs1567086", 0.1483),
      ("rs3257620", 0.156),
      ("rs5133591", 0.1777),
      ("rs5373017", 0.2924),
      ("rs8330247", 0.4473),
      ("rs9175124", 0.4493),
      ("rs4795741", 0.4626),
      ("rs707848", 0.4742),
      ("rs8717426", 0.4782),
      ("rs2075136", 0.4847),
      ("rs8068892", 0.5091),
      ("rs5481294", 0.5193),
      ("rs8451144", 0.5797),
      ("rs3685511", 0.5869),
      ("rs9221689", 0.6197),
      ("rs4046230", 0.6502),
      ("rs7349598", 0.8316),
      ("rs205038", 0.8486),
      ("rs3646124", 0.8575),
      ("rs6071519", 0.8897),
      ("rs5395585", 0.9147),
      ("rs9101921", 0.9753))

    val genotypes = testFile("RegressionIntegrationTestData_genotypes.vcf")
    val phenotypes = testFile("LinearRegressionIntegrationTestData_phenotypes.txt")

    val resultsPath = tmpFile("out/")

    val gnocchiCommand =
      genotypes + " " +
        phenotypes + " " +
        "DOMINANT_LINEAR " +
        resultsPath + " " +
        "-mind 0.1 " +
        "-maf 0.1 " +
        "-geno 0.1 " +
        "-phenoName pheno_1 " +
        "-sampleIDName IID " +
        "-saveAsText"

    RegressPhenotypes(gnocchiCommand.split(" ")).run(sc)

    val gnocchiResults = sparkSession.read.format("csv").option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .load(resultsPath)
      .select($"uniqueID", $"pValue")
      .as[(String, Double)]
      .collect()
      .toMap

    assert((plinkResults.keys.toSet diff gnocchiResults.keys.toSet).isEmpty, "Plink filters different!")
    assert((gnocchiResults.keys.toSet diff plinkResults.keys.toSet).isEmpty, "Plink filters different!")
    gnocchiResults.foreach { case (snp, pvalue) => assert(pvalue - plinkResults(snp) < 0.0001, s"Difference between plink and gnocchi results for SNP $snp") }
  }

  /**
   * plink command:
   * plink --vcf resources/RegressionIntegrationTestData_genotypes.vcf --make-bed --out regressionIntegrationTestData
   * plink --bfile regressionIntegrationTestData --pheno resources/LogisticRegressionIntegrationTestData_phenotypes.txt --pheno-name pheno_1 --logistic --adjust --out test --allow-no-sex --mind 0.1 --maf 0.1 --geno 0.1 --1
   */
  sparkTest("RegressPhenotypes results should match plink output: Logistic Additive.") {
    val sparkSession = sc.sparkSession
    import sparkSession.implicits._

    val plinkResults = Map(
      ("rs8717426", 0.01017),
      ("rs3685511", 0.0697),
      ("rs5373017", 0.07347),
      ("rs3646124", 0.08301),
      ("rs7349598", 0.1155),
      ("rs8451144", 0.1175),
      ("rs1567086", 0.1176),
      ("rs3042096", 0.1196),
      ("rs8868170", 0.1225),
      ("rs4795741", 0.141),
      ("rs891382", 0.143),
      ("rs919086", 0.173),
      ("rs9175124", 0.2403),
      ("rs4046230", 0.2835),
      ("rs5395585", 0.3205),
      ("rs2075136", 0.3245),
      ("rs9221689", 0.3435),
      ("rs7717145", 0.585),
      ("rs6071519", 0.6157),
      ("rs9101921", 0.6371),
      ("rs3144742", 0.7063),
      ("rs707848", 0.7355),
      ("rs5481294", 0.7425),
      ("rs5133591", 0.7781),
      ("rs3257620", 0.8395),
      ("rs9715474", 0.8672),
      ("rs8068892", 0.8992),
      ("rs8330247", 0.9032),
      ("rs205038", 0.9124))

    val genotypes = testFile("RegressionIntegrationTestData_genotypes.vcf")
    val phenotypes = testFile("LogisticRegressionIntegrationTestData_phenotypes.txt")

    val resultsPath = tmpFile("out/")

    val gnocchiCommand =
      genotypes + " " +
        phenotypes + " " +
        "ADDITIVE_LOGISTIC " +
        resultsPath + " " +
        "-mind 0.1 " +
        "-maf 0.1 " +
        "-geno 0.1 " +
        "-phenoName pheno_1 " +
        "-sampleIDName IID " +
        "-saveAsText"

    RegressPhenotypes(gnocchiCommand.split(" ")).run(sc)

    val gnocchiResults = sparkSession.read.format("csv").option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .load(resultsPath)
      .select($"uniqueID", $"pValue")
      .as[(String, Double)]
      .collect()
      .toMap

    assert((plinkResults.keys.toSet diff gnocchiResults.keys.toSet).isEmpty, "Plink filters different!")
    assert((gnocchiResults.keys.toSet diff plinkResults.keys.toSet).isEmpty, "Plink filters different!")
    gnocchiResults.foreach { case (snp, pvalue) => assert(pvalue - plinkResults(snp) < 0.0001, s"Difference between plink and gnocchi results for SNP $snp") }
  }

  /**
   * plink command:
   * plink --vcf resources/RegressionIntegrationTestData_genotypes.vcf --make-bed --out regressionIntegrationTestData
   * plink --bfile regressionIntegrationTestData --pheno resources/LogisticRegressionIntegrationTestData_phenotypes.txt --pheno-name pheno_1 --logistic dominant --adjust --out test --allow-no-sex --mind 0.1 --maf 0.1 --geno 0.1 --1
   */
  sparkTest("RegressPhenotypes results should match plink output: Logistic Dominant.") {
    val sparkSession = sc.sparkSession
    import sparkSession.implicits._

    val plinkResults = Map(
      ("rs3685511", 0.01768),
      ("rs3042096", 0.03138),
      ("rs5373017", 0.04239),
      ("rs4795741", 0.06928),
      ("rs7349598", 0.07639),
      ("rs1567086", 0.07792),
      ("rs919086", 0.113),
      ("rs9175124", 0.1284),
      ("rs5395585", 0.1304),
      ("rs891382", 0.1357),
      ("rs9221689", 0.1542),
      ("rs8717426", 0.1572),
      ("rs3646124", 0.1652),
      ("rs9101921", 0.1764),
      ("rs2075136", 0.1798),
      ("rs8451144", 0.2126),
      ("rs707848", 0.2945),
      ("rs8868170", 0.4411),
      ("rs3144742", 0.5194),
      ("rs7717145", 0.5479),
      ("rs8068892", 0.5718),
      ("rs4046230", 0.6146),
      ("rs3257620", 0.6629),
      ("rs205038", 0.6675),
      ("rs5133591", 0.79),
      ("rs9715474", 0.7962),
      ("rs5481294", 0.846),
      ("rs6071519", 0.8669),
      ("rs8330247", 0.9376))

    val genotypes = testFile("RegressionIntegrationTestData_genotypes.vcf")
    val phenotypes = testFile("LogisticRegressionIntegrationTestData_phenotypes.txt")

    val resultsPath = tmpFile("out/")

    val gnocchiCommand =
      genotypes + " " +
        phenotypes + " " +
        "DOMINANT_LOGISTIC " +
        resultsPath + " " +
        "-mind 0.1 " +
        "-maf 0.1 " +
        "-geno 0.1 " +
        "-phenoName pheno_1 " +
        "-sampleIDName IID " +
        "-saveAsText"

    RegressPhenotypes(gnocchiCommand.split(" ")).run(sc)

    val gnocchiResults = sparkSession.read.format("csv").option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .load(resultsPath)
      .select($"uniqueID", $"pValue")
      .as[(String, Double)]
      .collect()
      .toMap

    val plinkRes = testFile("plink.test.assoc.linear.additive.adjusted")

    assert((plinkResults.keys.toSet diff gnocchiResults.keys.toSet).isEmpty, "Plink filters different!")
    assert((gnocchiResults.keys.toSet diff plinkResults.keys.toSet).isEmpty, "Plink filters different!")
    gnocchiResults.foreach { case (snp, pvalue) => assert(pvalue - plinkResults(snp) < 0.0001, s"Difference between plink and gnocchi results for SNP $snp") }
  }
}
