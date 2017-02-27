// /**
//  * Copyright 2015 Frank Austin Nothaft
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  *     http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */
// package net.fnothaft.gnocchi.association

// import net.fnothaft.gnocchi.GnocchiFunSuite
// import net.fnothaft.gnocchi.models.{ BooleanPhenotype, GenotypeState, Phenotype }
// import org.apache.spark.sql.SQLContext

// class LoadPhenotypeSuite extends GnocchiFunSuite {

//   def gs(sampleId: String): GenotypeState = {
//     GenotypeState("1",
//       0L,
//       1L,
//       "A",
//       "T",
//       sampleId,
//       0)
//   }

//   test("load simple phenotypes") {
//     val p1 = LoadPhenotypes.parseLine[Boolean]("mySample, a phenotype, true")
//     assert(p1.sampleId === "mySample")
//     assert(p1.phenotype === "a phenotype")
//     assert(p1.value)

//     val p2 = LoadPhenotypes.parseLine[Boolean]("mySample, another phenotype, false")
//     assert(p2.sampleId === "mySample")
//     assert(p2.phenotype === "another phenotype")
//     assert(!p2.value)

//     intercept[AssertionError] {
//       LoadPhenotypes.parseLine[Boolean]("mySample, bad phenotype")
//     }
//   }

//   ignore("load phenotypes from a file") {
//     val filepath = ClassLoader.getSystemClassLoader.getResource("samplePhenotypes.csv").getFile

//     val phenotypes = LoadPhenotypes[Boolean](filepath, sc)
//       .collect()

//     assert(phenotypes.length === 2)
//     assert(phenotypes.forall(_.sampleId == "mySample"))
//     assert(phenotypes.filter(_.phenotype == "a phenotype").length === 1)
//     assert(phenotypes.filter(_.phenotype == "a phenotype").head.value)
//     assert(phenotypes.filter(_.phenotype == "another phenotype").length === 1)
//     assert(!phenotypes.filter(_.phenotype == "another phenotype").head.value)
//   }

//   var logMsgs = Seq[String]()
//   def logFn(s: String): Unit = {
//     logMsgs = logMsgs :+ s
//   }

//   def makePheno(pheno: String, sample: String, value: Boolean): Phenotype[Boolean] = {
//     BooleanPhenotype(pheno, sample, value)
//   }

//   ignore("validating genotype/phenotype match should succeed if ids match") {
//     val sqlContext = SQLContext.getOrCreate(sc)
//     import sqlContext.implicits._

//     logMsgs = Seq.empty

//     val (mp, mg, samples) = LoadPhenotypes.validateSamples(sqlContext.createDataset(Seq(
//       makePheno("myPhenotype",
//         "mySample",
//         true))),
//       sqlContext.createDataset(Seq(
//         gs("mySample"))),
//       logFn)
//     assert(mp === 0)
//     assert(mg === 0)

//     val sArray = samples.collect
//     assert(sArray.length === 1)
//     assert(sArray.head === "mySample")

//     assert(logMsgs.size === 1)
//     assert(logMsgs(0) === "Regressing across 1 samples and 1 observed phenotypes.")
//   }

//   ignore("validating genotype/phenotype match should fail if ids do not match") {
//     val sqlContext = SQLContext.getOrCreate(sc)
//     import sqlContext.implicits._

//     logMsgs = Seq.empty

//     val (mp, mg, samples) = LoadPhenotypes.validateSamples(sqlContext.createDataset(Seq(
//       makePheno("myPhenotype",
//         "notMySample",
//         true))),
//       sqlContext.createDataset(Seq(
//         gs("mySample"))),
//       logFn)
//     assert(mp === 1)
//     assert(mg === 1)

//     val sArray = samples.collect
//     assert(sArray.length === 1)
//     assert(sArray.head === "mySample")

//     assert(logMsgs.size === 5)
//     assert(logMsgs(0) === "Regressing across 1 samples and 1 observed phenotypes.")
//     assert(logMsgs(1) === "Missing 1 genotyped sample in phenotypes:")
//     assert(logMsgs(2) === "mySample")
//     assert(logMsgs(3) === "Missing 1 phenotyped sample in genotypes:")
//     assert(logMsgs(4) === "notMySample")
//   }

//   ignore("validating phenotypes should succeed if there is a phenotype per sample") {
//     val sqlContext = SQLContext.getOrCreate(sc)
//     import sqlContext.implicits._

//     logMsgs = Seq.empty

//     val count = LoadPhenotypes.validatePhenotypes(sqlContext.createDataset(Seq(
//       makePheno("myPhenotype",
//         "mySample",
//         true))),
//       sqlContext.createDataset(Seq("mySample")),
//       logFn)
//     assert(count === 0)

//     assert(logMsgs.isEmpty)
//   }

//   ignore("validating phenotypes should fail if there are missing phenotypes for a sample") {
//     val sqlContext = SQLContext.getOrCreate(sc)
//     import sqlContext.implicits._

//     logMsgs = Seq.empty

//     val count = LoadPhenotypes.validatePhenotypes(sqlContext.createDataset(Seq(
//       makePheno("myPhenotype",
//         "mySample",
//         true))),
//       sqlContext.createDataset(Seq("mySample", "notMySample")),
//       logFn)
//     assert(count === 1)

//     assert(logMsgs.size === 2)
//     assert(logMsgs(0) === "Have 1 missing phenotype observation:")
//     assert(logMsgs(1) === "Missing observation of myPhenotype phenotype for sample notMySample.")
//   }
// }
