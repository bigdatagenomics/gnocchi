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

// import htsjdk.samtools.ValidationStringency
// import net.fnothaft.gnocchi.models._
// import org.apache.spark.{ Logging, SparkContext }
// import org.apache.spark.sql.{ Dataset, Row, SQLContext }

// private[gnocchi] object LoadPhenotypes extends Serializable with Logging {

//   private[association] def validateSamples[T](pDs: Dataset[Phenotype[T]],
//                                               sDs: Dataset[GenotypeState],
//                                               logFn: String => Unit): (Long, Long, Dataset[String]) = {
//     // get sample ids
//     val genotypedSamplesDF = sDs.toDF()
//     val genotypedSamples = genotypedSamplesDF.select(genotypedSamplesDF("sampleId"))
//       .distinct
//       .cache
//     val phenotypedSamplesDF = pDs.toDF()
//     val phenotypedSamples = phenotypedSamplesDF.select(phenotypedSamplesDF("sampleId"))
//       .distinct
//       .cache

//     // log the number of samples we are regressing
//     logFn("Regressing across %d samples and %d observed phenotypes.".format(
//       genotypedSamples.count,
//       phenotypedSamples.count))

//     // which samples are we missing? 
//     val gsPre = genotypedSamples.select(genotypedSamples("sampleId").as("gSampleId"))
//     val psPre = phenotypedSamples.select(phenotypedSamples("sampleId").as("pSampleId"))
//     val jt = gsPre.join(psPre, gsPre("gSampleId") === psPre("pSampleId"), "outer")
//       .cache
//     val missingInPts = jt.where(jt("pSampleId").isNull)
//       .select("gSampleId")
//       .rdd
//       .map(r => r match {
//         case Row(gSampleId: String) => gSampleId
//       }).cache
//     val missingInGts = jt.where(jt("gSampleId").isNull)
//       .select("pSampleId")
//       .rdd
//       .map(r => r match {
//         case Row(pSampleId: String) => pSampleId
//       }).cache
//     val mipCount = missingInPts.count()
//     val migCount = missingInGts.count()
//     jt.unpersist()

//     // record missing samples
//     if (mipCount == 0) {
//       log.info("All genotyped samples have a matching sample in the phenotypes.")
//     } else {
//       val plural = if (mipCount == 1) {
//         ""
//       } else {
//         "s"
//       }
//       logFn("Missing %d genotyped sample%s in phenotypes:".format(mipCount, plural))
//       missingInPts.toLocalIterator
//         .foreach(logFn)
//     }
//     if (migCount == 0) {
//       log.info("All phenotyped samples have a matching sample in the genotypes.")
//     } else {
//       val plural = if (migCount == 1) {
//         ""
//       } else {
//         "s"
//       }
//       logFn("Missing %d phenotyped sample%s in genotypes:".format(migCount, plural))
//       missingInGts.toLocalIterator
//         .foreach(logFn)
//     }

//     // unpersist phenotyped samples
//     phenotypedSamples.unpersist()
//     missingInGts.unpersist()
//     missingInPts.unpersist()

//     import genotypedSamples.sqlContext.implicits._
//     (mipCount, migCount, genotypedSamples.as[String])
//   }

//   private[association] def validatePhenotypes[T](pDs: Dataset[Phenotype[T]],
//                                                  genotypedSamples: Dataset[String],
//                                                  logFn: String => Unit): Long = {

//     // what phenotypes do we have? take the cartesian product vs samples
//     val phenotypesDF = pDs.toDF()
//     val phenotypes = phenotypesDF.select(phenotypesDF("phenotype"))
//       .distinct
//       .cache
//     val s2p = genotypedSamples.toDF()
//       .withColumnRenamed("value", "sampleId")
//       .join(phenotypes)

//     // do a left outer join to identify missing phenotypes
//     // then flatmap and create error messages
//     val pheno = phenotypesDF.select(phenotypesDF("sampleId").as("sample"), phenotypesDF("phenotype").as("pheno"))
//     val jt = s2p.join(pheno, s2p("sampleId") === pheno("sample") && s2p("phenotype") === pheno("pheno"), "left_outer")
//     val missing = jt.where(jt("sample").isNull).select(jt("sampleId"), jt("phenotype"))
//       .map(kv => kv match {
//         case Row(sampleId: String, phenotype: String) => {
//           (phenotype,
//             "Missing observation of %s phenotype for sample %s.".format(phenotype,
//               sampleId))
//         }
//       }).sortByKey()
//       .map(kv => kv._2)
//       .cache

//     // log missing phenotype observations
//     val missingCount = missing.count
//     if (missingCount == 0) {
//       logFn("Have a phenotype observation for every phenotype for all genotyped samples.")
//     } else {
//       val plural = if (missingCount == 1) {
//         ""
//       } else {
//         "s"
//       }
//       logFn("Have %d missing phenotype observation%s:".format(missingCount, plural))
//       missing.toLocalIterator.foreach(logFn)
//     }

//     // unpersist rdds
//     missing.unpersist()
//     phenotypes.unpersist()

//     missingCount
//   }

//   def validate[T](pRdd: Dataset[Phenotype[T]],
//                   sRdd: Dataset[GenotypeState],
//                   stringency: ValidationStringency) {
//     // we skip validation if stringency is set to silent
//     if (stringency != ValidationStringency.SILENT) {
//       val (logFn, endFn) = stringency match {
//         case ValidationStringency.STRICT => {
//           ((s: String) => log.error(s),
//             (c: Long) => throw new IllegalArgumentException("Exiting with %d errors.".format(c)))
//         }
//         case _ => {
//           ((s: String) => log.warn(s),
//             (c: Long) => log.warn("Had %d warnings.".format(c)))
//         }
//       }

//       // validate sample ids
//       val (mipCount, migCount, genotypedSamples) = validateSamples[T](pRdd, sRdd, logFn)

//       // validate phenotypes across samples
//       val missingCount = validatePhenotypes(pRdd, genotypedSamples, logFn)

//       // unpersist genotyped samples rdd
//       genotypedSamples.unpersist()

//       // total error count
//       val totalErrors = mipCount + migCount + missingCount
//       if (totalErrors == 0) {
//         log.info("No errors during genotype/phenotype validation.")
//       } else {
//         endFn(totalErrors)
//       }
//     }
//   }

//   def apply[T](file: String,
//                sc: SparkContext)(implicit mT: Manifest[T]): Dataset[Phenotype[T]] = {
//     log.info("Loading phenotypes from %s.".format(file))

//     // get sql context
//     val sqlContext = SQLContext.getOrCreate(sc)

//     // load and parse text file
//     import sqlContext.implicits._
//     sqlContext.createDataset(sc.textFile(file)
//       .map(parseLine[T]))
//   }

//   private[gnocchi] def parseLine[T](line: String)(implicit mT: Manifest[T]): Phenotype[T] = {
//     val splits = line.split(",")
//     assert(splits.length == 3, "Line was incorrectly formatted, did not contain sample, phenotype, phenotypeValue:\n%s".format(line))

//     val pheno = if (manifest[T] == manifest[Int]) {
//       IntPhenotype(splits(1).trim,
//         splits(0).trim,
//         splits(2).trim.toInt)
//     } else if (manifest[T] == manifest[Boolean]) {
//       BooleanPhenotype(splits(1).trim,
//         splits(0).trim,
//         splits(2).trim == "true")
//     } else if (manifest[T] == manifest[Double]) {
//       DoublePhenotype(splits(1).trim,
//         splits(0).trim,
//         splits(2).trim.toDouble)
//     } else {
//       throw new IllegalArgumentException("Type not found.")
//     }

//     pheno.asInstanceOf[Phenotype[T]]
//   }
// }
