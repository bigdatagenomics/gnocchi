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

// import net.fnothaft.gnocchi.models.{
//   Association,
//   GenotypeState,
//   Phenotype
// }
// import org.apache.spark.sql.{ Dataset, Row }
// import org.apache.spark.sql.functions._
// import org.bdgenomics.formats.avro.{ Contig, Variant }
// import scala.math.{ log => mathLog }

// private[gnocchi] object ScoreAssociation extends Serializable {

//   // store the natural log of 10 as a constant
//   val LOG10 = mathLog(10.0)

//   /**
//    * Runs a chi squared test to rule out the null hypothesis that there is no association
//    * at this site.
//    *
//    * @param homRefNP The number of samples with a homozygous reference genotype and without the phenotype.
//    * @param hetNP The number of samples with a heterozygous genotype and without the phenotype.
//    * @param homAltNP The number of samples with a homozygous alternate genotype and without the phenotype.
//    * @param homRefP The number of samples with a homozygous reference genotype and with the phenotype.
//    * @param hetP The number of samples with a heterozygous genotype and with the phenotype.
//    * @param homAltP The number of samples with a homozygous alternate genotype and with the phenotype.
//    * @return Returns a tuple of four doubles. This tuple contains the odds ratio for an association with
//    *         the heterozygous genotype, the odds ratio for an association with the homozygous alt genotype,
//    *         the chi squared statistic, and the log probability of the null hypothesis.
//    */
//   def chiSquared(homRefNP: Long, hetNP: Long, homAltNP: Long,
//                  homRefP: Long, hetP: Long, homAltP: Long): (Double, Double, Double, Double) = {
//     // odds ratio for het
//     val oHet = (homRefNP * hetP).toDouble / (homRefP * hetNP).toDouble

//     // odds ratio for hom alt
//     val oHom = (homRefNP * homAltP).toDouble / (homRefP * homAltNP).toDouble

//     def stat(v: Long, d: Double): Double = {
//       val t = v.toDouble - d
//       t * t / d
//     }

//     // chi squared test statistic
//     val np = (homRefNP + hetNP + homAltNP).toDouble
//     val p = (homRefP + hetP + homAltP).toDouble
//     val n = np + p
//     val homRef = (homRefNP + homRefP).toDouble
//     val het = (hetNP + hetP).toDouble
//     val homAlt = (homAltNP + homAltP).toDouble
//     val χ2 = (stat(homRefNP, homRef * np / n) +
//       stat(hetNP, het * np / n) +
//       stat(homAltNP, homAlt * np / n) +
//       stat(homRefP, homRef * p / n) +
//       stat(hetP, het * p / n) +
//       stat(homAltP, homAlt * p / n))

//     // for 2 degrees of freedom, CDF of chi squared distribution is F(x) = 1 - exp(-x / 2)
//     // therefore, log probability of null hypothesis is -x / 2
//     val logP = -χ2 / 2.0

//     (oHet, oHom, χ2, logP)
//   }

//   def apply(genotypes: Dataset[GenotypeState],
//             phenotypes: Dataset[Phenotype[Boolean]]): Dataset[Association] = {

//     // get sql context from an input; will need later to construct returned dataset
//     val sqlContext = genotypes.sqlContext

//     // join genotype and phenotype observations
//     val gdf = genotypes.toDF
//     val pdf = phenotypes.toDF.withColumnRenamed("sampleId", "pSampleId")
//     val joined = gdf.join(pdf, gdf("sampleId") === pdf("pSampleId")).drop("pSampleId")

//     // project chi squared stats per variant/sample/phenotype combo
//     val sampleStats = joined.select(joined("contig").as("contig"),
//       joined("start").as("start"),
//       joined("end").as("end"),
//       joined("ref").as("ref"),
//       joined("alt").as("alt"),
//       joined("phenotype").as("phenotype"),
//       when(joined("genotypeState") === 0 && !joined("value"), 1)
//         .otherwise(0).as("0/0"),
//       when(joined("genotypeState") === 1 && !joined("value"), 1)
//         .otherwise(0).as("0/1"),
//       when(joined("genotypeState") === 2 && !joined("value"), 1)
//         .otherwise(0).as("0/2"),
//       when(joined("genotypeState") === 0 && joined("value"), 1)
//         .otherwise(0).as("1/0"),
//       when(joined("genotypeState") === 1 && joined("value"), 1)
//         .otherwise(0).as("1/1"),
//       when(joined("genotypeState") === 2 && joined("value"), 1)
//         .otherwise(0).as("1/2"))

//     // aggregate by site/phenotype combo
//     val chiSquaredPerSiteRDD = sampleStats.groupBy("contig", "start", "end", "ref", "alt", "phenotype")
//       .sum("0/0", "0/1", "0/2", "1/0", "1/1", "1/2")
//       .withColumnRenamed("sum(0/0)", "s00")
//       .withColumnRenamed("sum(0/1)", "s01")
//       .withColumnRenamed("sum(0/2)", "s02")
//       .withColumnRenamed("sum(1/0)", "s10")
//       .withColumnRenamed("sum(1/1)", "s11")
//       .withColumnRenamed("sum(1/2)", "s12")
//       .map(r => r match {
//         case Row(contig: String,
//           start: Long,
//           end: Long,
//           ref: String,
//           alt: String,
//           phenotype: String,
//           s00: Long,
//           s01: Long,
//           s02: Long,
//           s10: Long,
//           s11: Long,
//           s12: Long) => {

//           // calculate chi squared statistic
//           val (oHet, oHom, χ2, logP) = chiSquared(s00, s01, s02, s10, s11, s12)

//           Association(Variant.newBuilder()
//             .setContig(Contig.newBuilder()
//               .setContigName(contig)
//               .build())
//             .setStart(start)
//             .setEnd(end)
//             .setReferenceAllele(ref)
//             .setAlternateAllele(alt)
//             .build(),
//             phenotype,
//             logP,
//             Map("logOddsHet" -> oHet,
//               "logOddsHom" -> oHom,
//               "χ2" -> χ2,
//               "s00" -> s00,
//               "s01" -> s01,
//               "s02" -> s02,
//               "s10" -> s10,
//               "s11" -> s11,
//               "s12" -> s12))
//         }
//       })

//     // translate RDD back into dataset
//     import sqlContext.implicits._
//     sqlContext.createDataset(chiSquaredPerSiteRDD)
//   }
// }
