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
// import net.fnothaft.gnocchi.models.{ Association, BooleanPhenotype, GenotypeState }
// import net.fnothaft.gnocchi.sql.GnocchiContext._
// import org.apache.spark.sql.SQLContext
// import org.bdgenomics.formats.avro._
// import org.bdgenomics.utils.misc.MathUtils
// import scala.collection.JavaConversions._

// class ScoreAssociationSuite extends GnocchiFunSuite {

//   test("check chi squared") {
//     val (oHet, oHom, χ2, logP) = ScoreAssociation.chiSquared(90, 40, 15, 10, 10, 10)

//     assert(MathUtils.fpEquals(oHet, (90.0 * 10.0) / (40.0 * 10.0)))
//     assert(MathUtils.fpEquals(oHom, (90.0 * 10.0) / (15.0 * 10.0)))
//     assert(MathUtils.fpEquals(χ2, 13.074712643678161))
//     assert(MathUtils.fpEquals(logP, -6.537356321839081))
//   }

//   ignore("compute association") {
//     val ctg = Contig.newBuilder()
//       .setContigName("1")
//       .build()
//     val v1 = Variant.newBuilder()
//       .setContig(ctg)
//       .setStart(100L)
//       .setEnd(101L)
//       .setReferenceAllele("A")
//       .setAlternateAllele("C")
//       .build()
//     val v2 = Variant.newBuilder()
//       .setContig(ctg)
//       .setStart(100L)
//       .setEnd(101L)
//       .setReferenceAllele("A")
//       .setAlternateAllele("T")
//       .build()

//     val homRefNP = (0 until 90).map(i => {
//       val sample = "sample_%d".format(i)
//       val gHomRef = GenotypeState(v1.getContig.getContigName,
//         v1.getStart,
//         v1.getEnd,
//         v1.getReferenceAllele,
//         v1.getAlternateAllele,
//         sample,
//         0)
//       val pNot = BooleanPhenotype("a phenotype",
//         sample,
//         false)
//       (gHomRef, pNot)
//     }).toSeq

//     val homRefP = (90 until 100).map(i => {
//       val sample = "sample_%d".format(i)
//       val gHomRef = GenotypeState(v1.getContig.getContigName,
//         v1.getStart,
//         v1.getEnd,
//         v1.getReferenceAllele,
//         v1.getAlternateAllele,
//         sample,
//         0)
//       val pHas = BooleanPhenotype("a phenotype",
//         sample,
//         true)
//       (gHomRef, pHas)
//     }).toSeq

//     val hetNP = (100 until 140).map(i => {
//       val sample = "sample_%d".format(i)
//       val gHet = GenotypeState(v1.getContig.getContigName,
//         v1.getStart,
//         v1.getEnd,
//         v1.getReferenceAllele,
//         v1.getAlternateAllele,
//         sample,
//         1)
//       val pNot = BooleanPhenotype("a phenotype",
//         sample,
//         false)
//       (gHet, pNot)
//     }).toSeq

//     val hetP = (140 until 150).map(i => {
//       val sample = "sample_%d".format(i)
//       val gHet = GenotypeState(v1.getContig.getContigName,
//         v1.getStart,
//         v1.getEnd,
//         v1.getReferenceAllele,
//         v1.getAlternateAllele,
//         sample,
//         1)
//       val pHas = BooleanPhenotype("a phenotype",
//         sample,
//         true)
//       (gHet, pHas)
//     }).toSeq

//     val homAltNP = (150 until 165).map(i => {
//       val sample = "sample_%d".format(i)
//       val gHomAlt = GenotypeState(v1.getContig.getContigName,
//         v1.getStart,
//         v1.getEnd,
//         v1.getReferenceAllele,
//         v1.getAlternateAllele,
//         sample,
//         2)
//       val pNot = BooleanPhenotype("a phenotype",
//         sample,
//         false)
//       (gHomAlt, pNot)
//     }).toSeq

//     val homAltP = (165 until 175).map(i => {
//       val sample = "sample_%d".format(i)
//       val gHomAlt = GenotypeState(v1.getContig.getContigName,
//         v1.getStart,
//         v1.getEnd,
//         v1.getReferenceAllele,
//         v1.getAlternateAllele,
//         sample,
//         2)
//       val pHas = BooleanPhenotype("a phenotype",
//         sample,
//         true)
//       (gHomAlt, pHas)
//     }).toSeq

//     val obs = homRefP ++ homRefNP ++ hetP ++ hetNP ++ homAltP ++ homAltNP
//     val gts = obs.map(_._1) // for some godforsaken reason, unzip won't pass compile???
//     val pts = obs.map(_._2)
//     val sqlContext = SQLContext.getOrCreate(sc)
//     import sqlContext.implicits._
//     val associations = ScoreAssociation(sqlContext.createDataset(gts),
//       sqlContext.createDataset(pts))
//       .collect()

//     assert(associations.length === 1)
//     val association = associations.head
//     assert(association.phenotype === "a phenotype")
//     assert(association.variant.getContig.getContigName === "1")
//     assert(association.variant.getStart === 100L)
//     assert(association.variant.getAlternateAllele === "C")
//     assert(MathUtils.fpEquals(association.statistics("logOddsHet").asInstanceOf[Double],
//       (90.0 * 10.0) / (40.0 * 10.0)))
//     assert(MathUtils.fpEquals(association.statistics("logOddsHom").asInstanceOf[Double],
//       (90.0 * 10.0) / (15.0 * 10.0)))
//     assert(MathUtils.fpEquals(association.statistics("χ2").asInstanceOf[Double],
//       13.074712643678161))
//     assert(MathUtils.fpEquals(association.logPValue, -2.8391377768100514))
//   }
// }
