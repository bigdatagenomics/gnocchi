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
package net.fnothaft.gnocchi

import net.fnothaft.gnocchi.primitives.genotype.GenotypeState
import net.fnothaft.gnocchi.primitives.phenotype.Phenotype
import net.fnothaft.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.utils.misc.SparkFunSuite

trait GnocchiFunSuite extends SparkFunSuite {
  override val appName: String = "gnocchi"
  override val properties: Map[String, String] = Map(
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    "spark.kryo.registrator" -> "org.bdgenomics.adam.serialization.ADAMKryoRegistrator",
    "spark.kryo.referenceTracking" -> "true",
    "spark.driver.allowMultipleContexts" -> "false")

  val random = scala.util.Random

  def createSampleGenotypeStates(num: Int = 10,
                                 maf: Double = 0.0,
                                 geno: Double = 0.0,
                                 ploidy: Int = 2): List[GenotypeState] = {
    val iids = random.shuffle(1000 to 9999 + num).take(num)
    val gsString = List.fill((num * ploidy * maf).toInt)("1") ++ List.fill((num * ploidy * geno).toInt)(".")
    val gsString2 = gsString ++ List.fill((num * ploidy) - gsString.length)("0")
    val gsVect = random.shuffle(gsString2).grouped(2).toList.map(_.mkString("/"))

    gsVect.zip(iids).map(x => GenotypeState(x._2.toString, x._1))
  }

  def createSampleCalledVariant(chromosome: Option[Int] = None,
                                position: Option[Int] = None,
                                uniqueID: Option[String] = None,
                                referenceAllele: Option[String] = None,
                                alternateAllele: Option[String] = None,
                                qualityScore: Option[String] = None,
                                filter: Option[String] = None,
                                info: Option[String] = None,
                                format: Option[String] = None,
                                samples: Option[List[GenotypeState]] = None): CalledVariant = {

    val chrom = if (chromosome.isEmpty) random.nextInt(22) + 1 else chromosome.get
    val pos = if (position.isEmpty) random.nextInt(100000000) else position.get
    val uid = if (uniqueID.isEmpty) "rs" + (random.nextInt(1000000) + 10000) else uniqueID.get
    val ref = if (referenceAllele.isEmpty) random.shuffle(List("A", "C", "T", "G")).head else referenceAllele.get
    val alt = if (alternateAllele.isEmpty) random.shuffle(List("A", "C", "T", "G").filter(_ != ref)).head else alternateAllele.get
    val qs = if (qualityScore.isEmpty) "." else qualityScore.get
    val fil = if (filter.isEmpty) "." else filter.get
    val inf = if (info.isEmpty) "." else info.get
    val form = if (format.isEmpty) "." else format.get
    val sam = if (samples.isEmpty) createSampleGenotypeStates(num = 50) else samples.get

    CalledVariant(chrom, pos, uid, ref, alt, qs, fil, inf, form, sam)
  }

  def createSamplePhenotype(calledVariant: Option[CalledVariant] = None,
                            sampleIDs: Option[List[Int]] = None,
                            numCovariate: Int = 0,
                            cases: Option[List[Int]] = None,
                            num: Int = 10,
                            phenoName: String = "pheno",
                            caseControl: Boolean = false): Map[String, Phenotype] = {

    val ids: List[Int] = if (calledVariant.isDefined) {
      calledVariant.get.samples.map(_.sampleID.toInt)
    } else if (sampleIDs.isDefined) {
      sampleIDs.get
    } else {
      random.shuffle(1000 to 9999 + num).take(num).toList
    }

    val phenos = if (caseControl) {
      ids.map(id => (id.toString, Phenotype(id.toString, phenoName, random.shuffle(List(0, 1)).head, random.shuffle(1000 to 9999 + numCovariate).take(numCovariate).toList.map(_.toDouble))))
    } else {
      ids.map(id => (id.toString, Phenotype(id.toString, phenoName, random.nextDouble() * 100, random.shuffle(1000 to 9999 + numCovariate).take(numCovariate).toList.map(_.toDouble))))
    }
    phenos.toMap
  }
}

