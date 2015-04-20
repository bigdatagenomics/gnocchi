/**
 * Copyright 2015 Frank Austin Nothaft
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
package net.fnothaft.g2pilot

import net.fnothaft.g2pilot.avro.{ Association, Phenotype }
import org.bdgenomics.adam.models.ReferencePosition
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.misc.MathUtils
import scala.collection.JavaConversions._

class ScoreAssociationSuite extends G2PilotFunSuite {

  test("convert gp pairs") {

    val ctg = Contig.newBuilder()
      .setContigName("1")
      .build()
    val v = Variant.newBuilder()
      .setContig(ctg)
      .setStart(100L)
      .setEnd(101L)
      .setReferenceAllele("A")
      .setAlternateAllele("C")
      .build()
    
    // make a reference genotype that is not associated with a phenotype
    val g1 = Genotype.newBuilder()
      .setVariant(v)
      .setSampleId("mySample")
      .setAlleles(seqAsJavaList(Seq(GenotypeAllele.Ref, GenotypeAllele.Ref)))
      .build()
    val p1 = Phenotype.newBuilder()
      .setPhenotype("a phenotype")
      .setSampleId("mySample")
      .setHasPhenotype(false)
      .build()

    val opt1 = ScoreAssociation.toRecord(("mySample", (g1, p1)))
    assert(opt1.isDefined)
    val ((pos1, phenotype1), key1) = opt1.get
    assert(pos1 === ReferencePosition("1", 100L))
    assert(phenotype1 === "a phenotype")
    assert(key1.allele === "N")
    assert(key1.dosage === 0)
    assert(!key1.hasPhenotype)
    
    // make a het genotype that is associated with a phenotype
    val g2 = Genotype.newBuilder()
      .setVariant(v)
      .setSampleId("mySample")
      .setAlleles(seqAsJavaList(Seq(GenotypeAllele.Ref, GenotypeAllele.Alt)))
      .build()
    val p2 = Phenotype.newBuilder()
      .setPhenotype("another phenotype")
      .setSampleId("mySample")
      .setHasPhenotype(true)
      .build()

    val opt2 = ScoreAssociation.toRecord(("mySample", (g2, p2)))
    assert(opt2.isDefined)
    val ((pos2, phenotype2), key2) = opt2.get
    assert(pos2 === ReferencePosition("1", 100L))
    assert(phenotype2 === "another phenotype")
    assert(key2.allele === "C")
    assert(key2.dosage === 1)
    assert(key2.hasPhenotype)

    // make an "illegal" genotype (haploid)
    val g3 = Genotype.newBuilder()
      .setVariant(v)
      .setSampleId("mySample")
      .setAlleles(seqAsJavaList(Seq(GenotypeAllele.Ref)))
      .build()

    val opt3 = ScoreAssociation.toRecord(("mySample", (g3, p2)))
    assert(opt3.isEmpty)
  }

  test("check chi squared") {
    val (oHet, oHom, χ2, logP) = ScoreAssociation.chiSquared(90, 40, 15, 10, 10, 10)

    assert(MathUtils.fpEquals(oHet, (90.0 * 10.0) / (40.0 * 10.0)))
    assert(MathUtils.fpEquals(oHom, (90.0 * 10.0) / (15.0 * 10.0)))
    assert(MathUtils.fpEquals(χ2, 13.074712643678161))
    assert(MathUtils.fpEquals(logP, -6.537356321839081))
  }

  sparkTest("compute association") {
    val ctg = Contig.newBuilder()
      .setContigName("1")
      .build()
    val v1 = Variant.newBuilder()
      .setContig(ctg)
      .setStart(100L)
      .setEnd(101L)
      .setReferenceAllele("A")
      .setAlternateAllele("C")
      .build()
    val v2 = Variant.newBuilder()
      .setContig(ctg)
      .setStart(100L)
      .setEnd(101L)
      .setReferenceAllele("A")
      .setAlternateAllele("T")
      .build()
    
    val homRefNP = (0 until 90).map(i => {
      val sample = "sample_%d".format(i)
      val gHomRef = Genotype.newBuilder()
        .setVariant(v1)
        .setAlleles(seqAsJavaList(Seq(GenotypeAllele.Ref, GenotypeAllele.Ref)))
        .setSampleId(sample)
        .build()
      val pNot = Phenotype.newBuilder()
        .setPhenotype("a phenotype")
        .setHasPhenotype(false)
        .setSampleId(sample)
        .build()
      (sample, (gHomRef, pNot))
    }).toSeq

    val homRefP = (90 until 100).map(i => {
      val sample = "sample_%d".format(i)
      val gHomRef = Genotype.newBuilder()
        .setVariant(v1)
        .setAlleles(seqAsJavaList(Seq(GenotypeAllele.Ref, GenotypeAllele.Ref)))
        .setSampleId(sample)
        .build()
      val pHas = Phenotype.newBuilder()
        .setPhenotype("a phenotype")
        .setHasPhenotype(true)
        .setSampleId(sample)
        .build()
      (sample, (gHomRef, pHas))
    }).toSeq

    val hetNP = (100 until 140).map(i => {
      val sample = "sample_%d".format(i)
      val gHet = Genotype.newBuilder()
        .setVariant(v1)
        .setAlleles(seqAsJavaList(Seq(GenotypeAllele.Alt, GenotypeAllele.Ref)))
        .setSampleId(sample)
        .build()
      val pNot = Phenotype.newBuilder()
        .setPhenotype("a phenotype")
        .setHasPhenotype(false)
        .setSampleId(sample)
        .build()
      (sample, (gHet, pNot))
    }).toSeq

    val hetP = (140 until 150).map(i => {
      val sample = "sample_%d".format(i)
      val gHet = Genotype.newBuilder()
        .setVariant(v1)
        .setAlleles(seqAsJavaList(Seq(GenotypeAllele.Ref, GenotypeAllele.Alt)))
        .setSampleId(sample)
        .build()
      val pHas = Phenotype.newBuilder()
        .setPhenotype("a phenotype")
        .setHasPhenotype(true)
        .setSampleId(sample)
        .build()
      (sample, (gHet, pHas))
    }).toSeq

    val homAltNP = (150 until 165).map(i => {
      val sample = "sample_%d".format(i)
      val gHomAlt = Genotype.newBuilder()
        .setVariant(v1)
        .setAlleles(seqAsJavaList(Seq(GenotypeAllele.Alt, GenotypeAllele.Alt)))
        .setSampleId(sample)
        .build()
      val pNot = Phenotype.newBuilder()
        .setPhenotype("a phenotype")
        .setHasPhenotype(false)
        .setSampleId(sample)
        .build()
      (sample, (gHomAlt, pNot))
    }).toSeq

    val homAltP = (165 until 175).map(i => {
      val sample = "sample_%d".format(i)
      val gHomAlt = Genotype.newBuilder()
        .setVariant(v1)
        .setAlleles(seqAsJavaList(Seq(GenotypeAllele.Alt, GenotypeAllele.Alt)))
        .setSampleId(sample)
        .build()
      val pHas = Phenotype.newBuilder()
        .setPhenotype("a phenotype")
        .setHasPhenotype(true)
        .setSampleId(sample)
        .build()
      (sample, (gHomAlt, pHas))
    }).toSeq

    val triNP = (175 until 200).map(i => {
      val sample = "sample_%d".format(i)
      val gHet = Genotype.newBuilder()
        .setVariant(v2)
        .setAlleles(seqAsJavaList(Seq(GenotypeAllele.Alt, GenotypeAllele.Ref)))
        .setSampleId(sample)
        .build()
      val pNot = Phenotype.newBuilder()
        .setPhenotype("a phenotype")
        .setHasPhenotype(false)
        .setSampleId(sample)
        .build()
      (sample, (gHet, pNot))
    }).toSeq

    val rdd = sc.parallelize(homRefP ++ homRefNP ++ hetP ++ hetNP ++ homAltP ++ homAltNP ++ triNP)
    val associations = ScoreAssociation(rdd)
      .collect()
    
    assert(associations.length === 1)
    val association = associations.head
    assert(association.getPhenotype === "a phenotype")
    assert(association.getChromosome === "1")
    assert(association.getPosition === 100L)
    assert(association.getAlternateAllele === "C")
    assert(MathUtils.fpEquals(association.getOddsRatioHet, (90.0 * 10.0) / (40.0 * 10.0)))
    assert(MathUtils.fpEquals(association.getOddsRatioHomAlt, (90.0 * 10.0) / (15.0 * 10.0)))
    assert(MathUtils.fpEquals(association.getChiSquared, 13.074712643678161))
    assert(MathUtils.fpEquals(association.getLog10PNullHypothesis, -2.8391377768100514))
    assert(MathUtils.fpEquals(association.getMajorAlleleFrequency, (100.0 * 2.0 + 50.0) / (2.0 * 175.0)))
  }
}
