// this is a test script to generate data that can be used to test the gnocchi application
// to use this, hack together a gnocchi shell

import org.bdgenomics.formats.avro._
import net.fnothaft.gnocchi.avro._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.models.VariantContext
import scala.collection.JavaConversions._

val ctg = Contig.newBuilder().setContigName("1").build()
val v1 = Variant.newBuilder().setContig(ctg).setStart(100L).setEnd(101L).setReferenceAllele("A").setAlternateAllele("C").build()
val v2 = Variant.newBuilder().setContig(ctg).setStart(100L).setEnd(101L).setReferenceAllele("A").setAlternateAllele("T").build()

val homRefNP = (0 until 90).map(i => {
  val sample = "sample_%d".format(i)
  val gHomRef = Genotype.newBuilder().setVariant(v1).setAlleles(seqAsJavaList(Seq(GenotypeAllele.Ref, GenotypeAllele.Ref))).setSampleId(sample).build()
  val pNot = Phenotype.newBuilder().setPhenotype("a phenotype").setHasPhenotype(false).setSampleId(sample).build()
  (sample, (gHomRef, pNot))
}).toSeq

val homRefP = (90 until 100).map(i => {
  val sample = "sample_%d".format(i)
  val gHomRef = Genotype.newBuilder().setVariant(v1).setAlleles(seqAsJavaList(Seq(GenotypeAllele.Ref, GenotypeAllele.Ref))).setSampleId(sample).build()
  val pHas = Phenotype.newBuilder().setPhenotype("a phenotype").setHasPhenotype(true).setSampleId(sample).build()
  (sample, (gHomRef, pHas))
}).toSeq

val hetNP = (100 until 140).map(i => {
  val sample = "sample_%d".format(i)
  val gHet = Genotype.newBuilder().setVariant(v1).setAlleles(seqAsJavaList(Seq(GenotypeAllele.Alt, GenotypeAllele.Ref))).setSampleId(sample).build()
  val pNot = Phenotype.newBuilder().setPhenotype("a phenotype").setHasPhenotype(false).setSampleId(sample).build()
  (sample, (gHet, pNot))
}).toSeq

val hetP = (140 until 150).map(i => {
  val sample = "sample_%d".format(i)
  val gHet = Genotype.newBuilder().setVariant(v1).setAlleles(seqAsJavaList(Seq(GenotypeAllele.Ref, GenotypeAllele.Alt))).setSampleId(sample).build()
  val pHas = Phenotype.newBuilder().setPhenotype("a phenotype").setHasPhenotype(true).setSampleId(sample).build()
  (sample, (gHet, pHas))
}).toSeq

val homAltNP = (150 until 165).map(i => {
  val sample = "sample_%d".format(i)
  val gHomAlt = Genotype.newBuilder().setVariant(v1).setAlleles(seqAsJavaList(Seq(GenotypeAllele.Alt, GenotypeAllele.Alt))).setSampleId(sample).build()
  val pNot = Phenotype.newBuilder().setPhenotype("a phenotype").setHasPhenotype(false).setSampleId(sample).build()
  (sample, (gHomAlt, pNot))
}).toSeq

val homAltP = (165 until 175).map(i => {
  val sample = "sample_%d".format(i)
  val gHomAlt = Genotype.newBuilder().setVariant(v1).setAlleles(seqAsJavaList(Seq(GenotypeAllele.Alt, GenotypeAllele.Alt))).setSampleId(sample).build()
  val pHas = Phenotype.newBuilder().setPhenotype("a phenotype").setHasPhenotype(true).setSampleId(sample).build()
  (sample, (gHomAlt, pHas))
}).toSeq

val vcRdd = sc.parallelize(Seq(VariantContext.buildFromGenotypes((homRefP ++ homRefNP ++ hetP ++ hetNP ++ homAltP ++ homAltNP).map(kv => kv._2._1))), 1)
vcRdd.saveAsVcf("sample.vcf")
sc.parallelize(homRefP ++ homRefNP ++ hetP ++ hetNP ++ homAltP ++ homAltNP, 1).map(kv => kv._2._2).map(p => "%s, %s, %s".format(p.getSampleId, p.getPhenotype, p.getHasPhenotype)).saveAsTextFile("samplePhenotypes.csv")
exit()
