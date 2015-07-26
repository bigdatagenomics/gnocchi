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
package net.fnothaft.gnocchi

import net.fnothaft.gnocchi.avro.{ Association, Phenotype }
import org.apache.spark.SparkContext._
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferencePosition
import org.bdgenomics.formats.avro.{ Genotype, GenotypeAllele }
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scala.math.{ log => mathLog }

private[gnocchi] case class GPKey(allele: String, dosage: Int, hasPhenotype: Boolean) {
}

object ScoreAssociation extends Serializable with Logging {

  // store the natural log of 10 as a constant
  val LOG10 = mathLog(10.0)

  /**
   *
   */
  private[gnocchi] def toRecord(kv: (String, (Genotype, Phenotype))): Option[((ReferencePosition, String), GPKey)] = {
    // extract the (sample ID, (site genotype, sample phenotype)) from the key-value pair we got
    val (s, (g, p)) = kv

    // get the position that we're looking at
    val v = g.getVariant
    val pos = ReferencePosition(v.getContig.getContigName,
                                v.getStart)

    // what genotypes does the sample have?
    val key = asScalaBuffer(g.getAlleles).toList match {
      case List(GenotypeAllele.Ref, GenotypeAllele.Ref) => {
        Some(GPKey("N", 0, p.getHasPhenotype))
      }
      case List(GenotypeAllele.Ref, GenotypeAllele.Alt) | List(GenotypeAllele.Alt, GenotypeAllele.Ref) => {
        Some(GPKey(v.getAlternateAllele, 1, p.getHasPhenotype))
      }
      case List(GenotypeAllele.Alt, GenotypeAllele.Alt) => {
        Some(GPKey(v.getAlternateAllele, 2, p.getHasPhenotype))
      }
      case _ => {
        // We discard any genotypes that are not diploid, as the statistical model we are using for
        // association testing uses 2 degrees of freedom (i.e., we assume biallelic diploid sites)
        log.warn("Discarding sample %s alt %s at pos %s with genotype: %s".format(s,
                                                                                  g.getVariant
                                                                                    .getAlternateAllele,
                                                                                  pos,
                                                                                  g.getAlleles))
        None
      }
    }

    // attach (position, phenotype) key
    key.map(v => ((pos, p.getPhenotype), v))
  }

  /**
   * Runs a chi squared test to rule out the null hypothesis that there is no association
   * at this site.
   *
   * @param homRefNP The number of samples with a homozygous reference genotype and without the phenotype.
   * @param hetNP The number of samples with a heterozygous genotype and without the phenotype.
   * @param homAltNP The number of samples with a homozygous alternate genotype and without the phenotype.
   * @param homRefP The number of samples with a homozygous reference genotype and with the phenotype.
   * @param hetP The number of samples with a heterozygous genotype and with the phenotype.
   * @param homAltP The number of samples with a homozygous alternate genotype and with the phenotype.
   * @return Returns a tuple of four doubles. This tuple contains the odds ratio for an association with
   *         the heterozygous genotype, the odds ratio for an association with the homozygous alt genotype,
   *         the chi squared statistic, and the log probability of the null hypothesis.
   */
  private[gnocchi] def chiSquared(homRefNP: Int, hetNP: Int, homAltNP: Int,
                                  homRefP: Int, hetP: Int, homAltP: Int): (Double, Double, Double, Double) = {
    // odds ratio for het
    val oHet = (homRefNP * hetP).toDouble / (homRefP * hetNP).toDouble

    // odds ratio for hom alt
    val oHom = (homRefNP * homAltP).toDouble / (homRefP * homAltNP).toDouble

    def stat(v: Int, d: Double): Double = {
      val t = v.toDouble - d
      t * t / d
    }

    // chi squared test statistic
    val np = (homRefNP + hetNP + homAltNP).toDouble
    val p = (homRefP + hetP + homAltP).toDouble
    val n = np + p
    val homRef = (homRefNP + homRefP).toDouble
    val het = (hetNP + hetP).toDouble
    val homAlt = (homAltNP + homAltP).toDouble
    val χ2 = (stat(homRefNP, homRef * np / n) +
             stat(hetNP, het * np / n) +
             stat(homAltNP, homAlt * np / n) +
             stat(homRefP, homRef * p / n) +
             stat(hetP, het * p / n) +
             stat(homAltP, homAlt * p / n))

    // for 2 degrees of freedom, CDF of chi squared distribution is F(x) = 1 - exp(-x / 2)
    // therefore, log probability of null hypothesis is -x / 2
    val logP = -χ2 / 2.0

    (oHet, oHom, χ2, logP)
  }

  /**
   * Computes the genotype-to-phenotype association statistics at this site. This is done using
   * a Chi squared test.
   *
   * @param A tuple describing the site, and the data we've observed. Contains ((the position of the site, the phenotype
   *        we are testing), a map describing the genotypes we've observed).
   * @return If there are any alternate alleles at the site, returns genotype/phenotype association statistics.
   */
  private[gnocchi] def toAssociation(site: ((ReferencePosition, String), HashMap[GPKey, Int])): Option[Association] = {
    // unpack observation
    val ((pos, phenotype), map) = site

    // find minor allele
    val alleles = map.keys
      .map(_.allele)
      .toSet
      .map((v: String) => {
        // filter all keys that contain this allele, and sum their occurence counts
        (v, map.filter(kv => kv._1.allele == v)
          .map(kv => (kv._1, kv._1.dosage * kv._2))
          .values
          .sum)
      })

    // do we have any alts?
    if (alleles.filter(kv => kv._1 != "N").size == 0) {
      log.info("Discarding site %s as either no alts were observed.".format(pos))
      None
    } else if (alleles.filter(kv => kv._1 == "N").size != 1) {
      log.info("Discarding site %s as no ref was observed.".format(pos))
      None
    } else {
      // pick most frequently appearing alt allele
      val alt = alleles.filter(kv => kv._1 != "N")
        .toSeq
        .sortBy(v => v._2)
        .last
        ._1
    
      // get counts
      val homRefP = map.getOrElse(GPKey("N", 0, true), 0)
      val hetP = map.getOrElse(GPKey(alt, 1, true), 0)
      val homAltP = map.getOrElse(GPKey(alt, 2, true), 0)
      val homRefNP = map.getOrElse(GPKey("N", 0, false), 0)
      val hetNP = map.getOrElse(GPKey(alt, 1, false), 0)
      val homAltNP = map.getOrElse(GPKey(alt, 2, false), 0)

      try {
        // compute chi squared stat
        val (oHet, oHom, χ2, logP) = chiSquared(homRefNP, hetNP, homAltNP,
                                                homRefP, hetP, homAltP)
        
        // compute major allele frequency
        val maf = (((homRefP + homRefNP) * 2 + hetP + hetNP).toDouble /
                   (2.0 * (homRefP + hetP + homAltP + homRefNP + hetNP + homAltNP).toDouble))
        
        Some(Association.newBuilder()
          .setPhenotype(phenotype)
          .setChromosome(pos.referenceName)
          .setPosition(pos.pos)
          .setAlternateAllele(alt)
          .setOddsRatioHet(oHet)
          .setOddsRatioHomAlt(oHom)
          .setChiSquared(χ2)
          .setLog10PNullHypothesis(logP / LOG10)
          .setMajorAlleleFrequency(maf)
          .build())
      } catch {
        case t: Throwable => {
          log.warn("Caught exception %s at site %s. Discarding site.".format(t, pos))
          None
        }
      }
    }
  }

  /**
   * 
   */
  def apply(rdd: RDD[(String, (Genotype, Phenotype))]): RDD[Association] = {
    rdd.flatMap(toRecord)
      .aggregateByKey(HashMap[GPKey, Int]())((m, v) => {
        // optionally get the value from the map
        // if we get a None, insert this key
        // if we get Some(k), increment the key value
        m.get(v).fold(m += (v -> 1))(cv => {
          m.update(v, cv + 1)
          m
        })

        // return the modified map
        m
      }, (m1, m2) => {
        // merge maps
        m2.foreach(kv => {
          val (k, v) = kv
          
          // optionally get the value from the map
          // if we get a None, insert this key value pair
          // if we get Some(k), merge the key value pair
          m1.get(k).fold(m1 += (k -> v))(cv => {
            m1.update(k, cv + v)
            m1
          })
        })

        // return the map that we updated in place
        m1
      }).flatMap(toAssociation)
  }
}
