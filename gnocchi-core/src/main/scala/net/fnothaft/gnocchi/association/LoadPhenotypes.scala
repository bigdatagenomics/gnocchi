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
package net.fnothaft.gnocchi.association

import htsjdk.samtools.ValidationStringency
import net.fnothaft.gnocchi.avro.Phenotype
import org.apache.spark.SparkContext._
import org.apache.spark.{ Logging, SparkContext }
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.Genotype

private[gnocchi] object LoadPhenotypes extends Serializable with Logging {

  private[association] def validateSamples(pRdd: RDD[Phenotype],
                                           sRdd: RDD[Genotype],
                                           logFn: String => Unit): (Long, Long, RDD[String]) = {
    // get sample ids
    val genotypedSamples = sRdd.map(_.getSampleId)
      .distinct
      .cache
    val phenotypedSamples = pRdd.map(_.getSampleId)
      .distinct
      .cache
    
    // log the number of samples we are regressing
    log.info("Regressing across %d samples and %d observed phenotypes.".format(
      genotypedSamples.count,
      pRdd.count))
    
    // which samples are we missing?
    val missingInPts = genotypedSamples.subtract(phenotypedSamples).cache
    val missingInGts = phenotypedSamples.subtract(genotypedSamples).cache
    val mipCount = missingInPts.count()
    val migCount = missingInGts.count()
    
    // record missing samples
    if (mipCount == 0) {
      log.info("All genotyped samples have a matching sample in the phenotypes.")
    } else {
      val plural = if (mipCount == 1) {
        ""
      } else {
        "s"
      }
      logFn("Missing %d genotyped sample%s in phenotypes:".format(mipCount, plural))
      missingInPts.toLocalIterator
        .foreach(logFn)
    }
    if (migCount == 0) {
      log.info("All phenotyped samples have a matching sample in the genotypes.")
    } else {
      val plural = if (migCount == 1) {
        ""
      } else {
        "s"
      }
      logFn("Missing %d phenotyped sample%s in genotypes:".format(migCount, plural))
      missingInGts.toLocalIterator
        .foreach(logFn)
    }
    
    // unpersist phenotyped samples
    phenotypedSamples.unpersist()
    missingInGts.unpersist()
    missingInPts.unpersist()
    
    (mipCount, migCount, genotypedSamples)
  }

  private[association] def validatePhenotypes(pRdd: RDD[Phenotype],
                                              genotypedSamples: RDD[String],
                                              logFn: String => Unit): Long = {
    // what phenotypes do we have? take the cartesian product vs samples
    val phenotypes = pRdd.map(_.getPhenotype)
      .distinct
      .cache
    val s2p = genotypedSamples.cartesian(phenotypes)
    
    // do a left outer join to identify missing phenotypes
    // then flatmap and create error messages
    val missing = s2p.leftOuterJoin(pRdd.map(p => (p.getSampleId, p.getPhenotype)))
      .flatMap(kv => kv match {
        case (sample, (phenotype, None)) => {
          Some((phenotype,
                "Missing observation of %s phenotype for sample %s.".format(phenotype,
                                                                            sample)))
        }
        case _ => {
          None
        }
      }).sortByKey()
        .map(kv => kv._2)
        .cache
    
    // log missing phenotype observations
    val missingCount = missing.count
    if (missingCount == 0) {
      log.info("Have a phenotype observation for every phenotype for all genotyped samples.")
    } else {
      val plural = if (missingCount == 1) {
        ""
      } else {
        "s"
      }
      logFn("Have %d missing phenotype observation%s:".format(missingCount, plural))
      missing.toLocalIterator.foreach(logFn)
    }
   
    // unpersist rdds
    missing.unpersist()
    phenotypes.unpersist()
 
    missingCount
  }

  def validate(pRdd: RDD[Phenotype],
               sRdd: RDD[Genotype],
               stringency: ValidationStringency) {
    // we skip validation if stringency is set to silent
    if (stringency != ValidationStringency.SILENT) {
      val (logFn, endFn) = stringency match {
        case ValidationStringency.STRICT => {
          ((s: String) => log.error(s),
           (c: Long) => throw new IllegalArgumentException("Exiting with %d errors.".format(c)))
        }
        case _ => {
          ((s: String) => log.warn(s),
           (c: Long) => log.warn("Had %d warnings.".format(c)))
        }
      }

      // validate sample ids
      val (mipCount, migCount, genotypedSamples) = validateSamples(pRdd, sRdd, logFn)

      // validate phenotypes across samples
      val missingCount = validatePhenotypes(pRdd, genotypedSamples, logFn)

      // unpersist genotyped samples rdd
      genotypedSamples.unpersist()

      // total error count
      val totalErrors = mipCount + migCount + missingCount
      if (totalErrors == 0) {
        log.info("No errors during genotype/phenotype validation.")
      } else {
        endFn(totalErrors)
      }
    }
  }

  def apply(file: String,
            sc: SparkContext): RDD[Phenotype] = {
    log.info("Loading phenotypes from %s.".format(file))

    // load and parse text file
    sc.textFile(file)
      .map(parseLine)
  }

  private[gnocchi] def parseLine(line: String): Phenotype = {
    val splits = line.split(",")
    assert(splits.length == 3, "Line was incorrectly formatted, did not contain sample, phenotype, hasPhenotype:\n%s".format(line))

    Phenotype.newBuilder()
      .setSampleId(splits(0).trim)
      .setPhenotype(splits(1).trim)
      .setHasPhenotype(splits(2).trim == "true")
      .build()
  }
}
