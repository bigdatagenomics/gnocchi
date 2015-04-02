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

import net.fnothaft.g2pilot.avro.Phenotype
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext._
import org.apache.spark.{ Logging, SparkContext }
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.BroadcastRegionJoin
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.cli._
import org.bdgenomics.utils.parquet.rdd.BDGParquetContext._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }
import parquet.avro.AvroReadSupport
import parquet.hadoop.ParquetInputFormat
import parquet.hadoop.util.ContextUtil

object G2Pilot extends BDGCommandCompanion {
  val commandName = "g2pilot"
  val commandDescription = "Pilot code for computing genotype/phenotype associations using ADAM"

  def apply(cmdLine: Array[String]) = {
    new G2Pilot(Args4j[G2PilotArgs](cmdLine))
  }
}

class G2PilotArgs extends Args4jBase {
  @Argument(required = true, metaVar = "GENOTYPES", usage = "The genotypes to process.", index = 0)
  var genotypes: String = null

  @Argument(required = true, metaVar = "PHENOTYPES", usage = "The phenotypes to process.", index = 1)
  var phenotypes: String = null

  @Args4jOption(required = false, name = "-regions", usage = "The regions to filter genotypes by.")
  var regions: String = null
}

class G2Pilot(protected val args: G2PilotArgs) extends BDGSparkCommand[G2PilotArgs] {
  val companion = G2Pilot

  def run(sc: SparkContext, job: Job) {
    // load in genotype data
    val genotypes = sc.loadGenotypes(args.genotypes)

    // load in phenotype data
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[Phenotype]])
    val phenotypes = sc.newAPIHadoopFile(args.phenotypes,
                                      classOf[ParquetInputFormat[Phenotype]],
                                      classOf[Void],
                                      classOf[Phenotype],
                                      ContextUtil.getConfiguration(job))
                                        .map(kv => kv._2)


    // if we have regions, then load and filter
    val filteredGenotypes = if (args.regions != null) {
      // load in regions
      val features = sc.loadFeatures(args.regions)

      // key both genotype and feature RDDs by region and join
      // then drop the feature
      BroadcastRegionJoin.partitionAndJoin(features.keyBy(ReferenceRegion(_)),
                                           genotypes.keyBy(gt => {
                                             val v = gt.getVariant
                                             ReferenceRegion(v.getContig
                                               .getContigName,
                                                             v.getStart,
                                                             v.getEnd)
                                           })).map(kv => kv._2)
    } else {
      genotypes
    }

    // key both genotypes and phenotypes by the sample and join
    val g2p = filteredGenotypes.keyBy(_.getSampleId)
      .join(phenotypes.keyBy(_.getSampleId))

    // for now, let's just count the number of g2p pairs that we have
    println("Have " + g2p.count() + " genotype-to-phenotype pairs.")
  }
}
