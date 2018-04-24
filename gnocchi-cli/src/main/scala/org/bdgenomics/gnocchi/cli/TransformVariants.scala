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
package org.bdgenomics.gnocchi.cli

import org.apache.spark.SparkContext
import org.bdgenomics.gnocchi.sql.GnocchiSession._
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.gnocchi.sql.GenotypeDataset
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object TransformVariants extends BDGCommandCompanion {
  val commandName = "transformVariants"
  val commandDescription = "Transform vcf or ADAM variant files to Gnocchi Format."

  def apply(cmdLine: Array[String]) = {
    new TransformVariants(Args4j[TransformVariantsArgs](cmdLine))
  }
}

class TransformVariantsArgs extends Args4jBase {
  @Argument(required = true, metaVar = "INPUT", usage = "The variants file to convert (e.g., .vcf, .vcf.gz, .vcf.bgzf, .vcf.bgz). If extension is not detected, ADAM Parquet is assumed.", index = 0)
  var inputPath: String = null

  @Argument(required = true, metaVar = "OUTPUT", usage = "Location to write GNOCCHI variants data.", index = 1)
  var outputPath: String = null

  @Args4jOption(required = false, name = "-datasetName", usage = "Unique identifier for the dataset that will be converted.")
  var datasetUID = ""

  @Args4jOption(required = false, name = "-allelicAssumption", usage = "Allelic assumption to use for dataset. One of [ADDITIVE, DOMINANT, RECESSIVE]. Default is ADDITIVE.")
  var allelicAssumption = "ADDITIVE"

  @Args4jOption(required = false, name = "-vcfDirectory", usage = "Set flag if the input path is a directory containing multiple VCF files.")
  var vcfDirectory = false

  @Args4jOption(required = false, name = "-saveADAMParquet", usage = "Set flag to save the ADAM Formatted VariantContextRDD. Saves to OUTPUT/adam")
  var saveAdamParquet = false

  @Args4jOption(required = false, name = "-variantContextParquet", usage = "Is the input ADAM Formatted VariantContextRDD?")
  var variantContextParquet = false
}

class TransformVariants(protected val args: TransformVariantsArgs) extends BDGSparkCommand[TransformVariantsArgs] {
  val companion = TransformVariants

  def run(sc: SparkContext) {

    val ac = new ADAMContext(sc)

    val rawGenotypes = if (args.vcfDirectory || sc.isVcfExt(args.inputPath)) {
      val variantContextRDD = ac.loadVcf(args.inputPath)
      if (args.saveAdamParquet) { variantContextRDD.saveAsParquet(args.outputPath + "/adam") }
      sc.wrapAdamVariantContextRDD(variantContextRDD, args.datasetUID, args.allelicAssumption)
    } else if (args.variantContextParquet) {
      sc.loadAdamVariantContextRDD(args.inputPath, args.datasetUID, args.allelicAssumption)
    } else {
      sc.loadGenotypes(args.inputPath, args.datasetUID, args.allelicAssumption, adamFormat = true)
    }

    if (args.saveAdamParquet) {
      rawGenotypes.save(args.outputPath + "/gnocchi")
    } else {
      rawGenotypes.save(args.outputPath)
    }
  }
}
