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
import org.apache.spark.{ Logging, SparkContext }
import org.apache.spark.rdd.RDD

object LoadPhenotypes extends Serializable with Logging {

  def apply(file: String,
            sc: SparkContext): RDD[Phenotype] = {
    log.info("Loading phenotypes from %s.".format(file))

    // load and parse text file
    sc.textFile(file)
      .map(parseLine)
  }

  private[g2pilot] def parseLine(line: String): Phenotype = {
    val splits = line.split(",")
    assert(splits.length == 3, "Line was incorrectly formatted, did not contain sample, phenotype, hasPhenotype:\n%s".format(line))

    Phenotype.newBuilder()
      .setSampleId(splits(0).trim)
      .setPhenotype(splits(1).trim)
      .setHasPhenotype(splits(2).trim == "true")
      .build()
  }
}
