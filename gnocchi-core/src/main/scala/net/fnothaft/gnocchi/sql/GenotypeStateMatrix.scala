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
package net.fnothaft.gnocchi.sql

import net.fnothaft.gnocchi.models.GenotypeState
import org.apache.spark.SparkContext._
import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.linalg.distributed.{ MatrixEntry, RowMatrix }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, Row }
import org.bdgenomics.formats.avro.Variant

object GenotypeStateMatrix extends Serializable with Logging {

  def apply(ds: Dataset[GenotypeState]): (RowMatrix, Map[String, Int]) = {

    // """
    // Creates a matrix where the rows are sites and the columns are samples.
    // The value at any given location is the genotype dose (Double) for a sample at that site. 
    // """
    // get sample id's and broadcast
    val df = ds.toDF()
    df.cache()
    val sampleIds = df.select(df("sampleId"))
      .distinct
      .rdd
      .map(r => r match {
        case Row(sampleId: String) => sampleId
      }).collect
      .zipWithIndex
      .toMap
    val rdd = ds.rdd
    val bcastIds = rdd.context.broadcast(sampleIds)
    val samples = sampleIds.size
    log.info("Have %d samples.".format(samples))

    // filter out reference calls and join against sample id's, then group by pos
    val samplesByVariant = rdd.flatMap(filterAndJoin(_, bcastIds.value))
      .groupByKey
      .cache
    val sites = samplesByVariant.count
    log.info("Have %d sites.".format(sites))

    // create matrix by mapping sites into rows
    val matrix = new RowMatrix(samplesByVariant.map(siteToRow(_, samples)), sites, samples)
    samplesByVariant.unpersist()

    // return matrix and sample id map
    (matrix, sampleIds)
  }

  /**
   * Filters out reference calls and joins against a broadcast map between
   * sample ID strings and sample indices.
   *
   * @param gt Genotype entry to process.
   * @param ids Map between string sample ID and sample index.
   * @return Optionally returns (variant, (sample id, called dosage)).
   */
  private[sql] def filterAndJoin(gt: GenotypeState,
                                 ids: Map[String, Int]): Option[(Variant, (Int, Double))] = {
    // did we have any alt calls? if so, join against the sample id, else filter
    if (gt.genotypeState != 0) {
      Some((gt.variant, (ids(gt.sampleId), gt.genotypeState.toDouble)))
    } else {
      None
    }
  }

  /**
   * Maps a site where genotypes were observed into a sparse vector.
   *
   * @param site A genotype vector. The key is the variant that is genotyped,
   *   and the iterator is a sparse mapping between the integer sample ID and
   *   allele count.
   * @param rowSize The number of samples genotyped.
   * @return Returns a sparse vector of genotype observations for this variant.
   */
  private def siteToRow(site: (Variant, Iterable[(Int, Double)]),
                        rowSize: Int): Vector = {
    val (_, obs) = site
    val os = obs.toSeq
    Vectors.sparse(rowSize, os)
  }
}
