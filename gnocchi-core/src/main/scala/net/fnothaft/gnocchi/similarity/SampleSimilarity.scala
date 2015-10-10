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
package net.fnothaft.gnocchi.similarity

import net.fnothaft.gnocchi.avro.Similarity
import org.apache.spark.SparkContext._
import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.linalg.distributed.{ MatrixEntry, RowMatrix }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{ Genotype, GenotypeAllele, Variant }
import scala.collection.JavaConverters._

object SampleSimilarity extends Serializable with Logging {
  
  /**
   * Computes the pairwise similarity of all samples in a genotyped dataset.
   *
   * @param rdd The RDD of genotypes.
   * @param similarityThreshold A parameter for approximating the similarity
   *    comparison. To ensure an exact comparison, set to 0.0.
   */
  def apply(rdd: RDD[Genotype], similarityThreshold: Double = 0.5): RDD[Similarity] = {

    // get sample id's and broadcast
    val sampleIds = rdd.map(_.getSampleId)
      .distinct
      .collect
      .zipWithIndex
      .toMap
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

    // compute similarity
    log.info("Computing similarity with threshold %f.".format(similarityThreshold))
    val similarity = matrix.columnSimilarities(similarityThreshold)

    // reverse sample ID map and broadcast
    val idToSample = sampleIds.map(v => (v._2.toLong, v._1))
    val bcastSamples = rdd.context.broadcast(idToSample)

    // map matrix entries into similarities and return
    similarity.entries
      .map(entryToSimilarity(_, bcastSamples.value))
  }

  /**
   * Maps an element from a similarity matrix into a sample similarity element.
   *
   * @param entry The matrix entry to map.
   * @param idToSample A mapping between numeric sample IDs and the string sample ID.
   * @return Returns a similarity element.
   */
  private def entryToSimilarity(entry: MatrixEntry,
                                idToSample: Map[Long, String]): Similarity = {
    Similarity.newBuilder
      .setFrom(idToSample(entry.i))
      .setTo(idToSample(entry.j))
      .setSimilarity(entry.value)
      .build()
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

  /**
   * Filters out reference calls and joins against a broadcast map between
   * sample ID strings and sample indices.
   *
   * @param gt Genotype entry to process.
   * @param ids Map between string sample ID and sample index.
   * @return Optionally returns (variant, (sample id, called dosage)).
   */
  private[similarity] def filterAndJoin(gt: Genotype,
                                        ids: Map[String, Int]): Option[(Variant, (Int, Double))] = {
    // how many times did we observe the alt allele here?
    val altCount = gt.getAlleles
      .asScala
      .count(_ == GenotypeAllele.Alt)
    
    // did we have any alt calls? if so, join against the sample id, else filter
    if (altCount > 0) {
      Some((gt.getVariant, (ids(gt.getSampleId), altCount.toDouble)))
    } else {
      None
    }
  }
}
