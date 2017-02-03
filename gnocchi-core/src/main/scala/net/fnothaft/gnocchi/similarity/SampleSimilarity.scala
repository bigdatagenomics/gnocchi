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
package net.fnothaft.gnocchi.similarity

import net.fnothaft.gnocchi.models.{ GenotypeState, Similarity }
import net.fnothaft.gnocchi.sql.GenotypeStateMatrix
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import scala.collection.JavaConverters._

object SampleSimilarity extends Serializable {

  /**
   * Computes the pairwise similarity of all samples in a genotyped dataset.
   *
   * @param rdd The RDD of genotypes.
   * @param similarityThreshold A parameter for approximating the similarity
   *    comparison. To ensure an exact comparison, set to 0.0.
   */
  def apply(ds: Dataset[GenotypeState], similarityThreshold: Double = 0.5): RDD[Similarity] = {

    // get matrix
    val (matrix, sampleIds) = GenotypeStateMatrix(ds)

    // compute similarity
    println("Computing similarity with threshold %f.".format(similarityThreshold))
    val similarity = matrix.columnSimilarities(similarityThreshold)

    // reverse sample ID map and broadcast
    val idToSample = sampleIds.map(v => (v._2.toLong, v._1))
    val bcastSamples = similarity.entries.context.broadcast(idToSample)

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
    Similarity(idToSample(entry.i),
      idToSample(entry.j),
      entry.value)
  }
}
