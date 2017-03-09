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
package net.fnothaft.gnocchi.clustering

import net.fnothaft.gnocchi.models.{ GenotypeState, ReducedDimension }
import net.fnothaft.gnocchi.sql.GenotypeStateMatrix
import org.apache.spark.mllib.linalg.{ DenseMatrix, Vector, Vectors }
import org.apache.spark.sql.{ Dataset, Row }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{ Genotype, GenotypeAllele, Variant }

object WideFlatPCA extends Serializable {

  def apply(ds: Dataset[GenotypeState], components: Int): Dataset[ReducedDimension] = {

    // get matrix
    val (matrix, sampleIds) = GenotypeStateMatrix(ds)

    // svd calculates:
    // A = U Σ Vt
    // A -> samples x genotypes
    // U -> samples x components
    // Σ -> components x components
    // V -> genotypes x components (Vt -> components x genotypes)
    //
    // the rotated PCA scores are given by:
    // T = U Σ
    // T = samples x components
    //
    // however, in our case, we are actually running svd on At, thus svd
    // is actually calculating:
    // At = V Σ Ut
    //
    // therefore, we must mentally alias the return values:
    // Vt -> Ut <- components x samples (returned as V --> samples x components)
    // U -> V <- don't care (but is genotypes x components)

    // run svd - computeU is false because U = V
    val svd = matrix.computeSVD(components, computeU = false)

    // alias V <-> U
    val uMat = svd.V

    // Σ is actually a diagonal matrix
    // note: this would be more efficient as a sparse matrix, but alas, our API
    // only supports dense/dense matrix multiplication
    val Σ = DenseMatrix.diag(svd.s)

    // multiply U and Σ
    val uΣ = uMat.multiply(Σ)

    // invert sampleId map
    val idToSample = sampleIds.map(v => (v._2, v._1))

    // add back ids and return
    import ds.sqlContext.implicits._
    ds.sqlContext.createDataset(uΣ.toArray
      .grouped(components)
      .toSeq
      .zipWithIndex
      .map(kv => {
        val (components, index) = kv
        ReducedDimension(idToSample(index), components)
      }))
  }
}
