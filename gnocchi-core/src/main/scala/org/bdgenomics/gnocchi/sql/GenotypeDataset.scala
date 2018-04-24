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
package org.bdgenomics.gnocchi.sql

import java.io.ObjectOutputStream

import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.apache.hadoop.fs.Path

/**
 * Use this object as a container for genomic data stored in [[CalledVariant]] objects. This object
 * is meant to correspond directly to a genomic dataset that exists in a single location. Building
 * joint models over multiple instances of this object should be possible.
 *
 * @param genotypes the dataset of CalledVariant objects associated with this dataset
 * @param datasetUID Unique Identifier for this dataset of genomic variants. A good example for this
 *                   would be the dbGaP study accession ID
 */
case class GenotypeDataset(@transient genotypes: Dataset[CalledVariant],
                           datasetUID: String,
                           allelicAssumption: String,
                           sampleUIDs: Set[String]) extends Serializable {

  /**
   * Transform the allelic assumption of this GenotypeDataset to a newly specified allelic assumption
   *
   * @param newAllelicAssumption The new allelic assumption of the resulting [[GenotypeDataset]]
   * @return the new [[GenotypeDataset]]
   */
  def transformAllelicAssumption(newAllelicAssumption: String): GenotypeDataset = {
    GenotypeDataset(genotypes, datasetUID, newAllelicAssumption, sampleUIDs)
  }

  /**
   * Save this object to a specified location.
   *
   * @param saveTo the path to save this object to
   */
  def save(saveTo: String): Unit = {
    val metadataPath = new Path(saveTo + "/metaData")

    val metadata_fs = metadataPath.getFileSystem(genotypes.sparkSession.sparkContext.hadoopConfiguration)
    val metadata_oos = new ObjectOutputStream(metadata_fs.create(metadataPath))

    metadata_oos.writeObject(this)
    metadata_oos.close()

    genotypes.write.parquet(saveTo + "/genotypes")
  }
}
