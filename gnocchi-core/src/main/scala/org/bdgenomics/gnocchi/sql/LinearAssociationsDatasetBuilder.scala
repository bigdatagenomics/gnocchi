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
import org.apache.spark.sql.functions.col
import org.apache.hadoop.fs.Path
import org.bdgenomics.gnocchi.algorithms.siteregression.LinearRegressionResults
import org.bdgenomics.gnocchi.models.LinearGnocchiModel
import org.bdgenomics.gnocchi.primitives.association.LinearAssociationBuilder
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.sql.GnocchiSession._

/**
 * The [[LinearAssociationsDatasetBuilder]] object is used to store an "in flux" set of associations
 * that are being incrementally built across multiple distributed datasets. The [[Dataset]] of
 * [[LinearAssociationBuilder]] objects store partially built
 * [[org.bdgenomics.gnocchi.primitives.association.LinearAssociation]] objects. This object is a
 * wrapper around the collection of associations that stores relevant metadata necessary for
 * tracking the merging state.
 *
 * This object is intended to be used as follows. Researcher #1 builds a GnocchiModel on Dataset #1
 * and sends the model to Researcher #2. Researcher #2 builds a second GnocchiModel on Dataset #2
 * and merges his GnocchiModel with the GnocchiModel given to him by Researcher #1. Researcher #2
 * then passes the merged GnocchiModel to the [[LinearAssociationsDatasetBuilder()]] method along
 * with Dataset #2. The resulting [[LinearAssociationsDatasetBuilder]] then contains the merged
 * GnocchiModel, and half-way built LinearAssociations. To complete the exchange, Researcher #2
 * serializes the [[LinearAssociationsDatasetBuilder]] and sends it to Researcher #1, who then calls
 * the [[addNewData()]] method on the deserialized object, with Dataset #1. At that point, the
 * merged Gnocchi Model has seen all the data points used to construct the merged Gnocchi Model, and
 * the exchange is complete. The resulting associations are equivalent to a model built of the
 * superset of Dataset #1 and #2, but completed in separated environments.
 *
 * @param linearAssociationBuilders
 * @param phenotypeNames
 * @param covariatesNames
 * @param sampleUIDs
 * @param numSamples
 * @param allelicAssumption
 */
case class LinearAssociationsDatasetBuilder(@transient linearAssociationBuilders: Dataset[LinearAssociationBuilder],
                                            phenotypeNames: String,
                                            covariatesNames: List[String],
                                            sampleUIDs: Set[String],
                                            numSamples: Int,
                                            allelicAssumption: String) {

  //  val fullyBuilt = has seen genotypes for all individual ids used to build the model

  import linearAssociationBuilders.sparkSession.implicits._

  /**
   * Joins a [[Dataset]] of [[CalledVariant]] with a [[Dataset]] of [[LinearAssociationBuilder]] and updates
   * the [[LinearAssociationBuilder]] objects. It then constructs a new [[LinearAssociationsDatasetBuilder]]
   * from the resulting merged associations.
   *
   * ToDo: Make sure individuals in new data were part of the originally created model
   *
   * @param newGenotypeData new [[Dataset]] of [[CalledVariant]] to apply the model to and get the sum of squared residuals for
   * @param newPhenotypeData new [[PhenotypesContainer]] object containing the phenotype data corresponding to the
   *                         newGenotypeData param
   * @return a new [[LinearAssociationsDatasetBuilder]] which is updated with the added data
   */
  def addNewData(newGenotypeData: GenotypeDataset,
                 newPhenotypeData: PhenotypesContainer): LinearAssociationsDatasetBuilder = {
    LinearAssociationsDatasetBuilder(
      linearAssociationBuilders
        .joinWith(newGenotypeData.genotypes, linearAssociationBuilders("model.uniqueID") === newGenotypeData.genotypes("uniqueID"))
        .map { case (builder, newVariant) => builder.addNewData(newVariant, newPhenotypeData.phenotypes.value, allelicAssumption) },
      phenotypeNames,
      covariatesNames,
      sampleUIDs,
      numSamples,
      allelicAssumption)
  }

  /**
   * Save the associations that are stored in this object to the specified location.
   *
   * @param outPath Path to the output location.
   */
  def saveAssociations(outPath: String): Unit = {
    val sc = linearAssociationBuilders.sparkSession.sparkContext
    sc.saveAssociations(linearAssociationBuilders.map(_.association),
      outPath,
      saveAsText = true)
  }

  /**
   * Serialize this object, and save it to the specified location.
   *
   * @param saveTo path to save this serialized object to.
   */
  def save(saveTo: String): Unit = {
    val metadataPath = new Path(saveTo + "/metaData")

    val metadata_fs = metadataPath.getFileSystem(linearAssociationBuilders.sparkSession.sparkContext.hadoopConfiguration)
    val metadata_oos = new ObjectOutputStream(metadata_fs.create(metadataPath))

    metadata_oos.writeObject(this)
    metadata_oos.close()

    linearAssociationBuilders.write.parquet(saveTo + "/LinearAssociationBuilders")
  }
}

/**
 * Singleton object, used to construct a [[LinearAssociationsDatasetBuilder]]
 */
object LinearAssociationsDatasetBuilder {

  /**
   * Package up a [[LinearGnocchiModel]] with a set of [[LinearAssociationBuilder]] objects into a
   * [[LinearAssociationsDatasetBuilder]]
   *
   * @param model The [[LinearGnocchiModel]] to place in the dataset builder
   * @param genotypeData The genotype data to apply the model to
   * @param phenotypeData The phenotype data to apply the model to
   * @return A [[LinearAssociationsDatasetBuilder]] that contains partially built LinearAssociation
   *         objects
   */
  def apply(model: LinearGnocchiModel,
            genotypeData: GenotypeDataset,
            phenotypeData: PhenotypesContainer): LinearAssociationsDatasetBuilder = {

    import model.variantModels.sparkSession.implicits._

    val linearAssociationBuilders = model.variantModels.joinWith(genotypeData.genotypes, model.variantModels("uniqueID") === genotypeData.genotypes("uniqueID"))
      .map {
        case (model, genotype) => {
          LinearAssociationBuilder(model, model.createAssociation(genotype, phenotypeData.phenotypes.value, genotypeData.allelicAssumption))
        }
      }
    LinearAssociationsDatasetBuilder(linearAssociationBuilders,
      model.phenotypeName,
      model.covariatesNames,
      model.sampleUIDs,
      model.numSamples,
      model.allelicAssumption)
  }
}