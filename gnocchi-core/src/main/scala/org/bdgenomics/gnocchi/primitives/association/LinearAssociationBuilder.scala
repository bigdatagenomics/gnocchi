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
package org.bdgenomics.gnocchi.primitives.association

import breeze.linalg.{ DenseMatrix, DenseVector }
import org.bdgenomics.gnocchi.algorithms.siteregression.LinearSiteRegression
import org.bdgenomics.gnocchi.models.variant.LinearVariantModel
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant

import scala.collection.immutable.Map

/**
 * An object used to store the incremental results of applying a pre-built model to a number of
 * datasets. This object both stores the model that will be applied to each constituent dataset, and
 * the partially built associations of all datasets that the model has been applied to up to this
 * point.
 *
 * @param model a fixed pre-built model that does not change over the course of adding new data to
 *              this builder
 * @param association the current state of the association in its partially-built form
 */
case class LinearAssociationBuilder(model: LinearVariantModel,
                                    association: LinearAssociation) {

  /**
   * Applies the model to a [[CalledVariant]] and merges the resulting new association with the
   * existing association stored in this object. Then it returns a new [[LinearAssociationBuilder]]
   * with the updated [[LinearAssociation]]
   *
   * @todo Make this method private and only accessible to the
   *       [[org.bdgenomics.gnocchi.sql.LinearAssociationsDatasetBuilder]] object. We shouldn't need
   *       to add data outside of that object.
   *
   * @param genotype The [[CalledVariant]] holding the genotype data to add to the association
   * @param phenotypes The phenotype data corresponding to the genotype data
   * @param allelicAssumption the allelic assumption to use in this association
   * @return
   */
  def addNewData(genotype: CalledVariant,
                 phenotypes: Map[String, Phenotype],
                 allelicAssumption: String): LinearAssociationBuilder = {

    val (x, y) = LinearSiteRegression.prepareDesignMatrix(genotype, phenotypes, allelicAssumption)

    val xTx_shaped = new DenseMatrix(model.numPredictors, model.numPredictors, model.xTx)
    val beta = new DenseVector(model.weights.toArray)

    val (genoSE, t, pValue, ssResiduals) =
      LinearSiteRegression.calculateSignificance(
        x,
        y,
        beta,
        xTx_shaped,
        Option(association.ssResiduals),
        Option(association.numSamples))

    // I don't like having the num samples updated here...
    val newAssociation =
      LinearAssociation(
        model.uniqueID,
        model.chromosome,
        model.position,
        model.numSamples + x.rows,
        pValue,
        genoSE,
        ssResiduals,
        t)

    LinearAssociationBuilder(model, newAssociation)
  }
}
