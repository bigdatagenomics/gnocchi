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

package org.bdgenomics.gnocchi.api.java.core

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.sql.{ GenotypeDataset, GnocchiSession, PhenotypesContainer }

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConverters._

object JavaGnocchiSession {
  // convert to and from java/scala implementation
  implicit def fromGnocchiSession(gs: GnocchiSession): JavaGnocchiSession = new JavaGnocchiSession(gs)
  implicit def toGnocchiSession(jgs: JavaGnocchiSession): GnocchiSession = jgs.gs
}

/**
 * The JavaGnocchiSession provides java-friendly functions on top of GnocchiSession.
 *
 * @param gs The GnocchiSession to wrap.
 */
class JavaGnocchiSession(val gs: GnocchiSession) extends Serializable {

  /**
   * @return Returns the Gnocchi Spark Context associated with this Java Gnocchi Session.
   */
  def getSparkContext: JavaSparkContext = new JavaSparkContext(gs.sc)

  /**
   * Returns a filtered Dataset of CalledVariant objects, where all values with
   * fewer samples than the mind threshold are filtered out.
   *
   * @param genotypes The Dataset of CalledVariant objects to filter on
   * @param mind The percentage threshold of samples to have filled in; values
   *             with fewer samples will be removed in this operation.
   * @param ploidy The number of sets of chromosomes
   *
   * @return Returns an updated Dataset with values removed, as specified by the
   *         filtering
   */
  def filterSamples(genotypes: Dataset[CalledVariant], mind: java.lang.Double, ploidy: java.lang.Double): Dataset[CalledVariant] = {
    gs.filterSamples(genotypes, mind, ploidy)
  }

  /**
   * Returns a filtered Dataset of CalledVariant objects, where all variants with
   * values less than the specified geno or maf threshold are filtered out.
   *
   * @param genotypes The Dataset of CalledVariant objects to filter on
   * @param geno The percentage threshold for geno values for each CalledVariant
   *             object
   * @param maf The percentage threshold for Minor Allele Frequency for each
   *            CalledVariant object
   *
   * @return Returns an updated Dataset with values removed, as specified by the
   *         filtering
   */
  def filterVariants(genotypes: Dataset[CalledVariant], geno: java.lang.Double, maf: java.lang.Double): Dataset[CalledVariant] = {
    gs.filterVariants(genotypes, geno, maf)
  }

  /**
   * Returns a modified Dataset of CalledVariant objects, where any value with a
   * maf > 0.5 is recoded. The recoding is specified as flipping the referenceAllele
   * and alternateAllele when the frequency of alt is greater than that of ref.
   *
   * @param genotypes The Dataset of CalledVariant objects to recode
   *
   * @return Returns an updated Dataset that has been recoded
   */
  def recodeMajorAllele(genotypes: Dataset[CalledVariant]): Dataset[CalledVariant] = {
    gs.recodeMajorAllele(genotypes)
  }

  /**
   * @note currently this does not enforce that the uniqueID is, in fact, unique across the dataset. Checking uniqueness
   *       would require a shuffle, which adds overhead that might not be necessary right now.
   *
   * @param genotypesPath A string specifying the location in the file system of the genotypes file to load in.
   * @return a [[Dataset]] of [[CalledVariant]] objects loaded from a vcf file
   */
  def loadGenotypes(genotypesPath: java.lang.String, datasetUID: java.lang.String, allelicAssumption: java.lang.String): GenotypeDataset = {
    gs.loadGenotypes(genotypesPath, datasetUID, allelicAssumption)
  }

  /**
   * Returns a map of phenotype name to phenotype object, which is loaded from
   * a file, specified by phenotypesPath
   *
   * @param phenotypesPath A string specifying the location in the file system
   *                       of the phenotypes file to load in.
   * @param primaryID The primary sample ID
   * @param phenoName The primary phenotype
   * @param delimiter The delimiter used in the input file
   * @param covarPath Optional parameter specifying the location in the file
   *                  system of the covariants file
   * @param covarNames Optional paramter specifying the names of the covariants
   *                   detailed in the covariants file
   * @param covarDelimiter The delimiter used in the covariants file
   *
   * @return A Map of phenotype name to Phenotype object
   */

  def loadPhenotypes(phenotypesPath: java.lang.String,
                     primaryID: java.lang.String,
                     phenoName: java.lang.String,
                     delimiter: java.lang.String,
                     covarPath: java.lang.String,
                     covarNames: java.util.ArrayList[java.lang.String],
                     covarDelimiter: java.lang.String = "\t",
                     missing: java.util.ArrayList[java.lang.String] = new java.util.ArrayList[String](List("-9").asJava)): PhenotypesContainer = {

    // Convert python compatible nullable types to scala options                   
    val covarPathOption = if (covarPath == null) {
      None
    } else {
      Some(covarPath)
    }

    // Convert python compatible nullable types to scala options                   
    val covarNamesOption = if (covarNames == null) {
      None
    } else {
      val covarNamesList = asScalaBuffer(covarNames).toList
      Some(covarNamesList)
    }

    val missingList = asScalaBuffer(missing).toList

    gs.loadPhenotypes(phenotypesPath,
      primaryID,
      phenoName,
      delimiter,
      covarPathOption,
      covarNamesOption,
      covarDelimiter,
      missingList)
  }

  /**
   * Returns a specific Phenotype object given the phenotypeMap and key
   *
   * @param phenotypeMap Map of phenotype name to Phenotype object
   * @param key String of the Phenotype name to access
   *
   * @return The Phenotype corresponding to the input key
   */
  def getPhenotypeByKey(phenotypeMap: Map[java.lang.String, Phenotype], key: java.lang.String): Phenotype = {
    phenotypeMap(key)
  }
}
