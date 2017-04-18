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
package net.fnothaft.gnocchi.sql

import net.fnothaft.gnocchi.rdd.association.Association
import net.fnothaft.gnocchi.rdd.genotype.GenotypeState
import net.fnothaft.gnocchi.rdd.phenotype.Phenotype
import org.apache.spark.SparkContext
import org.bdgenomics.utils.misc.Logging
//import net.fnothaft.gnocchi.transformations.{ Gs2variant, PairSamplesWithPhenotypes }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Column, DataFrame, Dataset, SQLContext }
import org.apache.spark.sql.functions._
import org.bdgenomics.formats.avro.{ Contig, Variant }

object GnocchiContext {

  implicit def gcFromSqlContext(sqlContext: SQLContext): GnocchiSqlContext =
    new GnocchiSqlContext(sqlContext)

  // Add GnocchiContext methods
  implicit def sparkContextToADAMContext(sc: SparkContext): GnocchiContext = new GnocchiContext(sc)
}

class GnocchiContext(@transient val sc: SparkContext) extends Serializable with Logging {

  def loadAndFilterGenotypes(genotypesPath: String,
                             adamDestination: String,
                             ploidy: Int,
                             mind: Double,
                             maf: Double,
                             geno: Double,
                             overwrite: Boolean): RDD[GenotypeState] = {

  }

  def loadPhenotypes(phenotypesPath: String,
                     phenoName: String,
                     oneTwo: Boolean,
                     includeCovariates: Boolean = false,
                     covarFile: Option[String] = None,
                     covarNames: Option[String] = None): RDD[Phenotype] = {

  }
}

class GnocchiSqlContext private[sql] (@transient sqlContext: SQLContext) extends Serializable {

  import sqlContext.implicits._

  def toGenotypeStateDataset(gtFrame: DataFrame, ploidy: Int): Dataset[GenotypeState] = {
    toGenotypeStateDataFrame(gtFrame, ploidy).as[GenotypeState]
  }


  def toGenotypeStateDataFrame(gtFrame: DataFrame, ploidy: Int, sparse: Boolean = false): DataFrame = {

    val filteredGtFrame = if (sparse) {
      // if we want the sparse representation, we prefilter
      val sparseFilter = (0 until ploidy).map(i => {
        gtFrame("alleles").getItem(i) !== "Ref"
      }).reduce(_ || _)
      gtFrame.filter(sparseFilter)
    } else {
      gtFrame
    }

    // generate expression
    val genotypeState = (0 until ploidy).map(i => {
      val c: Column = when(filteredGtFrame("alleles").getItem(i) === "Ref", 1).otherwise(0)
      c
    }).reduce(_ + _)

    val missingGenotypes = (0 until ploidy).map(i => {
      val c: Column = when(filteredGtFrame("alleles").getItem(i) === "NoCall", 1).otherwise(0)
      c
    }).reduce(_ + _)

    filteredGtFrame.select(filteredGtFrame("variant.contig.contigName").as("contig"),
      filteredGtFrame("variant.start").as("start"),
      filteredGtFrame("variant.end").as("end"),
      filteredGtFrame("variant.referenceAllele").as("ref"),
      filteredGtFrame("variant.alternateAllele").as("alt"),
      filteredGtFrame("sampleId"),
      genotypeState.as("genotypeState"),
      missingGenotypes.as("missingGenotypes"))
  }

  /**
   * Generates an RDD of observations that can be used to create or update VariantModels.
   *
   * @param genotypes RDD of GenotypeState objects
   * @param phenotypes RDD of Pheontype objects
   * @return Returns an RDD of arrays of observations (genotype + phenotype), keyed by variant
   */
  def generateObservations(genotypes: RDD[GenotypeState],
                           phenotypes: RDD[Phenotype]): RDD[(Variant, Array[(Double, Array[Double])])] = {
    // convert genotypes and phenotypes into observations
    val data = pairSamplesWithPhenotypes(genotypes, phenotypes)
    // data is RDD[((Variant, String), Iterable[(String, (GenotypeState,Phenotype))])]
    data.map(kvv => {
      val (varStr, genPhenItr) = kvv
      val (variant, phenoName) = varStr
      //.asInstanceOf[Array[(String, (GenotypeState, Phenotype[Array[Double]]))]]
      val obs = genPhenItr.map(gp => {
        val (str, (gs, pheno)) = gp
        val ob = (gs, pheno.value)
        ob
      }).toArray
      (variant, obs).asInstanceOf[(Variant, Array[(Double, Array[Double])])]
    })
  }

  def pairSamplesWithPhenotypes(rdd: RDD[GenotypeState],
                                phenotypes: RDD[Phenotype]): RDD[((Variant, String), Iterable[(String, (GenotypeState, Phenotype))])] = {
    rdd.keyBy(_.sampleId)
      // join together the samples with both genotype and phenotype entry
      .join(phenotypes.keyBy(_.sampleId))
      .map(kvv => {
        // unpack the entry of the joined rdd into id and actual info
        val (sampleid, gsPheno) = kvv
        // unpack the information into genotype state and pheno
        val (gs, pheno) = gsPheno

        // create contig and Variant objects and group by Variant
        // pack up the information into an Association object
        val variant = gs2variant(gs)
        ((variant, pheno.phenotype), (sampleid, gsPheno))
      }).groupByKey
  }

  def gs2variant(gs: GenotypeState): Variant = {
    val variant = new Variant()
    val contig = new Contig()
    contig.setContigName(gs.contig)
    variant.setContig(contig)
    variant.setStart(gs.start)
    variant.setEnd(gs.end)
    variant.setAlternateAllele(gs.alt)
    variant
  }
}

//object AuxEncoders {
//  implicit def associationEncoder: org.apache.spark.sql.Encoder[Association] = org.apache.spark.sql.Encoders.kryo[Association]
//  implicit def genotypeStateEncoder: org.apache.spark.sql.Encoder[GenotypeState] = org.apache.spark.sql.Encoders.kryo[GenotypeState]
//}
