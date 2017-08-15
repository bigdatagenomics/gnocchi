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
package net.fnothaft.gnocchi.rdd.genotype

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{ Contig, Variant }

/**
 * Genotype model that stores all relevant genotype data.
 *
 * @todo add validation for ploidy being two and only single ref and alt values
 *
 * @param contigName identifier for genotype, VCFtoAdam initially sets this to be only the chromosome value, but in
 *               RegressPhenotypes.loadGenotypes() the contig identifier is changed to the form, CHROM_POS_ALT, which
 *               uniquely categorizes a single base
 * @param start start position of the contig
 * @param end end position of the contig
 * @param ref reference value at the contig location
 * @param alt alternate value at the contig location. Currently genotype state response is 1 when the genotype
 *            matches the alt value.
 * @param sampleId Patient Id
 * @param genotypeState Sum of matching responses against reference genome. Currently only logically supports values of
 *                      0, 1 or 2, meaning ploidy is 2 and there is only one ref value and one alt value.
 * @param missingGenotypes Number of genotypes that are marked as missing
 */
case class GenotypeState(contigName: String,
                         start: Long,
                         end: Long,
                         ref: String,
                         alt: String,
                         sampleId: String,
                         genotypeState: Int,
                         missingGenotypes: Int,
                         phaseSetId: Int = 0) {

  def referenceAllele: (ReferenceRegion, String) = {
    (ReferenceRegion(contigName, start, end), alt)
  }

  def variant: Variant = {
    Variant.newBuilder()
      .setContigName(contigName)
      .setStart(start)
      .setEnd(end)
      .setReferenceAllele(ref)
      .setAlternateAllele(alt)
      .build()
  }

  def tempVariantId = {
    val posString = start.toString
    s"$posString" + "_$alt"
  }
}
