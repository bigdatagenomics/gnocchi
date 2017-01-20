
/**
 * Copyright 2016 Taner Dagdelen
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
package net.fnothaft.gnocchi.transformations

import net.fnothaft.gnocchi.models.GenotypeState
import org.bdgenomics.formats.avro.{ Contig, Variant }

object Gs2variant {

  def apply(gs: GenotypeState): Variant = {
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
