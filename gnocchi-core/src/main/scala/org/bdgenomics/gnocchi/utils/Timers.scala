package org.bdgenomics.gnocchi.utils

import org.bdgenomics.utils.instrumentation.Metrics

object Timers extends Metrics {

  // Filter methods
  val FilterSamples = timer("Filter Samples")
  val FilterVariants = timer("Filter Variants")
  val RecodeMajorAllele = timer("Recode Major Allele")

  // Load methods
  val LoadGenotypes = timer("Load Genotypes")
  val LoadGnocchiGenotypes = timer("Load Gnocchi Formatted Genotypes")
  val LoadCalledVariantDSFromVariantContextRDD = timer("Load Gnocchi Format from ADAM Format")
  val LoadPhenotypes = timer("Load Phenotypes")
  val LoadGnocchiModel = timer("Load in a Gnocchi Model")

  // Save Methods
  val SaveAssociations = timer("Save Associations")

  // Regression Methods
  val CreateModelAndAssociations = timer("Create Model and Associations")

  val CreatAssociationsDataset = timer("Create Associations Dataset")
}
