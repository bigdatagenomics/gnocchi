package net.fnothaft.gnocchi.primitives.association

case class LinearAssociation(ssDeviations: Double,
                             ssResiduals: Double,
                             geneticParameterStandardError: Double,
                             tStatistic: Double,
                             residualDegreesOfFreedom: Int,
                             pValue: Double,
                             weights: List[Double],
                             numSamples: Int) extends Association