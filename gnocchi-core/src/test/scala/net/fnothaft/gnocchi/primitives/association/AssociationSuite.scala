package net.fnothaft.gnocchi.primitives.association

import net.fnothaft.gnocchi.GnocchiFunSuite

class AssociationSuite extends GnocchiFunSuite{
  sparkTest("LinearAssociation creation works.") {
    val assoc = LinearAssociation(ssDeviations = 0.5,
      ssResiduals = 0.5,
      geneticParameterStandardError = 0.5,
      tStatistic = 0.5,
      residualDegreesOfFreedom = 2,
      pValue = 0.5,
      weights = List(0.5, 0.5),
      numSamples = 10)
    assert(assoc.isInstanceOf[LinearAssociation], "Cannot create LinearAssociation")
  }

  sparkTest("LogisticAssociation creation works.") {
    val assoc = LogisticAssociation(geneticParameterStandardError = 0.5,
      pValue = 0.5,
      weights = List(0.5, 0.5),
      numSamples = 10)
    assert(assoc.isInstanceOf[LogisticAssociation], "Cannot create LogisticAssociation")
  }
}
