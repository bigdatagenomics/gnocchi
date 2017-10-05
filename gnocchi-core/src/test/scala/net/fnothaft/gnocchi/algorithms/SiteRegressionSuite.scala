package net.fnothaft.gnocchi.algorithms

import net.fnothaft.gnocchi.GnocchiFunSuite
import net.fnothaft.gnocchi.algorithms.siteregression.{ AdditiveLinearRegression, AdditiveLogisticRegression, DominantLinearRegression, DominantLogisticRegression }
import org.mockito.Mockito._

class SiteRegressionSuite extends GnocchiFunSuite {

  sparkTest("SiteRegression.Dominant.clipOrKeepState should map 0.0 to 0.0 and everything else to 1.0") {
    assert(DominantLinearRegression.clipOrKeepState(2.0) == 1.0,
      "SiteRegression.Dominant.clipOrKeepState does not correctly map 2.0 to 1.0")
    assert(DominantLinearRegression.clipOrKeepState(1.0) == 1.0,
      "SiteRegression.Dominant.clipOrKeepState does not correctly map 1.0 to 1.0")
    assert(DominantLinearRegression.clipOrKeepState(0.0) == 0.0,
      "SiteRegression.Dominant.clipOrKeepState does not correctly map 0.0 to 0.0")
    assert(DominantLogisticRegression.clipOrKeepState(2.0) == 1.0,
      "SiteRegression.Dominant.clipOrKeepState does not correctly map 2.0 to 1.0")
    assert(DominantLogisticRegression.clipOrKeepState(1.0) == 1.0,
      "SiteRegression.Dominant.clipOrKeepState does not correctly map 1.0 to 1.0")
    assert(DominantLogisticRegression.clipOrKeepState(0.0) == 0.0,
      "SiteRegression.Dominant.clipOrKeepState does not correctly map 0.0 to 0.0")
  }

  sparkTest("SiteRegression.Additive.clipOrKeepState should be an identity map") {
    assert(AdditiveLinearRegression.clipOrKeepState(2.0) == 2.0,
      "SiteRegression.Additive.clipOrKeepState does not correctly map 2.0 to 2.0")
    assert(AdditiveLinearRegression.clipOrKeepState(1.0) == 1.0,
      "SiteRegression.Additive.clipOrKeepState does not correctly map 1.0 to 1.0")
    assert(AdditiveLinearRegression.clipOrKeepState(0.0) == 0.0,
      "SiteRegression.Additive.clipOrKeepState does not correctly map 0.0 to 0.0")
    assert(AdditiveLogisticRegression.clipOrKeepState(2.0) == 2.0,
      "SiteRegression.Additive.clipOrKeepState does not correctly map 2.0 to 2.0")
    assert(AdditiveLogisticRegression.clipOrKeepState(1.0) == 1.0,
      "SiteRegression.Additive.clipOrKeepState does not correctly map 1.0 to 1.0")
    assert(AdditiveLogisticRegression.clipOrKeepState(0.0) == 0.0,
      "SiteRegression.Additive.clipOrKeepState does not correctly map 0.0 to 0.0")
  }

  ignore("SiteRegression.Recessive.clipOrKeepState should map 2.0 to 1.0 and everything else to 0.0") {
    //    assert(RecessiveLinearRegression.clipOrKeepState(2.0) == 1.0,
    //      "SiteRegression.Dominant.clipOrKeepState does not correctly map 2.0 to 1.0")
    //    assert(RecessiveLinearRegression.clipOrKeepState(1.0) == 0.0,
    //      "SiteRegression.Dominant.clipOrKeepState does not correctly map 1.0 to 0.0")
    //    assert(RecessiveLinearRegression.clipOrKeepState(0.0) == 0.0,
    //      "SiteRegression.Dominant.clipOrKeepState does not correctly map 0.0 to 0.0")
  }
}
