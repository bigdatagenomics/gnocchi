package org.bdgenomics.gnocchi.api.java

import org.apache.spark.SparkContext
import org.bdgenomics.gnocchi.cli.RegressPhenotypes
import org.bdgenomics.gnocchi.sql.GnocchiSession

import scala.collection.JavaConversions._

object JavaRegressPhenotypes {
  var gs: GnocchiSession = null

  def generate(gs: GnocchiSession) { this.gs = gs }

  def apply(args: java.util.List[java.lang.String]) = {
    RegressPhenotypes(asScalaBuffer(args).toArray).run(this.gs.sc)
  }
}

