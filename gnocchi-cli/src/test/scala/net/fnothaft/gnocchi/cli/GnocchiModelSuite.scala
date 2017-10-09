///**
// * Licensed to Big Data Genomics (BDG) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The BDG licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// */
//
//package net.fnothaft.gnocchi.cli
//
//import java.io.File
//
//import net.fnothaft.gnocchi.GnocchiFunSuite
//import java.nio.file.{ Files, Paths }
//
//import net.fnothaft.gnocchi.models.linear.AdditiveLinearGnocchiModel
//import net.fnothaft.gnocchi.models.variant.VariantModel
//import org.scalatest.Ignore
//
//class GnocchiModelSuite extends GnocchiFunSuite {
//
//  //  //
//  //  val path = "src/test/resources/testData/Association"
//  //  val destination = Files.createTempDirectory("").toAbsolutePath.toString + "/" + path
//  //  val genoFilePath = ClassLoader.getSystemClassLoader.getResource("5snps10samples.vcf").getFile
//  //  val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("10samples5Phenotypes2covars.txt").getFile
//  //  val covarFilePath = ClassLoader.getSystemClassLoader.getResource("10samples5Phenotypes2covars.txt").getFile
//  //  val modelPath = "src/test/resources/testData/GnocchiModel"
//  //  val baseDir = new File(".").getAbsolutePath
//  //  val modelDestination = baseDir + "src/test/resources/testData/GnocchiModel"
//  //
//  //  ignore("Test GnocchiModel save method") {
//  //    val gm = new AdditiveLinearGnocchiModel
//  //    SaveGnocchiModel(gm, modelDestination)
//  //    assert(Files.exists(Paths.get(modelDestination)), "File doesn't exist")
//  //  }
//  //
//  //  // Construct the GnocchiModel, save, and check that the saved model exists and has the right information inside.
//  //  ignore("Test GnocchiModel construction, saving, loading: 5 snps, 10 samples, 1 phenotype, 2 random noise covars") {
//  //    /*
//  //     Uniform White Noise for Covar 1 (pheno4):
//  //      0.8404
//  //     -0.8880
//  //      0.1001
//  //     -0.5445
//  //      0.3035
//  //     -0.6003
//  //      0.4900
//  //      0.7394
//  //      1.7119
//  //     -0.1941
//  //    Uniform White Noise for Covar 2 (pheno5):
//  //      2.9080
//  //      0.8252
//  //      1.3790
//  //     -1.0582
//  //     -0.4686
//  //     -0.2725
//  //      1.0984
//  //     -0.2779
//  //      0.7015
//  //     -2.0518
//  //   */
//  //
//  //    val cliCall = s"../bin/gnocchi-submit ConstructGnocchiModel $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -saveModelTo $modelDestination -phenoName pheno1 -covar -covarFile $covarFilePath -covarNames pheno4,pheno5 -overwriteParquet"
//  //    val cliArgs = cliCall.split(" ").drop(2)
//  //    ConstructGnocchiModel(cliArgs).run(sc)
//  //    val loadedModel = LoadGnocchiModel(modelDestination)
//  //    println(loadedModel.variantModels)
//  //    val variantModels = loadedModel.variantModels
//  //    val regressionResult = variantModels.head._2.asInstanceOf[VariantModel].association
//  //    println(regressionResult.statistics)
//  //    assert(regressionResult.statistics("rSquared") === 0.833277921795612, "Incorrect rSquared = " + regressionResult.statistics("rSquared"))
//  //
//  //  }
//  //
//  //  ignore("GnocchiModel check numSamples for construction, saving, loading, updating, re-saving, re-loading: 5 snps, 5 + 5 samples, 1 phenotype, 2 random noise covars") {
//  //    val tmp = Files.createTempDirectory("").toAbsolutePath.toString + "/"
//  //
//  //    // create GM on subset of the data
//  //    val firstGenoFilePath = ClassLoader.getSystemClassLoader.getResource("5snpsFirst5samples.vcf").getFile
//  //    val firstPhenoFilePath = ClassLoader.getSystemClassLoader.getResource("first5samples5phenotypes2covars.txt").getFile
//  //    val ogModelDestination = tmp + "OriginalModel"
//  //    val cliCall = s"../bin/gnocchi-submit ConstructGnocchiModel $firstGenoFilePath $firstPhenoFilePath ADDITIVE_LINEAR $destination -saveAsText -saveModelTo $ogModelDestination -phenoName pheno1 -covar -covarFile $firstPhenoFilePath -covarNames pheno4,pheno5 -overwriteParquet"
//  //    val cliArgs = cliCall.split(" ").drop(2)
//  //    ConstructGnocchiModel(cliArgs).run(sc)
//  //
//  //    // update GM on the remainder of the data
//  //    val genosForUpdate = ClassLoader.getSystemClassLoader.getResource("5snpsSecond5samples.vcf").getFile
//  //    val phenosForUpdate = ClassLoader.getSystemClassLoader.getResource("second5samples5phenotypes2covars.txt").getFile
//  //    val updatedModelDestination = tmp + "UpdatedModel"
//  //    val updateCliCall = s"../bin/gnocchi-submit UpdateGnocchiModel $genosForUpdate $phenosForUpdate ADDITIVE_LINEAR $destination -saveAsText -modelLocation $ogModelDestination -saveModelTo $updatedModelDestination -phenoName pheno1 -covar -covarFile $phenosForUpdate -covarNames pheno4,pheno5 -overwriteParquet"
//  //    val updateCliArgs = updateCliCall.split(" ").drop(2)
//  //    UpdateGnocchiModel(updateCliArgs).run(sc)
//  //
//  //    // create GM on all of the data
//  //    val fullRecomputeCliCall = s"../bin/gnocchi-submit ConstructGnocchiModel $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -saveModelTo $modelDestination -phenoName pheno1 -covar -covarFile $covarFilePath -covarNames pheno4,pheno5 -overwriteParquet"
//  //    val fullRecomputeCliArgs = fullRecomputeCliCall.split(" ").drop(2)
//  //    ConstructGnocchiModel(fullRecomputeCliArgs).run(sc)
//  //
//  //    // load in all three models
//  //    val fullRecomputeModel = LoadGnocchiModel(modelDestination)
//  //    val ogModel = LoadGnocchiModel(ogModelDestination)
//  //    val updatedModel = LoadGnocchiModel(updatedModelDestination)
//  //
//  //    // verify that their numSamples are correct.
//  //    val fullNumSamples = fullRecomputeModel.numSamples.toArray.map(_._2).max
//  //    val ogNumSamples = ogModel.numSamples.toArray.map(_._2).max
//  //    val updatedNumSamples = updatedModel.numSamples.toArray.map(_._2).max
//  //    assert(updatedNumSamples === 10, "Number of samples in Updated model not consistent with full recompute model")
//  //    assert(ogNumSamples === 5, "Incorrect number of samples in original model before update.")
//  //  }
//  //
//  //  ignore("GnocchiModel loading saved model, making predictions, and re-saving: 5 snps, 10 samples, 1 phenotype, 2 random noise covars") {
//  //    //    val tmp = Files.createTempDirectory("").toAbsolutePath.toString + "/"
//  //    //    // build original model
//  //    //    val fullRecomputeCliCall = s"../bin/gnocchi-submit ConstructGnocchiModel $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -saveModelTo $modelDestination -phenoName pheno1 -covar -covarFile $covarFilePath -covarNames pheno4,pheno5 -overwriteParquet"
//  //    //    val fullRecomputeCliArgs = fullRecomputeCliCall.split(" ").drop(2)
//  //    //    ConstructGnocchiModel(fullRecomputeCliArgs).run(sc)
//  //    //
//  //    //    // make predictions
//  //    //    val predictions = tmp + "predictions"
//  //    //    val predictCliCall = s"../bin/gnocchi-submit PredictWithGnocchiModel $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -modelLocation $modelDestination -savePredictionsTo $predictions-phenoName pheno1 -covar -covarFile $covarFilePath -covarNames pheno4,pheno5 -overwriteParquet"
//  //    //    val predictCliArgs = predictCliCall.split(" ").drop(2)
//  //    //    PredictWithGnocchiModel(predictCliArgs).run(sc)
//  //
//  //    // check predictions TODO: figure out how to check predictions
//  //    assert(false)
//  //  }
//  //
//  //  ignore("GnocchiModel loading saved model, evaluating model on new data, and re-saving") {
//  //    // build original model
//  //    //    val tmp = Files.createTempDirectory("").toAbsolutePath.toString + "/"
//  //    //    // build original model
//  //    //    val fullRecomputeCliCall = s"../bin/gnocchi-submit ConstructGnocchiModel $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -saveModelTo $modelDestination -phenoName pheno1 -covar -covarFile $covarFilePath -covarNames pheno4,pheno5 -overwriteParquet"
//  //    //    val fullRecomputeCliArgs = fullRecomputeCliCall.split(" ").drop(2)
//  //    //    ConstructGnocchiModel(fullRecomputeCliArgs).run(sc)
//  //    //
//  //    //    // evaluate model on same data
//  //    //    val predictions = tmp + "predictions"
//  //    //    val evaluations = tmp + "evaluations"
//  //    //    val postEvalDestination = tmp + "postEvalModel"
//  //    //    val evalCliCall = s"../bin/gnocchi-submit EvaluateGnocchiModel $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -modelLocation $modelDestination -savePredictionsTo $predictions -saveEvalResultsTo $evaluations -saveModelTo $postEvalDestination -phenoName pheno1 -covar -covarFile $covarFilePath -covarNames pheno4,pheno5 -overwriteParquet"
//  //    //    val evalCliArgs = evalCliCall.split(" ").drop(2)
//  //    //    EvaluateGnocchiModel(evalCliArgs).run(sc)
//  //    //
//  //    //    // load model and check for test scores
//  //    //    val modelPostEval = LoadGnocchiModel(postEvalDestination)
//  //    //    //    val varsAndModels = modelPostEval.variantModels //List(Variant, VariantModel)
//  //    //    //    val varModels = varsAndModels.map(_._2)
//  //    //    //    val preds = varModels.foreach(_.predictions)
//  //
//  //    assert(false) // TODO: finish implementing predict and evaluate functionality
//  //
//  //  }
//  //  ignore("GnocchiModel check results of construction, saving, loading, updating, re-saving, re-loading: 5 snps, 5 + 5 samples, 1 phenotype, 2 random noise covars") {
//  //    //    // create GM on subset of the data
//  //    //    val genoFilePath =
//  //    //    val phenoFilePath =
//  //    //    val ogModelDestination =
//  //    //    val cliCall = s"../bin/gnocchi-submit ConstructGnocchiModel $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -saveModelTo $ogModelDestination -phenoName pheno1 -covar -covarFile $covarFilePath -covarNames pheno4,pheno5 -overwriteParquet"
//  //    //    val cliArgs = cliCall.split(" ").drop(2)
//  //    //    ConstructGnocchiModel(cliArgs).run(sc)
//  //    //
//  //    //    // update GM on the remainder of the data
//  //    //    val genosForUpdate =
//  //    //    val phenosForUpdate =
//  //    //    val modelLocation = ogModelDestination
//  //    //    val updatedModelDestination = baseDir + "src/test/resources/testData/UpdatedGnocchiModel"
//  //    //    val updateCliCall = s"../bin/gnocchi-submit UpdateGnocchiModel $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -modelLocation $modelLocation -saveModelTo $updatedModelDestination -phenoName pheno1 -covar -covarFile $covarFilePath -covarNames pheno4,pheno5 -overwriteParquet"
//  //    //
//  //    //    // create GM on all of the data
//  //    //    val fullRecomputeCliCall = s"../bin/gnocchi-submit ConstructGnocchiModel $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -saveModelTo $modelDestination -phenoName pheno1 -covar -covarFile $covarFilePath -covarNames pheno4,pheno5 -overwriteParquet"
//  //    //    val fullRecomputeCliArgs = fullRecomputeCliCall.split(" ").drop(2)
//  //    //    ConstructGnocchiModel(fullRecomputeCliArgs).run(sc)
//  //    //
//  //    //    // load in all three models
//  //    //    val fullRecomputeModel = LoadGnocchiModel(ogModelDestination)
//  //    //    val ogModel = LoadGnocchiModel(ogModelDestination)
//  //    //    val updatedModel = LoadGnocchiModel(updatedModelDestination)
//  //
//  //    // verify that accuracies are similar between updated an full recompute models.
//  //    assert(false)
//  //
//  //  }
//  //
//  //  ignore("GnocchiModel ensuring that endpoint phenotype is same when updating, or testing.") {
//  //    assert(false)
//  //  }
//  //
//  //  ignore("GnocchiModel ensuring that number and names of covariates is same when updating or testing.") {
//  //    assert(false)
//  //  }
//  //
//  //  ignore("GnocchiModel ensuring that type of model in update or test call is same as the model being loaded.") {
//  //    assert(false)
//  //  }
//
//}