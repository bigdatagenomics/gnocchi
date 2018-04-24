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
package org.bdgenomics.gnocchi.primitives.association

/**
 * Storage object for Logistic regression results. This object is the result of applying a
 * [[org.bdgenomics.gnocchi.models.variant.LogisticVariantModel]] to a
 * [[org.bdgenomics.gnocchi.primitives.variants.CalledVariant]] and obtaining the relevant
 * statistics that are listed below.
 *
 * @param uniqueID the UID for the variant that this object stores statistics on
 * @param chromosome Chromosome of variant
 * @param position Position of variant
 * @param numSamples number of samples used to create this association
 * @param pValue pValue of this variant
 * @param genotypeStandardError Standard error of this variant
 */
case class LogisticAssociation(uniqueID: String,
                               chromosome: Int,
                               position: Int,
                               numSamples: Int,
                               pValue: Double,
                               genotypeStandardError: Double) extends Association