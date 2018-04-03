# PyGnocchi

Python based Statistical associations using the ADAM genomics analysis platform on Spark. The currently supported operations are Genome Wide Association using Linear and Logistic models with either Dominant or Additive associations.

PyGnocchi exposes the core functionality of Gnocchi to users through a Python interface. Included is a guide on how to install and get started using PyGnocchi (both in shell and Jupyter Notebook) as well a detailed explanation of the available functions and how to use them.

## Installation and Usage

To build and test, enable the `python` profile:

```bash
mvn -Ppython package
```

This will enable the `gnocchi-python` module as part of the Gnocchi build. This module
uses Maven to invoke a Makefile that builds a Python egg and runs tests. To
build this module, we require either an active [Conda](https://conda.io/) or
[virtualenv](https://virtualenv.pypa.io/en/stable/) environment.

[To setup and activate a Conda
environment](https://conda.io/docs/using/envs.html), run:

```bash
conda create -n gnocchi python=2.7 anaconda
source activate gnocchi
```

[To setup and activate a virtualenv
environment](https://virtualenv.pypa.io/en/stable/userguide/#usage), run:

```bash
virtualenv gnocchi
. gnocchi/bin/activate
```

Additionally, to run tests, the PySpark dependencies must be on the Python module
load path and the Gnocchi JARs must be built and provided to PySpark. This can be
done with the following bash script:

```bash
. scripts/pyspark-prepare.sh
```

We provide two scripts to run pyGnocchi. The first creates a shell instance of pySpark and loads the pyGnocchi JAR to the spark path:

```bash
. bin/pygnocchi
```

The second script loads pyGnocchi as a JAR and submits it to a jupyter notebook based instance of pySpark:

```bash
. bin/pygnocchi-notebook
```

## GnocchiSession

PyGnocchi preserves almost all of the semantic ways of interacting with the core Gnocchi CLI, including GnocchiSession. Just as in the Scala CLI for GnocchiSession, pyGnocchi provides access to loading genotypes and phenotypes, filtering out variants, and the other methods documented below.

### Creating a GnocchiSession

To initialize a GnocchiSession, pass in the SparkSession object to the GnocchiSession constructor.

```python
>>> gs = GnocchiSession(spark)
```

This will load the core GnocchiSession methods onto the `gs` object. You can then interact with Gnocchi through the exposed methods as described below.

### Methods

The methods available in the GnocchiSession are articulated below but see the method level documentation for further detail on their uses and arguments

- `GnocchiSession.loadGenotypes` - Loads a Dataset of CalledVariant objects from a file
- `GnocchiSession.loadPhenotypes` - Returns a map of phenotype name to phenotype object, which is loaded from a file, specified by phenotypesPath
- `GnocchiSession.filterVariants` - Returns a filtered Dataset of CalledVariant objects, where all variants with values less than the specified geno or maf threshold are filtered out
- `GnocchiSession.filterSamples` - Returns a filtered Dataset of CalledVariant objects, where all values with fewer samples than the mind threshold are filtered out
- `GnocchiSession.recodeMajorAllele` - Returns a modified Dataset of CalledVariant objects, where any value with a maf > 0.5 is recoded. The recoding is specified as flipping the referenceAllele and alternateAllele when the frequency of alt is greater than that of ref

### Examples

Load data from genotype and phenotype files
```python
>>> genos = gs.loadGenotypes(genotypesPath)
>>> phenos = gs.loadPhenotypes(phenotypesPath, "SampleID", "pheno1", "\t")
```

Filter out variants and samples
```python
>>> filteredGenos = gs.filterSamples(genos, 0.1, 2)
>>> filteredGenosVariants = gs.filterVariants(filteredGenos, 0.1, 0.1)
```

## GnocchiModel 

### Creating GnocchiModels

Creating a model is done through the `LinearGnocchiModel.New` or `LogisticGnocchiModel.New` factory constructors. Both constructors return a new GnocchiModel object.

### Methods

- `GnocchiModel.mergeGnocchiModel` - Takes in a second GnocchiModel as argument and returns a new model that is the merger of the current model and the model passed in.
- `GnocchiModel.mergeVariantModels` and `GnocchiModel.mergeQCVariants` expose partial merge methods.
- `GnocchiModel.save` - This serializes the model to a specified file path

There are many accessor methods exposed to the python interface. These expose scala level variables to the python interface. Accessor methods for the GnocchiModels are explicitly defined for the scala to python conversion. 
- `getVariantModels`
- `getQCVariants`
- `getModelMetadata`
- `getModelType`
- `getPhenotype`
- `getCovariates`
- `getNumSamples`
- `getHaplotypeBlockErrorThreshold`
- `getFlaggedVariantModels`

### Examples

```python
>>> genos2 = gs.loadGenotypes(genotypesPath2)
>>> phenos2 = gs.loadPhenotypes(phenotypesPath2, "IID", "pheno_1", "\t")

# Use a factory pattern constructor to create a LinearGnocchiModel
>>> linearGnocchiModel2 = LinearGnocchiModel.New(spark, genos2, phenos2, 
                                                 ["pheno_1", "pheno_2", "pheno_3", "pheno_4", "pheno_5"])
>>> linearGnocchiModel2.getNumSamples()
10000                                             
```

## Accessing Spark primitives

By exposing the underlying Datasets to python, we can run Scala dataset operations on the CalledVariantDataset objects. This allows users to interact with the rich Scala-defined properties of the internal objects. 

For example, the following lines access the first element of the `filteredGenoVariants` Dataset using the `head()` operator and access many of its underlying variables.

```python
calledVariant = filteredGenosVariants.get().head()

print("calledVariant.chromosome =", calledVariant.chromosome())
print("calledVariant.position =", calledVariant.position())
print("calledVariant.uniqueID =", calledVariant.uniqueID())
print("calledVariant.referenceAllele =", calledVariant.referenceAllele())
print("calledVariant.alternateAllele =", calledVariant.alternateAllele())
```

<!-- ### Exposing Scala

The detailed technical specs for how this happens can be found documented at the [Py4J website](https://www.py4j.org/), but a brief summary is provided here. Essentially how PyGnocchi works is that we define Python wrappers for all the exposed classes and methods. However, in order for the Python wrapper to communicate with the raw Scala code we define an intermediary set of Java classes that act as middle-men for method calls and accesses to the Python object that are translated, using Py4J into requests to the Scala code. -->
