# gnocchi

[![Coverage Status](https://coveralls.io/repos/github/bigdatagenomics/gnocchi/badge.svg?branch=master)](https://coveralls.io/github/bigdatagenomics/gnocchi?branch=master)

Genome-Wide Association Studies using the [ADAM](https://github.com/bigdatagenomics/adam) genomics analysis platform.
This is work-in-progress. Currently, we support Additive Linear and Dominant Linear models (for continuous outcome variables), and Additive Logistic and Dominant Logistic models (for binary [0,1] or [1,2] outcome variables). Each can be run with or without phenotypic covariates.

# Build

To build, install [Maven](http://maven.apache.org). Then (once in the gnocchi directory) run:

```
mvn package
```

Maven will automatically pull down and install all of the necessary dependencies.
Occasionally, building in Maven will fail due to memory issues. You can work around this
by setting the `MAVEN_OPTS` environment variable to `-Xmx2g -XX:MaxPermSize=1g`.

# Run

To run, you'll need to install Spark. If you are just evaluating locally, you can use
[a prebuilt Spark distribution](http://spark.apache.org/downloads.html). If you'd like to
use a cluster, refer to Spark's [cluster overview](http://spark.apache.org/docs/latest/cluster-overview.html).

Once Spark is installed, set the environment variable `SPARK_HOME` to point to the Spark
installation root directory. Then, you can run `gnocchi` via `./bin/gnocchi-submit`.

We include test data. You can run with the test data by running:

```
./bin/gnocchi-submit regressPhenotypes testData/5snps10samples.vcf testData/10samples5Phenotypes2covars.txt ADDITIVE_LINEAR testData/associations -saveAsText -phenoName pheno1 -covar -covarFile testData/10samples5Phenotypes2covars.txt -covarNames pheno4,pheno5
```

## Phenotype Input

We accept phenotype and covariate inputs in CSV (tab delimited) format:

```
SampleID	pheno1	pheno2
mySample	pheno1_value	pheno2_value
```

Note: phenotypes and covariates must be numerical. For nominal scale data (i.e. categorical data), binarize. For ordinal scale data, convert to integers. 

# License

This project is released under an [Apache 2.0 license](LICENSE.txt).
