# Gnocchi

[![Coverage Status](https://coveralls.io/repos/github/bigdatagenomics/gnocchi/badge.svg?branch=master)](https://coveralls.io/github/bigdatagenomics/gnocchi?branch=master)

Statistical associations using the [ADAM](https://github.com/bigdatagenomics/adam) genomics analysis platform.
The currently supported operations are Genome Wide Association using Linear and Logistic models with either Dominant or Additive assumptions.

# Build

To build, install [Maven](http://maven.apache.org). Then (once in the gnocchi directory) run:

```
mvn package
```

Maven will automatically pull down and install all of the necessary dependencies.
Occasionally, building in Maven will fail due to memory issues. You can work around this
by setting the `MAVEN_OPTS` environment variable to `-Xmx2g -XX:MaxPermSize=1g`.

# Run

Gnocchi is built on top of [Apache Spark](http://spark.apache.org). If you are just evaluating locally, you can use
[a prebuilt Spark distribution](http://spark.apache.org/downloads.html). If you'd like to
use a cluster, refer to Spark's [cluster overview](http://spark.apache.org/docs/latest/cluster-overview.html).

Once Spark is installed, set the environment variable `SPARK_HOME` to point to the Spark
installation root directory. 

The target binaries are complied to the `bin/` directory. Add them to your path with 

```
echo "export PATH=[GNOCCHI INSTALLATION DIR]/gnocchi/bin:\$PATH" >> $HOME/.bashrc
source $HOME/.bashrc
```

You can then run `gnocchi` via `gnocchi-submit`, or open a shell using `gnocchi-shell`.

Test data is included. You can run with the test data by running:

```
gnocchi-submit regressPhenotypes testData/5snps10samples.vcf testData/10samples5Phenotypes2covars.txt ADDITIVE_LINEAR testData/associations -saveAsText -phenoName pheno1 -covar -covarFile testData/10samples5Phenotypes2covars.txt -covarNames pheno4,pheno5
```

## Phenotype Input

Format phenotypes in a CSV. Both comma and tabs are accepted as delimiters.

```
SampleID    pheno1    pheno2
00001       0.001     0.002
```

Note: phenotypes and covariates must be numerical. For nominal scale data (i.e. categorical data), binarize. For ordinal scale data, convert to integers. 

# License

This project is released under an [Apache 2.0 license](LICENSE.txt).
