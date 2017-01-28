# gnocchi

Genotype-phenotype analysis using the [ADAM](https://github.com/bigdatagenomics/adam) genomics analysis platform.
This is work-in-progress. Currently, we implement a simple case/control analysis using a Chi squared test.

# Build

To build, install [Maven](http://maven.apache.org). Then run:

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
./bin/gnocchi-submit regressPhenotypes testData/sample.vcf testData/samplePhenotypes.csv testData/associations -saveAsText
```

## Phenotype Input

We accept phenotype inputs in a CSV format:

```
Sample,Phenotype,Has Phenotype
mySample,a phenotype,true
```

The `has phenotype` column is binary true/false. See the test data for more descriptions.

# License

This project is released under an [Apache 2.0 license](LICENSE.txt).
