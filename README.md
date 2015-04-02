# g2pilot

Demo code showing how one could possibly do genotype-phenotype analysis using ADAM.
This is work-in-progress and doesn't work yet.

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
installation root directory. Then, you can run `g2pilot` via `./bin/g2pilot-submit`.

# License

This project is under an [Apache 2.0 license](LICENSE.txt).
