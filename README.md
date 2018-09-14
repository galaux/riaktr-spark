Riaktr Spark
============

This project uses Spark version 2.3.1.

In order to be able to run it, we assume you have:
- a copy of the [official Spark 2.3.1 distribution](https://www.apache.org/dyn/closer.lua/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz)
deployed on your machine
- a `SPARK_HOME` variable pointing at the uncompressed Spark distribution
- `sbt` available in your `PATH`

If the requirements are all met, just `cd` into this project's directory and run:

```bash
sbt package
${SPARK_HOME}/bin/spark-submit \
  --class "SimpleApp" \
  --master local \
  target/scala-2.11/simple-project_2.11-0.1.0-SNAPSHOT.jar \
  [CDRS_FILEPATH] \
  [CELLS_FILEPATH] \
  [OUT_FILEPATH]
```
