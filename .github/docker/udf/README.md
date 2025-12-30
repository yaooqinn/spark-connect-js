# Test UDF JAR

This directory contains a custom Java UDF for testing purposes.

## UDF Classes

- `org.apache.spark.sql.test.StringConcat` - Concatenates two strings

## Build Process

The UDF is compiled and packaged during the Docker build process. The Dockerfile:
1. Compiles `StringConcat.scala` using scalac with Spark JARs on the classpath
2. Packages the compiled class into `spark-js-test-udfs.jar`
3. Places the JAR in `/opt/spark/jars/` where it's automatically loaded by Spark

This eliminates the need for `--packages` option and provides a custom UDF for testing.
