# Apache Spark Connect Client for JavaScript

An <b><red>experimental</red></b> client for [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) for [Apache Spark](https://spark.apache.org/) written in [TypeScript](https://www.typescriptlang.org/).


# Roadmaps
- [ ] For minor changes or some features associated with certern classes, SEARCH 'TODO'
- [ ] Support Retry / Reattachable execution
- [ ] Support Checkpoint for DataFrame
- [ ] Support DataFrameNaFunctions
- [ ] Support DataFrame Join 
- [ ] Support User-Defined Functions (UDF)
  - [ ] UDF registration via `spark.udf.register()`
  - [ ] Inline UDFs via `udf()` function
  - [x] Java UDF registration via `spark.udf.registerJava()`
  - [ ] UDAF (User-Defined Aggregate Functions)
  - [ ] UDTF (User-Defined Table Functions)
- [x] Support DataFrame Join 
- [ ] Support UserDefinedType
  - [ ] UserDefinedType declaration
  - [ ] UserDefinedType & Proto bidi-converions
  - [ ] UserDefinedType & Arrow bidi-converions
- [ ] Maybe Optimize the Logging or it's framework

