# DataFrame Statistical Functions

This document provides examples of using the statistical functions available in the Spark Connect JS client.

## Overview

The statistical functions are accessible through the `stat` property of a DataFrame. These functions enable various statistical analyses including correlation, covariance, cross-tabulation, frequent items detection, stratified sampling, and approximate quantile calculation.

## Available Functions

### cov(col1, col2)

Calculate the sample covariance of two numerical columns.

```javascript
const df = await spark.sql("SELECT * FROM values (1.0, 2.0), (2.0, 3.0), (3.0, 4.0) AS data(a, b)");
const covariance = await df.stat.cov("a", "b");
console.log(`Covariance: ${covariance}`);
```

### corr(col1, col2, method?)

Calculate the correlation of two columns. Currently only supports Pearson Correlation Coefficient.

```javascript
const df = await spark.sql("SELECT * FROM values (1.0, 2.0), (2.0, 3.0), (3.0, 4.0) AS data(a, b)");
const correlation = await df.stat.corr("a", "b");
console.log(`Correlation: ${correlation}`);

// With explicit method
const pearsonCorr = await df.stat.corr("a", "b", "pearson");
```

### crosstab(col1, col2)

Computes a pair-wise frequency table (contingency table) of the given columns.

```javascript
const df = await spark.sql(`
  SELECT * FROM values 
    ('a', 'x'), 
    ('a', 'y'), 
    ('b', 'x'), 
    ('b', 'y') 
  AS data(col1, col2)
`);
const crosstabDf = df.stat.crosstab("col1", "col2");
await crosstabDf.show();
```

### freqItems(cols, support?)

Find frequent items for columns, possibly with false positives using the frequent element count algorithm.

```javascript
const df = await spark.sql(`
  SELECT * FROM values 
    ('a', 1), 
    ('a', 2), 
    ('b', 1), 
    ('b', 1) 
  AS data(col1, col2)
`);

// With default support (0.01 or 1%)
const freqDf = df.stat.freqItems(["col1", "col2"]);
await freqDf.show();

// With custom support threshold
const freqDfWithSupport = df.stat.freqItems(["col1", "col2"], 0.4);
await freqDfWithSupport.show();
```

### sampleBy(col, fractions, seed?)

Returns a stratified sample without replacement based on the fraction given on each stratum.

```javascript
import { col } from 'spark-connect';

const df = await spark.sql(`
  SELECT * FROM values 
    ('a', 1), 
    ('a', 2), 
    ('b', 3), 
    ('b', 4) 
  AS data(key, value)
`);

const fractions = new Map();
fractions.set("a", 0.5);
fractions.set("b", 0.3);

const sampledDf = df.stat.sampleBy(col("key"), fractions, 42);
await sampledDf.show();
```

### approxQuantile(cols, probabilities, relativeError)

Calculates the approximate quantiles of numerical columns of a DataFrame.

```javascript
const df = await spark.sql(`
  SELECT * FROM values 
    (1.0, 2.0), 
    (2.0, 3.0), 
    (3.0, 4.0), 
    (4.0, 5.0) 
  AS data(a, b)
`);

// Calculate min, median, and max (0.0, 0.5, 1.0 quantiles)
const quantileDf = df.stat.approxQuantile(["a", "b"], [0.0, 0.5, 1.0], 0.01);
await quantileDf.show();

// For exact quantiles, set relativeError to 0.0 (more expensive)
const exactQuantileDf = df.stat.approxQuantile(["a"], [0.25, 0.5, 0.75], 0.0);
await exactQuantileDf.show();
```

## Notes

- **support** parameter in `freqItems()` should be greater than 1e-4 (0.0001)
- **relativeError** in `approxQuantile()` should be >= 0. Setting it to 0 computes exact quantiles but is more expensive
- **method** parameter in `corr()` currently only supports "pearson" (default)
- All functions work with Spark Connect protocol and require a running Spark Connect server

## References

- [Spark SQL Statistical Functions](https://spark.apache.org/docs/latest/api/sql/index.html#statistical-functions)
- [PySpark DataFrame.stat](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.stat.html)
