# Aggregations and Grouping

Learn how to perform aggregations and group data using spark.js.

## Basic Aggregations

### Simple Aggregates

Perform aggregations on entire DataFrames:

```typescript
import { count, sum, avg, max, min } from 'spark.js';

// Count all rows
const total = await df.count();

// Aggregate using select
const stats = df.select(
  count('*').alias('count'),
  sum('amount').alias('total'),
  avg('amount').alias('average'),
  max('amount').alias('maximum'),
  min('amount').alias('minimum')
);

await stats.show();
```

### Using agg()

The `agg()` method allows multiple aggregations:

```typescript
const result = df.agg(
  count('*').alias('row_count'),
  sum('sales').alias('total_sales'),
  avg('price').alias('avg_price')
);
```

## GroupBy Operations

### Basic Grouping

Group data by one or more columns:

```typescript
// Group by single column
const grouped = df.groupBy('category');

// Group by multiple columns
const grouped = df.groupBy('category', 'region');
```

### Aggregating Groups

```typescript
import { count, sum, avg } from 'spark.js';

// Count per group
const counts = df.groupBy('category').count();

// Sum per group
const totals = df.groupBy('category').sum('amount');

// Average per group
const averages = df.groupBy('category').avg('score');

// Multiple aggregations
const stats = df.groupBy('category').agg(
  count('*').alias('count'),
  sum('amount').alias('total'),
  avg('amount').alias('average'),
  max('amount').alias('max'),
  min('amount').alias('min')
);
```

### Common GroupBy Methods

```typescript
// Count
df.groupBy('category').count()

// Sum
df.groupBy('category').sum('amount')

// Average
df.groupBy('category').avg('score')

// Min and Max
df.groupBy('category').min('value')
df.groupBy('category').max('value')

// Mean (same as avg)
df.groupBy('category').mean('value')
```

## Advanced Aggregations

### Multiple Aggregations

Use `agg()` for complex aggregations:

```typescript
import { 
  count, sum, avg, max, min, 
  stddev, variance, 
  collect_list, collect_set 
} from 'spark.js';

const result = df.groupBy('department').agg(
  count('*').alias('employee_count'),
  sum('salary').alias('total_salary'),
  avg('salary').alias('avg_salary'),
  max('salary').alias('max_salary'),
  min('salary').alias('min_salary'),
  stddev('salary').alias('salary_stddev'),
  collect_list('name').alias('employee_names')
);
```

### Conditional Aggregations

Aggregate with conditions:

```typescript
import { sum, when, lit } from 'spark.js';

const result = df.groupBy('category').agg(
  sum(
    when(col('status').equalTo('active'), col('amount'))
      .otherwise(lit(0))
  ).alias('active_amount'),
  
  sum(
    when(col('status').equalTo('inactive'), col('amount'))
      .otherwise(lit(0))
  ).alias('inactive_amount')
);
```

### Distinct Counts

```typescript
import { countDistinct, approx_count_distinct } from 'spark.js';

// Exact distinct count
df.groupBy('category').agg(
  countDistinct('user_id').alias('unique_users')
);

// Approximate distinct count (faster for large datasets)
df.groupBy('category').agg(
  approx_count_distinct('user_id').alias('approx_unique_users')
);
```

## Pivot Tables

Create pivot tables for cross-tabulation:

```typescript
// Basic pivot
const pivoted = df
  .groupBy('year')
  .pivot('quarter')
  .sum('revenue');

// Pivot with specific values
const pivoted = df
  .groupBy('product')
  .pivot('region', ['North', 'South', 'East', 'West'])
  .avg('sales');
```

Result structure:
```
product | North | South | East | West
--------|-------|-------|------|-----
A       | 100   | 150   | 200  | 120
B       | 80    | 90    | 110  | 95
```

## Rollup and Cube

### Rollup

Create hierarchical aggregations:

```typescript
// Rollup for subtotals
const result = df
  .rollup('year', 'quarter', 'month')
  .agg(sum('revenue').alias('total_revenue'));

// Creates aggregations at:
// - (year, quarter, month)
// - (year, quarter)
// - (year)
// - grand total (all nulls)
```

### Cube

Create aggregations for all combinations:

```typescript
// Cube for all combinations
const result = df
  .cube('region', 'product')
  .agg(sum('sales').alias('total_sales'));

// Creates aggregations for:
// - (region, product)
// - (region, null)
// - (null, product)
// - (null, null) - grand total
```

## Window Functions

Window functions perform calculations across rows related to the current row.

### Creating Windows

```typescript
import { Window } from 'spark.js';

// Partition by department, order by salary
const windowSpec = Window
  .partitionBy('department')
  .orderBy(col('salary').desc());

// With row boundaries
const windowSpec = Window
  .partitionBy('category')
  .orderBy('date')
  .rowsBetween(-2, 0);  // Current row and 2 preceding rows
```

### Ranking Functions

```typescript
import { row_number, rank, dense_rank, percent_rank } from 'spark.js';

const ranked = df.withColumn(
  'rank',
  rank().over(Window.partitionBy('department').orderBy(col('salary').desc()))
);

const withRowNum = df.withColumn(
  'row_num',
  row_number().over(Window.partitionBy('category').orderBy('date'))
);
```

### Aggregate Window Functions

```typescript
import { sum, avg, max, min } from 'spark.js';

const windowSpec = Window.partitionBy('category').orderBy('date');

const result = df
  .withColumn('running_total', sum('amount').over(windowSpec))
  .withColumn('running_avg', avg('amount').over(windowSpec));
```

### Lead and Lag

Access rows before or after the current row:

```typescript
import { lag, lead } from 'spark.js';

const windowSpec = Window
  .partitionBy('user_id')
  .orderBy('timestamp');

const result = df
  .withColumn('previous_value', lag(col('value'), 1).over(windowSpec))
  .withColumn('next_value', lead(col('value'), 1).over(windowSpec))
  .withColumn('change', col('value').minus(lag(col('value'), 1).over(windowSpec)));
```

## Statistical Aggregations

### Basic Statistics

```typescript
import { 
  mean, stddev, variance, 
  skewness, kurtosis 
} from 'spark.js';

const stats = df.agg(
  mean('value').alias('mean'),
  stddev('value').alias('std_dev'),
  variance('value').alias('variance'),
  skewness('value').alias('skewness'),
  kurtosis('value').alias('kurtosis')
);
```

### Correlation and Covariance

```typescript
import { corr, covar_pop, covar_samp } from 'spark.js';

const result = df.agg(
  corr('x', 'y').alias('correlation'),
  covar_pop('x', 'y').alias('covariance_pop'),
  covar_samp('x', 'y').alias('covariance_samp')
);
```

See [Statistical Functions](STATISTICAL_FUNCTIONS.md) for more details.

## Collection Aggregations

### Collecting Values

```typescript
import { collect_list, collect_set } from 'spark.js';

const result = df.groupBy('category').agg(
  collect_list('item').alias('all_items'),      // With duplicates
  collect_set('item').alias('unique_items')     // Without duplicates
);
```

### Array Aggregations

```typescript
import { array_agg, concat_ws } from 'spark.js';

// Concatenate strings
const result = df.groupBy('group').agg(
  concat_ws(',', collect_list('name')).alias('names_csv')
);
```

## Performance Optimization

### Avoid Skew

For skewed data, consider salting:

```typescript
import { lit, rand } from 'spark.js';

// Add salt for better distribution
const salted = df
  .withColumn('salt', (rand().multiply(lit(10))).cast('int'))
  .groupBy('category', 'salt')
  .agg(sum('amount'))
  .groupBy('category')
  .agg(sum('amount'));
```

### Broadcast Small Tables

For joins with small dimension tables:

```typescript
import { broadcast } from 'spark.js';

const result = largeDf.join(
  broadcast(smallDf),
  'key'
);
```

### Cache Intermediate Results

```typescript
// Cache frequently used aggregation
const grouped = df.groupBy('category').count();
grouped.cache();

// Use it multiple times
const filtered1 = grouped.filter('count > 100');
const filtered2 = grouped.filter('count < 50');
```

## Common Patterns

### Top N per Group

```typescript
import { row_number } from 'spark.js';

const windowSpec = Window
  .partitionBy('category')
  .orderBy(col('sales').desc());

const topN = df
  .withColumn('rank', row_number().over(windowSpec))
  .filter(col('rank').leq(10));  // Top 10 per category
```

### Moving Average

```typescript
const windowSpec = Window
  .partitionBy('product')
  .orderBy('date')
  .rowsBetween(-6, 0);  // 7-day window

const withMA = df.withColumn(
  'moving_avg_7d',
  avg('value').over(windowSpec)
);
```

### Percentage of Total

```typescript
const windowSpec = Window.partitionBy('category');

const withPct = df.withColumn(
  'percent_of_category',
  col('amount').divide(sum('amount').over(windowSpec)).multiply(lit(100))
);
```

### Cumulative Sum

```typescript
const windowSpec = Window
  .partitionBy('account')
  .orderBy('date')
  .rowsBetween(Window.unboundedPreceding, Window.currentRow);

const cumulative = df.withColumn(
  'cumulative_balance',
  sum('amount').over(windowSpec)
);
```

## Examples

### Sales Analysis

```typescript
// Group sales by region and product
const sales = df.groupBy('region', 'product').agg(
  sum('quantity').alias('total_quantity'),
  sum('revenue').alias('total_revenue'),
  avg('price').alias('avg_price'),
  count('*').alias('transaction_count')
);

// Add percentage of total revenue
const windowSpec = Window.partitionBy('region');
const withPct = sales.withColumn(
  'pct_of_region',
  col('total_revenue')
    .divide(sum('total_revenue').over(windowSpec))
    .multiply(lit(100))
);
```

### Cohort Analysis

```typescript
import { datediff, min } from 'spark.js';

// Calculate user cohort
const withCohort = df
  .withColumn('cohort_date', 
    min('first_purchase_date').over(Window.partitionBy('user_id'))
  )
  .withColumn('days_since_cohort',
    datediff(col('event_date'), col('cohort_date'))
  );

const cohortStats = withCohort
  .groupBy('cohort_date', 'days_since_cohort')
  .agg(countDistinct('user_id').alias('active_users'));
```

## Next Steps

- Learn about [Statistical Functions](STATISTICAL_FUNCTIONS.md)
- Explore [SQL Functions](sql-functions.md) for transformations
- Check the [API Reference](../api/) for complete function list
