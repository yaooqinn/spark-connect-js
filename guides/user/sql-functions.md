# SQL Functions Guide

spark.js provides a comprehensive set of SQL functions for data transformation and analysis, mirroring Apache Spark's built-in functions.

## Importing Functions

```typescript
import { 
  col, lit, when, 
  sum, avg, count, max, min,
  concat, upper, lower, trim,
  abs, round, ceil, floor
} from 'spark.js';
```

## Column Functions

### Creating Columns

```typescript
// Reference a column by name
const ageCol = col('age');

// Create a literal value
const five = lit(5);
const text = lit('hello');
```

### Aliasing

```typescript
const renamed = col('old_name').alias('new_name');
const calculated = col('price').multiply(lit(1.1)).alias('price_with_tax');
```

## Aggregate Functions

Aggregate functions operate on groups of rows.

### Basic Aggregates

```typescript
import { count, sum, avg, max, min } from 'spark.js';

// Count rows
df.select(count('*').alias('total_count'));

// Sum values
df.select(sum('amount').alias('total_amount'));

// Average
df.select(avg('score').alias('average_score'));

// Maximum and minimum
df.select(max('value').alias('max_value'));
df.select(min('value').alias('min_value'));
```

### Statistical Aggregates

```typescript
import { stddev, variance, corr, covarPop } from 'spark.js';

// Standard deviation
df.select(stddev('value').alias('std_dev'));

// Variance
df.select(variance('value').alias('var'));

// Correlation
df.select(corr('x', 'y').alias('correlation'));
```

See the [Statistical Functions Guide](STATISTICAL_FUNCTIONS.md) for more details.

### Grouping with Aggregates

```typescript
const result = df
  .groupBy('category')
  .agg(
    count('*').alias('count'),
    sum('amount').alias('total'),
    avg('score').alias('avg_score')
  );
```

## String Functions

### Case Conversion

```typescript
import { upper, lower, initcap } from 'spark.js';

df.select(
  upper(col('name')).alias('uppercase'),
  lower(col('name')).alias('lowercase'),
  initcap(col('name')).alias('titlecase')
);
```

### String Operations

```typescript
import { concat, substring, length, trim, ltrim, rtrim } from 'spark.js';

// Concatenation
df.withColumn('full_name', concat(col('first'), lit(' '), col('last')));

// Substring (1-indexed!)
df.withColumn('short', substring(col('name'), 1, 5));

// Length
df.withColumn('name_length', length(col('name')));

// Trim whitespace
df.withColumn('cleaned', trim(col('text')));
```

### String Search

```typescript
import { contains, startsWith, endsWith } from 'spark.js';

// Contains
df.filter(col('name').contains('test'));

// Starts with
df.filter(col('email').startsWith('admin'));

// Ends with
df.filter(col('filename').endsWith('.csv'));
```

### Pattern Matching

```typescript
import { like, rlike, regexp_replace } from 'spark.js';

// SQL LIKE
df.filter(col('name').like('%John%'));

// Regular expressions
df.filter(rlike(col('email'), '^[a-z]+@[a-z]+\\.com$'));

// Replace with regex
df.withColumn('cleaned', regexp_replace(col('text'), '[^a-zA-Z]', ''));
```

## Math Functions

### Basic Math

```typescript
import { abs, sqrt, pow, round } from 'spark.js';

// Absolute value
df.withColumn('abs_value', abs(col('number')));

// Square root
df.withColumn('sqrt_value', sqrt(col('number')));

// Power
df.withColumn('squared', pow(col('number'), lit(2)));

// Rounding
df.withColumn('rounded', round(col('price'), 2));
```

### Ceiling and Floor

```typescript
import { ceil, floor } from 'spark.js';

df.select(
  ceil(col('value')).alias('ceiling'),
  floor(col('value')).alias('floor')
);
```

### Trigonometric Functions

```typescript
import { sin, cos, tan, asin, acos, atan } from 'spark.js';

df.select(
  sin(col('angle')),
  cos(col('angle')),
  tan(col('angle'))
);
```

## Date and Time Functions

### Current Date/Time

```typescript
import { current_date, current_timestamp } from 'spark.js';

df.withColumn('date_added', current_date());
df.withColumn('timestamp_added', current_timestamp());
```

### Date Extraction

```typescript
import { year, month, dayofmonth, hour, minute, second } from 'spark.js';

df.select(
  year(col('date')).alias('year'),
  month(col('date')).alias('month'),
  dayofmonth(col('date')).alias('day')
);
```

### Date Arithmetic

```typescript
import { date_add, date_sub, datediff, months_between } from 'spark.js';

// Add days
df.withColumn('tomorrow', date_add(col('date'), lit(1)));

// Subtract days
df.withColumn('yesterday', date_sub(col('date'), lit(1)));

// Difference in days
df.withColumn('days_diff', datediff(col('end_date'), col('start_date')));
```

### Date Formatting

```typescript
import { date_format, to_date, to_timestamp } from 'spark.js';

// Format date
df.withColumn('formatted', date_format(col('date'), 'yyyy-MM-dd'));

// Parse string to date
df.withColumn('date', to_date(col('date_string'), 'yyyy-MM-dd'));

// Parse string to timestamp
df.withColumn('ts', to_timestamp(col('ts_string'), 'yyyy-MM-dd HH:mm:ss'));
```

## Conditional Functions

### When-Otherwise

```typescript
import { when } from 'spark.js';

df.withColumn('category',
  when(col('age').lt(18), lit('minor'))
    .when(col('age').lt(65), lit('adult'))
    .otherwise(lit('senior'))
);
```

### Coalesce

```typescript
import { coalesce } from 'spark.js';

// Return first non-null value
df.withColumn('value', coalesce(col('primary'), col('backup'), lit(0)));
```

### Case Expressions

```typescript
// Using when chains for complex conditions
df.withColumn('grade',
  when(col('score').geq(90), lit('A'))
    .when(col('score').geq(80), lit('B'))
    .when(col('score').geq(70), lit('C'))
    .when(col('score').geq(60), lit('D'))
    .otherwise(lit('F'))
);
```

## Null Handling

### Null Checks

```typescript
df.filter(col('value').isNull());
df.filter(col('value').isNotNull());
```

### Null Replacement

```typescript
import { coalesce } from 'spark.js';

// Replace nulls with default value
df.withColumn('filled', coalesce(col('value'), lit(0)));

// Using DataFrame methods
df.na.fill({ 'column': 0 });
```

## Array Functions

### Array Operations

```typescript
import { array, array_contains, explode, size } from 'spark.js';

// Create array
df.withColumn('arr', array(col('col1'), col('col2'), col('col3')));

// Check if array contains value
df.filter(array_contains(col('tags'), 'important'));

// Explode array to rows
df.select(col('id'), explode(col('items')).alias('item'));

// Array size
df.withColumn('num_items', size(col('items')));
```

## Window Functions

Window functions perform calculations across a set of rows related to the current row.

```typescript
import { row_number, rank, dense_rank, lag, lead } from 'spark.js';
import { Window } from 'spark.js';

// Define window specification
const windowSpec = Window
  .partitionBy('department')
  .orderBy(col('salary').desc());

// Apply window functions
df.withColumn('rank', rank().over(windowSpec));
df.withColumn('row_num', row_number().over(windowSpec));
df.withColumn('prev_salary', lag(col('salary'), 1).over(windowSpec));
```

## Collection Functions

### Map Operations

```typescript
import { map_keys, map_values } from 'spark.js';

df.select(
  map_keys(col('attributes')).alias('keys'),
  map_values(col('attributes')).alias('values')
);
```

## User-Defined Functions (UDF)

While native Spark functions are preferred for performance, you can define custom functions:

```typescript
// Note: UDF support may be limited in spark.js
// Check API documentation for current support
```

## Function Chaining

Functions can be chained for complex transformations:

```typescript
import { upper, trim, regexp_replace } from 'spark.js';

df.withColumn('clean_name',
  regexp_replace(
    upper(
      trim(col('name'))
    ),
    '[^A-Z ]',
    ''
  )
);
```

## Best Practices

1. **Use native functions**: Native Spark functions are optimized and faster than UDFs
2. **Column expressions over SQL strings**: Use `col('name').gt(5)` instead of `"name > 5"`
3. **Reuse columns**: Store column expressions in variables if used multiple times
4. **Read the docs**: Check [API reference](../api/) for complete function list

## Examples

### Data Cleaning

```typescript
import { trim, lower, regexp_replace, coalesce } from 'spark.js';

const cleaned = df
  .withColumn('email', lower(trim(col('email'))))
  .withColumn('phone', regexp_replace(col('phone'), '[^0-9]', ''))
  .withColumn('age', coalesce(col('age'), lit(0)));
```

### Feature Engineering

```typescript
import { year, month, datediff, when } from 'spark.js';

const features = df
  .withColumn('year', year(col('date')))
  .withColumn('month', month(col('date')))
  .withColumn('days_since', datediff(current_date(), col('date')))
  .withColumn('is_recent', when(col('days_since').lt(30), lit(1)).otherwise(lit(0)));
```

## Next Steps

- Explore [Aggregations](aggregations.md) for grouping operations
- See [Statistical Functions](STATISTICAL_FUNCTIONS.md) for statistical analysis
- Check the [API Reference](../api/) for the complete function catalog
