# Roadmap

This document outlines the planned features and improvements for spark.js. For the latest status, see the checkboxes below.

## SparkSession APIs

- [ ] **Session Lifecycle Management**
  - [ ] `close()` - Properly close session and release resources
  - [ ] `newSession()` - Create a new independent session
  - [ ] `cloneSession()` - Clone current session with all state
  - [ ] `cloneSession(sessionId)` - Clone with custom session ID

- [ ] **SQL with Parameters**
  - [ ] `sql(sqlText, args: Array)` - SQL with positional arguments
  - [ ] `sql(sqlText, args: Map<string, any>)` - SQL with named arguments (Map)
  - [ ] `sql(sqlText, args: Record<string, any>)` - SQL with named arguments (Object)

- [ ] **Operation Interruption**
  - [ ] `interruptAll()` - Interrupt all running operations
  - [ ] `interruptTag(tag)` - Interrupt operations with specific tag
  - [ ] `interruptOperation(operationId)` - Interrupt a specific operation

- [ ] **Session Tags** (for operation tracking and management)
  - [ ] `addTag(tag)` - Add a tag to the session
  - [ ] `removeTag(tag)` - Remove a tag from the session
  - [ ] `getTags()` - Get all session tags
  - [ ] `clearTags()` - Clear all session tags

- [ ] **Streaming Support**
  - [ ] `readStream` - DataStreamReader for reading streaming data
  - [ ] `streams` - StreamingQueryManager for managing streaming queries
  - [ ] Streaming DataFrame operations (writeStream, etc.)

- [ ] **Artifact Management** (for distributing files/libraries to executors)
  - [ ] `addArtifact(path)` - Add artifact from local path
  - [ ] `addArtifact(uri)` - Add artifact from URI
  - [ ] `addArtifact(bytes, target)` - Add artifact from bytes
  - [ ] `addArtifact(source, target)` - Add artifact with source and target
  - [ ] `addArtifacts(...uris)` - Add multiple artifacts at once

- [ ] **Dataset/DataFrame Creation**
  - [ ] `emptyDataset<T>()` - Create empty typed Dataset
  - [ ] `createDataset(data: List)` - Support for Java/JavaScript collections
  - [ ] `createDataFrame(rows: List<Row>, schema)` - Java collection support
  - [ ] `createDataFrame(data: List, beanClass)` - Java bean support

- [ ] **Table-Valued Functions**
  - [ ] `tvf` - TableValuedFunction support for TVF calls

- [ ] **External Command Execution**
  - [ ] `executeCommand(runner, command, options)` - Execute external commands

- [ ] **Developer APIs** (for advanced users)
  - [ ] `newDataFrame(f)` - Low-level DataFrame creation
  - [ ] `newDataFrame(cols)(f)` - DataFrame with column references
  - [ ] `newDataset<T>(encoder, f)` - Low-level Dataset creation
  - [ ] `newDataset<T>(encoder, cols, f)` - Dataset with column references

- [ ] **Internal Utilities**
  - [ ] `sameSemantics(plan, otherPlan)` - Check plan semantic equivalence
  - [ ] `semanticHash(plan)` - Get semantic hash of a plan
  - [ ] `timeZoneId` - Get configured time zone
  - [ ] `largeVarTypes` - Get Arrow large var types setting

## DataFrame/Dataset APIs

### Basic Operations
- [ ] `as(alias: Symbol)` - Alias with Symbol support
- [ ] `alias(alias: string | Symbol)` - Alias variants

### Column Selection
- [ ] `select` with multiple TypedColumn overloads (2-5 columns returning tuples)

### Filtering & Transformation
- [ ] `filter(func: T => Boolean)` - Filter with function
- [ ] `filter(f: FilterFunction[T])` - Filter with FilterFunction (Java)
- [ ] `map[U](f: T => U)` - Map transformation
- [ ] `map[U](f: MapFunction[T, U])` - Map with MapFunction (Java)
- [ ] `mapPartitions[U](func: Iterator[T] => Iterator[U])` - Transform partitions
- [ ] `mapPartitions[U](f: MapPartitionsFunction[T, U])` - Transform partitions (Java)
- [ ] `flatMap[U](func: T => IterableOnce[U])` - FlatMap transformation
- [ ] `flatMap[U](f: FlatMapFunction[T, U])` - FlatMap (Java)

### Column Operations
- [ ] `withColumns(colsMap: Map<string, Column>)` - Add/replace multiple columns with Map
- [ ] `withColumnsRenamed(colsMap: Map<string, string>)` - Rename multiple columns with Map
- [ ] `withMetadata(columnName, metadata)` - Update column metadata

### Joins
- [ ] `joinWith[U](other, condition)` - Join returning tuples
- [ ] `joinWith[U](other, condition, joinType)` - Join with type returning tuples

### Aggregations
- [ ] `agg(aggExpr: (string, string), ...aggExprs)` - Aggregate with tuples
- [ ] `agg(exprs: Map<string, string>)` - Aggregate with Map
- [ ] `reduce(func: (T, T) => T)` - Reduce operation
- [ ] `groupByKey[K](func: T => K)` - GroupBy with key function

### Deduplication
- [ ] `dropDuplicatesWithinWatermark()` - Drop duplicates within watermark
- [ ] `dropDuplicatesWithinWatermark(colNames)` - Drop duplicates within watermark for specific columns

### Sampling & Splitting
- [ ] `randomSplit(weights, seed?)` - Split dataset into random samples
- [ ] `randomSplitAsList(weights, seed?)` - Split into List (Java)

### Partitioning
- [ ] `repartitionById(numPartitions, partitionIdExpr)` - Repartition by partition ID expression

### Actions
- [ ] `collectAsList()` - Collect as Java List
- [ ] `toLocalIterator()` - Return local iterator
- [ ] `foreach(f: T => Unit)` - Iterate with function
- [ ] `foreach(func: ForeachFunction[T])` - Iterate (Java)
- [ ] `foreachPartition(f: Iterator[T] => Unit)` - Iterate partitions with function
- [ ] `foreachPartition(func: ForeachPartitionFunction[T])` - Iterate partitions (Java)

### Statistical & Summary
- [ ] `summary(statistics)` - Summary statistics with custom stats

### Streaming Operations
- [ ] `writeStream` - DataStreamWriter for streaming writes

### Observation
- [ ] `observe(name, expr, ...exprs)` - Observe metrics by name
- [ ] `observe(observation, expr, ...exprs)` - Observe with Observation object

### Serialization
- [ ] `toJSON` - Convert to JSON Dataset

### I/O Operations  
- [ ] `mergeInto(table, condition)` - Merge into table with condition

## User-Defined Functions (UDF)

- [ ] UDF registration via `spark.udf.register()`
- [ ] Inline UDFs via `udf()` function
- [ ] UDAF (User-Defined Aggregate Functions)
- [ ] UDTF (User-Defined Table Functions)
- [ ] `registerClassFinder()` - For dynamically generated classes

## DataFrame/Dataset Features

- [ ] Support Retry / Reattachable execution
- [ ] Support Checkpoint for DataFrame
- [ ] Support DataFrameNaFunctions

## Infrastructure & Quality

- [ ] Support UserDefinedType
  - [ ] UserDefinedType declaration
  - [ ] UserDefinedType & Proto bidirectional conversions
  - [ ] UserDefinedType & Arrow bidirectional conversions
- [ ] Maybe optimize the logging framework
- [ ] For minor changes or some features associated with certain classes, SEARCH 'TODO'

## SQL Functions

### Aggregate Functions
- [ ] `any_value` (with ignoreNulls) ✅ (implemented)
- [ ] `approx_count_distinct` ✅ (implemented)
- [ ] `approx_percentile` ✅ (implemented)
- [ ] `array_agg`
- [ ] `avg` / `mean` ✅ (implemented)
- [ ] `bit_and` ✅ (implemented)
- [ ] `bit_or` ✅ (implemented)
- [ ] `bit_xor` ✅ (implemented)
- [ ] `bitmap_construct_agg`
- [ ] `bitmap_or_agg`
- [ ] `bool_and` / `every` ✅ (implemented)
- [ ] `bool_or` ✅ (implemented)
- [ ] `collect_list` ✅ (implemented)
- [ ] `collect_set` ✅ (implemented)
- [ ] `corr` ✅ (implemented)
- [ ] `count` ✅ (implemented)
- [ ] `count_distinct` ✅ (implemented)
- [ ] `count_if` ✅ (implemented)
- [ ] `count_min_sketch` ✅ (implemented)
- [ ] `covar_pop` ✅ (implemented)
- [ ] `covar_samp` ✅ (implemented)
- [ ] `first` / `first_value` ✅ (implemented)
- [ ] `grouping` ✅ (implemented)
- [ ] `grouping_id` ✅ (implemented)
- [ ] `histogram_numeric` ✅ (implemented)
- [ ] `hll_sketch_agg` ✅ (implemented)
- [ ] `hll_union_agg` ✅ (implemented)
- [ ] `kurtosis` ✅ (implemented)
- [ ] `last` / `last_value` ✅ (implemented)
- [ ] `listagg` / `string_agg` ✅ (implemented)
- [ ] `max` ✅ (implemented)
- [ ] `max_by` ✅ (implemented)
- [ ] `median` ✅ (implemented)
- [ ] `min` ✅ (implemented)
- [ ] `min_by` ✅ (implemented)
- [ ] `mode` ✅ (implemented)
- [ ] `percentile` ✅ (implemented)
- [ ] `percentile_approx` ✅ (implemented)
- [ ] `product` ✅ (implemented)
- [ ] `regr_avgx` / `regr_avgy` / `regr_count` / `regr_intercept` / `regr_r2` / `regr_slope` / `regr_sxx` / `regr_sxy` / `regr_syy` ✅ (implemented)
- [ ] `skewness` ✅ (implemented)
- [ ] `some` / `any` ✅ (implemented)
- [ ] `std` / `stddev` / `stddev_samp` / `stddev_pop` ✅ (implemented)
- [ ] `sum` / `sum_distinct` ✅ (implemented)
- [ ] `try_avg`
- [ ] `try_sum`
- [ ] `var_pop` / `variance` / `var_samp` ✅ (implemented)

### String Functions
- [ ] `ascii`
- [ ] `base64`
- [ ] `bit_length`
- [ ] `btrim`
- [ ] `char`
- [ ] `char_length` / `character_length`
- [ ] `chr`
- [ ] `concat`
- [ ] `concat_ws`
- [ ] `contains`
- [ ] `decode`
- [ ] `elt`
- [ ] `encode`
- [ ] `endswith`
- [ ] `find_in_set`
- [ ] `format_number`
- [ ] `format_string`
- [ ] `initcap`
- [ ] `instr`
- [ ] `lcase` / `lower`
- [ ] `left`
- [ ] `len` / `length`
- [ ] `levenshtein`
- [ ] `locate`
- [ ] `lpad`
- [ ] `ltrim`
- [ ] `luhn_check`
- [ ] `mask`
- [ ] `octet_length`
- [ ] `overlay`
- [ ] `position`
- [ ] `printf`
- [ ] `regexp_count`
- [ ] `regexp_extract`
- [ ] `regexp_extract_all`
- [ ] `regexp_instr`
- [ ] `regexp_like`
- [ ] `regexp_replace`
- [ ] `regexp_substr`
- [ ] `repeat`
- [ ] `replace`
- [ ] `reverse`
- [ ] `right`
- [ ] `rpad`
- [ ] `rtrim`
- [ ] `sentences`
- [ ] `soundex`
- [ ] `space`
- [ ] `split`
- [ ] `split_part`
- [ ] `startswith`
- [ ] `substr` / `substring`
- [ ] `substring_index`
- [ ] `to_binary`
- [ ] `to_char`
- [ ] `to_number`
- [ ] `translate`
- [ ] `trim`
- [ ] `try_to_binary`
- [ ] `try_to_number`
- [ ] `ucase` / `upper`
- [ ] `unbase64`

### Date/Time Functions
- [ ] `add_months`
- [ ] `convert_timezone`
- [ ] `curdate` / `current_date`
- [ ] `current_timestamp` / `current_timezone`
- [ ] `date_add`
- [ ] `date_diff`
- [ ] `date_format`
- [ ] `date_from_unix_date`
- [ ] `date_part`
- [ ] `date_sub`
- [ ] `date_trunc`
- [ ] `dateadd`
- [ ] `datediff`
- [ ] `datepart`
- [ ] `day` / `dayofmonth`
- [ ] `dayofweek`
- [ ] `dayofyear`
- [ ] `extract`
- [ ] `from_unixtime`
- [ ] `from_utc_timestamp`
- [ ] `hour`
- [ ] `last_day`
- [ ] `localtimestamp`
- [ ] `make_date`
- [ ] `make_dt_interval`
- [ ] `make_interval`
- [ ] `make_timestamp`
- [ ] `make_timestamp_ltz`
- [ ] `make_timestamp_ntz`
- [ ] `make_ym_interval`
- [ ] `minute`
- [ ] `month`
- [ ] `months_between`
- [ ] `next_day`
- [ ] `now`
- [ ] `quarter`
- [ ] `second`
- [ ] `session_window`
- [ ] `timestamp_micros`
- [ ] `timestamp_millis`
- [ ] `timestamp_seconds`
- [ ] `to_date`
- [ ] `to_timestamp`
- [ ] `to_timestamp_ltz`
- [ ] `to_timestamp_ntz`
- [ ] `to_unix_timestamp`
- [ ] `to_utc_timestamp`
- [ ] `trunc`
- [ ] `try_to_timestamp`
- [ ] `unix_date`
- [ ] `unix_micros`
- [ ] `unix_millis`
- [ ] `unix_seconds`
- [ ] `unix_timestamp`
- [ ] `weekday`
- [ ] `weekofyear`
- [ ] `window`
- [ ] `window_time`
- [ ] `year`

### Mathematical Functions
- [ ] `abs`
- [ ] `acos`
- [ ] `acosh`
- [ ] `asin`
- [ ] `asinh`
- [ ] `atan`
- [ ] `atan2`
- [ ] `atanh`
- [ ] `bin`
- [ ] `bround`
- [ ] `cbrt`
- [ ] `ceil` / `ceiling`
- [ ] `conv`
- [ ] `cos`
- [ ] `cosh`
- [ ] `cot`
- [ ] `csc`
- [ ] `degrees`
- [ ] `e`
- [ ] `exp`
- [ ] `expm1`
- [ ] `factorial`
- [ ] `floor`
- [ ] `greatest`
- [ ] `hex`
- [ ] `hypot`
- [ ] `least`
- [ ] `ln`
- [ ] `log`
- [ ] `log10`
- [ ] `log1p`
- [ ] `log2`
- [ ] `negative` ✅ (implemented as `negate`)
- [ ] `pi`
- [ ] `pmod` ✅ (implemented)
- [ ] `positive` ✅ (implemented)
- [ ] `pow` / `power` ✅ (implemented)
- [ ] `radians`
- [ ] `rand` ✅ (implemented)
- [ ] `randn` ✅ (implemented)
- [ ] `randstr` ✅ (implemented)
- [ ] `rint` ✅ (implemented)
- [ ] `round`
- [ ] `sec`
- [ ] `sign` / `signum`
- [ ] `sin`
- [ ] `sinh`
- [ ] `sqrt` ✅ (implemented)
- [ ] `tan`
- [ ] `tanh`
- [ ] `try_add`
- [ ] `try_divide`
- [ ] `try_multiply`
- [ ] `try_subtract`
- [ ] `unhex`
- [ ] `width_bucket`

### Array Functions
- [ ] `array` ✅ (implemented)
- [ ] `array_agg`
- [ ] `array_append`
- [ ] `array_compact`
- [ ] `array_contains`
- [ ] `array_distinct`
- [ ] `array_except`
- [ ] `array_insert`
- [ ] `array_intersect`
- [ ] `array_join`
- [ ] `array_max`
- [ ] `array_min`
- [ ] `array_position`
- [ ] `array_prepend`
- [ ] `array_remove`
- [ ] `array_repeat`
- [ ] `array_size`
- [ ] `array_sort`
- [ ] `array_union`
- [ ] `arrays_overlap`
- [ ] `arrays_zip`
- [ ] `concat`
- [ ] `element_at`
- [ ] `exists`
- [ ] `explode`
- [ ] `explode_outer`
- [ ] `filter`
- [ ] `flatten`
- [ ] `forall`
- [ ] `get`
- [ ] `inline`
- [ ] `inline_outer`
- [ ] `posexplode`
- [ ] `posexplode_outer`
- [ ] `reverse`
- [ ] `sequence`
- [ ] `shuffle`
- [ ] `size`
- [ ] `slice`
- [ ] `sort_array`
- [ ] `transform`
- [ ] `zip_with`

### Map Functions
- [ ] `element_at`
- [ ] `map` ✅ (implemented)
- [ ] `map_concat`
- [ ] `map_contains_key`
- [ ] `map_entries`
- [ ] `map_filter`
- [ ] `map_from_arrays` ✅ (implemented)
- [ ] `map_from_entries`
- [ ] `map_keys`
- [ ] `map_values`
- [ ] `map_zip_with`
- [ ] `size`
- [ ] `str_to_map` ✅ (implemented)
- [ ] `transform_keys`
- [ ] `transform_values`

### Struct/Complex Functions
- [ ] `struct`
- [ ] `named_struct` ✅ (implemented)

### Window Functions
- [ ] `cume_dist` ✅ (implemented)
- [ ] `dense_rank` ✅ (implemented)
- [ ] `lag` ✅ (implemented)
- [ ] `lead` ✅ (implemented)
- [ ] `nth_value` ✅ (implemented)
- [ ] `ntile` ✅ (implemented)
- [ ] `percent_rank` ✅ (implemented)
- [ ] `rank` ✅ (implemented)
- [ ] `row_number` ✅ (implemented)

### Conditional Functions
- [ ] `coalesce` ✅ (implemented)
- [ ] `greatest`
- [ ] `if` / `iff`
- [ ] `ifnull`
- [ ] `least`
- [ ] `nanvl` ✅ (implemented)
- [ ] `nullif`
- [ ] `nvl`
- [ ] `nvl2`
- [ ] `when`

### Predicate Functions
- [ ] `isnan` ✅ (implemented)
- [ ] `isnull` ✅ (implemented)
- [ ] `isnotnull`
- [ ] `equal_null`

### Hash Functions
- [ ] `crc32`
- [ ] `hash`
- [ ] `md5`
- [ ] `sha` / `sha1`
- [ ] `sha2`
- [ ] `xxhash64`

### Bitwise Functions
- [ ] `bit_and` ✅ (implemented as aggregate)
- [ ] `bit_count`
- [ ] `bit_get`
- [ ] `bit_or` ✅ (implemented as aggregate)
- [ ] `bit_xor` ✅ (implemented as aggregate)
- [ ] `getbit`
- [ ] `shiftleft`
- [ ] `shiftright`
- [ ] `shiftrightunsigned`

### JSON Functions
- [ ] `from_json`
- [ ] `get_json_object`
- [ ] `json_array_length`
- [ ] `json_object_keys`
- [ ] `json_tuple`
- [ ] `schema_of_json`
- [ ] `to_json`

### CSV Functions
- [ ] `from_csv`
- [ ] `schema_of_csv`
- [ ] `to_csv`

### XML Functions
- [ ] `from_xml`
- [ ] `schema_of_xml`
- [ ] `to_xml`
- [ ] `xpath`
- [ ] `xpath_boolean`
- [ ] `xpath_double`
- [ ] `xpath_float`
- [ ] `xpath_int`
- [ ] `xpath_long`
- [ ] `xpath_number`
- [ ] `xpath_short`
- [ ] `xpath_string`

### VARIANT Functions
- [ ] `parse_json`
- [ ] `schema_of_variant`
- [ ] `schema_of_variant_agg`
- [ ] `to_variant_object`
- [ ] `try_parse_json`
- [ ] `variant_get`
- [ ] `variant_explode`

### URL Functions
- [ ] `parse_url`
- [ ] `url_decode`
- [ ] `url_encode`

### Column/Normal Functions
- [ ] `broadcast`
- [ ] `col` / `column` ✅ (implemented)
- [ ] `expr` ✅ (implemented)
- [ ] `input_file_block_length`
- [ ] `input_file_block_start`
- [ ] `input_file_name` ✅ (implemented)
- [ ] `lit` ✅ (implemented)
- [ ] `monotonically_increasing_id` ✅ (implemented)
- [ ] `spark_partition_id` ✅ (implemented)
- [ ] `typedlit`

### Misc Functions
- [ ] `aes_decrypt`
- [ ] `aes_encrypt`
- [ ] `assert_true`
- [ ] `bitmap_bit_position`
- [ ] `bitmap_bucket_number`
- [ ] `bitmap_count`
- [ ] `call_function`
- [ ] `call_udf`
- [ ] `cardinality`
- [ ] `cast`
- [ ] `collate`
- [ ] `collation`
- [ ] `current_catalog`
- [ ] `current_database`
- [ ] `current_schema`
- [ ] `current_user`
- [ ] `hll_sketch_estimate`
- [ ] `hll_union`
- [ ] `java_method`
- [ ] `not` ✅ (implemented)
- [ ] `raise_error`
- [ ] `reflect`
- [ ] `schema_of_json`
- [ ] `stack`
- [ ] `try_aes_decrypt`
- [ ] `typeof`
- [ ] `uuid`
- [ ] `version`

### Partition Transform Functions
- [ ] `bucket`
- [ ] `days`
- [ ] `hours`
- [ ] `months`
- [ ] `years`

### Sort Functions
- [ ] `asc` ✅ (implemented)
- [ ] `asc_nulls_first` ✅ (implemented)
- [ ] `asc_nulls_last` ✅ (implemented)
- [ ] `desc` ✅ (implemented)
- [ ] `desc_nulls_first` ✅ (implemented)
- [ ] `desc_nulls_last` ✅ (implemented)

### UDF Functions
- [ ] `udf` (for creating user-defined functions)
- [ ] `udaf` (for creating user-defined aggregate functions)
- [ ] `call_udf`

## Reference

See upstream implementations for reference:
- [SparkSession.scala](https://github.com/apache/spark/blob/master/sql/connect/common/src/main/scala/org/apache/spark/sql/connect/SparkSession.scala)
- [Dataset.scala](https://github.com/apache/spark/blob/master/sql/connect/common/src/main/scala/org/apache/spark/sql/connect/Dataset.scala)
- [functions.scala](https://github.com/apache/spark/blob/master/sql/connect/common/src/main/scala/org/apache/spark/sql/functions.scala)
