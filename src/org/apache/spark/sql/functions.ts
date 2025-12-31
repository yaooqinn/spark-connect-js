import { Column } from "./Column";
import { toLiteralBuilder } from "./proto/expression/utils";
import { randomInt } from "./util/helpers";

// TODOs:
// 1. broadcast

/**
 * Returns a [[Column]] based on the given column name.
 *
 * @group normal_funcs
 * @param colName column name
 * @returns {Column}
 */
export function col(colName: string): Column {
  return new Column(colName);
}

/**
 * Returns a [[Column]] based on the given column name. Alias of [[col]].
 *
 * @group normal_funcs
 * @param colName column name
 * @returns {Column}
 */
export function column(colName: string): Column {
  return col(colName);
}

/**
 * Creates a Column of literal value.
 * 
 * @remarks
 * The literal value can be of various types including primitives, dates, and Column instances.
 * If a Column is passed, it will be returned as-is.
 * 
 * @param v - The literal value (string, number, boolean, bigint, Date, null, or Column)
 * @returns A Column representing the literal value
 * 
 * @example
 * ```typescript
 * df.select(lit(5), lit("hello"), lit(true), lit(new Date()))
 * ```
 * 
 * @group normal_funcs
 */
export function lit(v: any): Column {
  if (v instanceof Column) {
    return v;
  } else {
    return new Column(b => b.withLiteral(toLiteralBuilder(v).builder.build()));
  }
}

/**
 * Creates a Column of integer literal value.
 * 
 * @param v - The integer value
 * @returns A Column representing the integer literal
 * 
 * @group normal_funcs
 */
export function int(v: number): Column {
  return new Column(b => b.withLiteralBuilder(l => l.withInt(v)));
}

/**
 * Creates a Column of long literal value (64-bit integer).
 * 
 * @param v - The long value as number or bigint
 * @returns A Column representing the long literal
 * 
 * @group normal_funcs
 */
export function long(v: number | bigint): Column {
  return new Column(b => b.withLiteralBuilder(l => l.withLong(v)));
}

/**
 * Returns a sort expression based on ascending order of the column.
 * {{{
 *   df.sort(asc("dept"), desc("age"))
 * }}}
 *
 * @group sort_funcs
 */
export function asc(columnName: string): Column {
  return new Column(columnName).asc;
}

/**
 * Returns a sort expression based on ascending order of the column, and null values return
 * before non-null values.
 * {{{
 *   df.sort(asc_nulls_first("dept"), desc("age"))
 * }}}
 *
 * @group sort_funcs
 */
export function asc_nulls_first(columnName: string): Column {
  return new Column(columnName).asc_nulls_first;
}

/**
 * Returns a sort expression based on ascending order of the column, and null values appear
 * after non-null values.
 * {{{
 *   df.sort(asc_nulls_last("dept"), desc("age"))
 * }}}
 *
 * @group sort_funcs
 */
export function asc_nulls_last(columnName: string): Column {
  return new Column(columnName).asc_nulls_last;
}

/**
 * Returns a sort expression based on the descending order of the column.
 * {{{
 *   df.sort(asc("dept"), desc("age"))
 * }}}
 *
 * @group sort_funcs
 */
export function desc(columnName: string): Column {
  return new Column(columnName).desc;
}

/**
 * Returns a sort expression based on the descending order of the column, and null values appear
 * before non-null values.
 * {{{
 *   df.sort(asc("dept"), desc_nulls_first("age"))
 * }}}
 *
 * @group sort_funcs
 */
export function desc_nulls_first(columnName: string): Column {
  return new Column(columnName).desc_nulls_first;
}

/**
 * Returns a sort expression based on the descending order of the column, and null values appear
 * after non-null values.
 * {{{
 *   df.sort(asc("dept"), desc_nulls_last("age"))
 * }}}
 *
 * @group sort_funcs
 */
export function desc_nulls_last(columnName: string): Column {
  return new Column(columnName).desc_nulls_last;
}

/**
 * Aggregate function: returns the approximate number of distinct items in a group.
 *
 * @param rsd
 *   maximum relative standard deviation allowed (default = 0.05)
 *
 * @group agg_funcs
 */
export function approx_count_distinct(columnName: string): Column;
export function approx_count_distinct(columnName: string, rsd: number): Column;
export function approx_count_distinct(column: Column): Column;
export function approx_count_distinct(column: Column, rsd: number): Column;
export function approx_count_distinct(column: string | Column, rsd?: number): Column {
  if (rsd === undefined) {
    return Column.fn("approx_count_distinct", column, false);
  } else {
    return Column.fn("approx_count_distinct",column, false, lit(rsd));
  }
}

/**
 * Aggregate function: returns the average of the values in a group.
 *
 * @group agg_funcs
 */
export function avg(column: string | Column): Column {
  return Column.fn("avg", column, false);
}

/**
 * Aggregate function: returns a list of objects with duplicates.
 *
 * @note
 *   The function is non-deterministic because the order of collected results depends on the
 *   order of the rows which may be non-deterministic after a shuffle.
 *
 * @group agg_funcs
 * @since 1.6.0
 */
export function collect_list(column: string | Column): Column {
  return Column.fn("collect_list", column, false);
}

/**
 * Aggregate function: returns a set of objects with duplicate elements eliminated.
 *
 * @note
 *   The function is non-deterministic because the order of collected results depends on the
 *   order of the rows which may be non-deterministic after a shuffle.
 *
 * @group agg_funcs
 */
export function collect_set(column: string | Column): Column {
  return Column.fn("collect_set", column, false);
}

export function count_min_sketch(e: Column, eps: Column, confidence: Column, seed?: Column): Column {
  return Column.fn("count_min_sketch", e, false, eps, confidence, seed ?? long(randomInt()));
}


/**
 * Aggregate function: returns the Pearson Correlation Coefficient for two columns.
 *
 * @group agg_funcs
 */
export function corr(column1: string, column2: string): Column;
export function corr(column1: Column, column2: Column): Column;
export function corr(column1: string | Column, column2: string | Column): Column {
  return Column.fn("corr", column1, false, typeof column2 === "string" ? col(column2) : column2);
}

export function count(column: string | Column): Column {
  return Column.fn("count", column, false);
}

  /**
   * Aggregate function: returns the number of distinct items in a group.
   *
   * @group agg_funcs
   */
export function countDistinct(column: string, ...cols: string[]): Column;
export function countDistinct(column: Column, ...cols: Column[]): Column;
export function countDistinct(column: string | Column, ...cols: (string | Column)[]): Column {
  if (typeof column === "string") {
    return count_distinct(col(column), ...cols.map(c => typeof c === "string" ? col(c) : c));
  } else {
    return count_distinct(column, ...cols.map(c => typeof c === "string" ? col(c) : c));
  }
}
/**
 * Aggregate function: returns the number of distinct items in a group.
 *
 * @group agg_funcs
 */
export function count_distinct(column: Column, ...cols: Column[]): Column {
  return Column.fn("count", column, true, ...cols);
}

/**
 * Aggregate function: returns the population covariance for two columns.
 *
 * @group agg_funcs
 */
export function covar_pop(column1: string, column2: string): Column;
/**
 * Aggregate function: returns the population covariance for two columns.
 *
 * @group agg_funcs
 */
export function covar_pop(column1: Column, column2: Column): Column;
export function covar_pop(column1: string | Column, column2: string | Column): Column {
  return Column.fn("covar_pop", column1, false, typeof column2 === "string" ? col(column2) : column2);
}

/**
 * Aggregate function: returns the sample covariance for two columns.
 *
 * @group agg_funcs
 */
export function covar_samp(column1: string, column2: string): Column;
export function covar_samp(column1: Column, column2: Column): Column;
export function covar_samp(column1: string | Column, column2: string | Column): Column {
  return Column.fn("covar_samp", column1, false, typeof column2 === "string" ? col(column2) : column2);
}

/**
 * Aggregate function: returns the first value in a group.
 *
 * The function by default returns the first values it sees. It will return the first non-null
 * value it sees when ignoreNulls is set to true. If all values are null, then null is returned.
 *
 * @note
 *   The function is non-deterministic because its results depends on the order of the rows
 *   which may be non-deterministic after a shuffle.
 *
 * @group agg_funcs
 */
export function first(column: string): Column;
export function first(column: Column): Column;
export function first(column: string, ignoreNulls: boolean): Column;
export function first(column: Column, ignoreNulls: boolean): Column;
export function first(column: string | Column, ignoreNulls?: boolean): Column {
  return Column.fn("first", column, false, lit(ignoreNulls ?? false));
}

/**
 * Aggregate function: returns the first value in a group.
 *
 * @note
 *   The function is non-deterministic because its results depends on the order of the rows
 *   which may be non-deterministic after a shuffle.
 *
 * @group agg_funcs
 * @since 3.5.0
 */
export function first_value(e: Column): Column;
export function first_value(e: Column, ignoreNulls: boolean): Column;
export function first_value(e: Column, ignoreNulls?: boolean): Column {
  return Column.fn("first_value", e, false, lit(ignoreNulls ?? false));
}

/**
 * Aggregate function: indicates whether a specified column in a GROUP BY list is aggregated or
 * not, returns 1 for aggregated or 0 for not aggregated in the result set.
 *
 * @group agg_funcs
 * @since 2.0.0
 */
export function grouping(column: string): Column;
export function grouping(column: Column): Column;
export function grouping(column: string | Column): Column {
  return Column.fn("grouping", column, false);
}

/**
 * Aggregate function: returns the level of grouping, equals to
 *
 * {{{
 *   (grouping(c1) <<; (n-1)) + (grouping(c2) <<; (n-2)) + ... + grouping(cn)
 * }}}
 *
 * @note
 *   The list of columns should match with grouping columns exactly, or empty (means all the
 *   grouping columns).
 *
 * @group agg_funcs
 */
export function grouping_id(...columns: string[]): Column;
export function grouping_id(...columns: Column[]): Column;
export function grouping_id(...columns: (string | Column)[]): Column {
  return new Column(b => b.withUnresolvedFunction(
    "grouping_id", columns.map(c => typeof c === "string" ? col(c).expr : c.expr), false, false));
}


export function hll_sketch_agg(column: string): Column;
export function hll_sketch_agg(column: Column): Column;
export function hll_sketch_agg(column: string, lgConfigK: number): Column;
export function hll_sketch_agg(column: Column, lgConfigK: number): Column;
export function hll_sketch_agg(column: Column, lgConfigK: Column): Column;
export function hll_sketch_agg(column: string | Column, lgConfigK?: number | Column): Column {
  if (lgConfigK === undefined) {
    return Column.fn("hll_sketch_agg", column, false);
  } else {
    return Column.fn("hll_sketch_agg", column, false, typeof lgConfigK === "number" ? int(lgConfigK) : lgConfigK);
  }
}

export function hll_union_agg(column: string): Column;
export function hll_union_agg(column: Column): Column;
export function hll_union_agg(column: string, allowDifferentLgConfigK: boolean): Column;
export function hll_union_agg(column: Column, allowDifferentLgConfigK: boolean): Column;
export function hll_union_agg(column: Column, allowDifferentLgConfigK: Column): Column;
export function hll_union_agg(column: string | Column, allowDifferentLgConfigK?: boolean | Column): Column {
  if (allowDifferentLgConfigK === undefined) {
    return Column.fn("hll_union_agg", column, false);
  } else {
    return Column.fn("hll_union_agg", column, false, typeof allowDifferentLgConfigK === "boolean" ? lit(allowDifferentLgConfigK) : allowDifferentLgConfigK);
  }
}

export function kurtosis(column: string): Column;
export function kurtosis(column: Column): Column;
export function kurtosis(column: string | Column): Column {
  return Column.fn("kurtosis", column, false);
}

export function last(column: string): Column;
export function last(column: Column): Column;
export function last(column: string, ignoreNulls: boolean): Column;
export function last(column: Column, ignoreNulls: boolean): Column;
export function last(column: string | Column, ignoreNulls?: boolean): Column {
  return Column.fn("last", column, false, lit(ignoreNulls ?? false));
}

export function last_value(e: Column): Column;
export function last_value(e: Column, ignoreNulls: boolean): Column;
export function last_value(e: Column, ignoreNulls?: boolean): Column {
  return Column.fn("last_value", e, false, lit(ignoreNulls ?? false));
}

export function mode(column: Column): Column;
export function mode(column: Column, deterministic: boolean): Column;
export function mode(column: Column, deterministic?: boolean): Column {
  return Column.fn("mode", column, false, lit(deterministic ?? false));
}

export function max(column: string | Column): Column {
  return Column.fn("max", column, false);
}

/**
 * Aggregate function: returns the value associated with the maximum value of ord.
 *
 * @note
 *   The function is non-deterministic so the output order can be different for those associated
 *   the same values of `e`.
 *
 * @group agg_funcs
 */
export function max_by(e: Column, ord: Column): Column {
  return Column.fn("max_by", e, false, ord);
}

/**
 * Aggregate function: returns the average of the values in a group. Alias for avg.
 *
 * @group agg_funcs
 */
export function mean(column: string | Column): Column {
  return avg(column);
}

/**
 * Aggregate function: returns the median of the values in a group.
 *
 * @group agg_funcs
 * @since 3.4.0
 */
export function median(column: string | Column): Column {
  return Column.fn("median", column, false);
}

/**
 * Aggregate function: returns the minimum value of the expression in a group.
 *
 * @group agg_funcs
 * @since 1.3.0
 */
export function min(column: string | Column): Column {
  return Column.fn("min", column, false);
}

export function min_by(e: Column, ord: Column): Column {
  return Column.fn("min_by", e, false, ord);
}

export function percentile(e: Column, percentage: Column, frequency?: Column): Column {
  if (frequency === undefined) {
    return Column.fn("percentile", e, false, percentage);
  } else {
    return Column.fn("percentile", e, false, percentage, frequency);
  }
}

export function percentile_approx(e: Column, percentage: Column, accuracy: Column): Column {
  return Column.fn("percentile_approx", e, false, percentage, accuracy);
}

export function approx_percentile(e: Column, percentage: Column, accuracy: Column): Column {
  return Column.fn("approx_percentile", e, false, percentage, accuracy);
}

/**
 * Aggregate function: returns the product of all numerical elements in a group.
 *
 * @group agg_funcs
 */
export function product(column: Column): Column {
  return Column.fn("product", column, false);
}

export function skewness(column: string | Column): Column {
  return Column.fn("skewness", column, false);
}

export function std(column: Column): Column {
  return Column.fn("stddev", column, false);
}
export function stddev(column: string | Column): Column {
  return Column.fn("stddev", column, false);
}

export function stddev_sample(column: string | Column): Column {
  return stddev_samp(column);
}

export function stddev_samp(column: string | Column): Column {
  return Column.fn("stddev_samp", column, false);
}

export function stddev_pop(column: string | Column): Column {
  return Column.fn("stddev_pop", column, false);
}

export function sum(column: string | Column): Column {
  return Column.fn("sum", column, false);
}

export function sum_distinct(column: string | Column): Column {
  return Column.fn("sum", column, true);
}

export function listagg(column: Column, separator?: string): Column {
  if (separator === undefined) {
    return Column.fn("listagg", column, false);
  } else {
    return Column.fn("listagg", column, false, lit(separator));
  }
}

export function listagg_distinct(column: Column, separator?: string): Column {
  if (separator === undefined) {
    return Column.fn("listagg", column, true);
  } else {
    return Column.fn("listagg", column, true, lit(separator));
  }
}

export function string_agg(column: Column, separator?: string): Column {
  return listagg(column, separator);
}

export function string_agg_distinct(column: Column, separator?: string): Column {
  return listagg_distinct(column, separator);
}

export function variance(column: string | Column): Column {
  return Column.fn("variance", column, false);
}

export function var_samp(column: string | Column): Column {
  return Column.fn("var_samp", column, false);
}

export function var_pop(column: string | Column): Column {
  return Column.fn("var_pop", column, false);
}

export function var_pop_distinct(column: string | Column): Column {
  return Column.fn("var_pop", column, true);
}

export function regr_avgx(column1: Column, column2: Column): Column {
  return Column.fn("regr_avgx", column1, false, column2);
}

export function regr_avgy(column1: Column, column2: Column): Column {
  return Column.fn("regr_avgy", column1, false, column2);
}

export function regr_count(column1: Column, column2: Column): Column {
  return Column.fn("regr_count", column1, false, column2);
}

export function regr_intercept(column1: Column, column2: Column): Column {
  return Column.fn("regr_intercept", column1, false, column2);
}

export function regr_r2(column1: Column, column2: Column): Column {
  return Column.fn("regr_r2", column1, false, column2);
}

export function regr_slope(column1: Column, column2: Column): Column {
  return Column.fn("regr_slope", column1, false, column2);
}

export function regr_sxx(column1: Column, column2: Column): Column {
  return Column.fn("regr_sxx", column1, false, column2);
}

export function regr_sxy(column1: Column, column2: Column): Column {
  return Column.fn("regr_sxy", column1, false, column2);
}

export function regr_syy(column1: Column, column2: Column): Column {
  return Column.fn("regr_syy", column1, false, column2);
}

export function any_value(column: string | Column, ignoreNulls: boolean = false): Column {
  return Column.fn("any_value", column, false, lit(ignoreNulls));
}

export function count_if(column: Column): Column {
  return Column.fn("count_if", column, false);
}

export function histogram_numeric(e: Column, nBins: Column): Column {
  return Column.fn("histogram_numeric", e, false, nBins);
}

export function every(column: Column): Column {
  return Column.fn("every", column, false);
}

export function bool_and(column: Column): Column {
  return Column.fn("bool_and", column, false);
}

export function some(column: Column): Column {
  return Column.fn("some", column, false);
}

export function any(column: Column): Column {
  return Column.fn("any", column, false);
}

export function bool_or(column: Column): Column {
  return Column.fn("bool_or", column, false);
}

export function bit_and(column: Column): Column {
  return Column.fn("bit_and", column, false);
}

export function bit_or(column: Column): Column {
  return Column.fn("bit_or", column, false);
}

export function bit_xor(column: Column): Column {
  return Column.fn("bit_xor", column, false);
}

export function cume_dist(column: Column): Column {
  return Column.fn("cume_dist", column, false);
}

export function dense_rank(column: Column): Column {
  return Column.fn("dense_rank", column, false);
}

/**
 * Window function: returns the value that is `offset` rows before the current row.
 * 
 * @param column - The column name or Column to compute lag for
 * @param offset - The number of rows to look back
 * @returns A Column representing the lag value
 * 
 * @example
 * ```typescript
 * df.select(lag(col("value"), 1))
 * ```
 * 
 * @group window_funcs
 */
export function lag(column: string | Column, offset: number): Column;
/**
 * Window function: returns the value that is `offset` rows before the current row, with default value.
 * 
 * @param column - The column name or Column to compute lag for
 * @param offset - The number of rows to look back
 * @param defaultValue - Default value when the offset is beyond the window
 * @returns A Column representing the lag value
 * 
 * @group window_funcs
 */
export function lag(column: string | Column, offset: number, defaultValue: any): Column;
/**
 * Window function: returns the value that is `offset` rows before the current row, with default value and null handling.
 * 
 * @param column - The column name or Column to compute lag for
 * @param offset - The number of rows to look back
 * @param defaultValue - Default value when the offset is beyond the window
 * @param ignoreNulls - Whether to skip null values
 * @returns A Column representing the lag value
 * 
 * @group window_funcs
 */
export function lag(column: string | Column, offset: number, defaultValue: any, ignoreNulls: boolean): Column;
export function lag(column: string | Column, offset: number, defaultValue?: any, ignoreNulls?: boolean): Column {
  if (defaultValue === undefined) {
    return Column.fn("lag", column, false, int(offset));
  } else if (ignoreNulls === undefined) {
    return Column.fn("lag", column, false, int(offset), lit(defaultValue));
  } else {
    return Column.fn("lag", column, false, int(offset), lit(defaultValue), lit(ignoreNulls));
  }
}

/**
 * Window function: returns the value that is `offset` rows after the current row.
 * 
 * @param column - The column name or Column to compute lead for
 * @param offset - The number of rows to look ahead
 * @returns A Column representing the lead value
 * 
 * @example
 * ```typescript
 * df.select(lead(col("value"), 1))
 * ```
 * 
 * @group window_funcs
 */
export function lead(column: string | Column, offset: number): Column;
/**
 * Window function: returns the value that is `offset` rows after the current row, with default value.
 * 
 * @param column - The column name or Column to compute lead for
 * @param offset - The number of rows to look ahead
 * @param defaultValue - Default value when the offset is beyond the window
 * @returns A Column representing the lead value
 * 
 * @group window_funcs
 */
export function lead(column: string | Column, offset: number, defaultValue: any): Column;
/**
 * Window function: returns the value that is `offset` rows after the current row, with default value and null handling.
 * 
 * @param column - The column name or Column to compute lead for
 * @param offset - The number of rows to look ahead
 * @param defaultValue - Default value when the offset is beyond the window
 * @param ignoreNulls - Whether to skip null values
 * @returns A Column representing the lead value
 * 
 * @group window_funcs
 */
export function lead(column: string | Column, offset: number, defaultValue: any, ignoreNulls: boolean): Column;
export function lead(column: string | Column, offset: number, defaultValue?: any, ignoreNulls?: boolean): Column {
  if (defaultValue === undefined) {
    return Column.fn("lead", column, false, int(offset));
  } else if (ignoreNulls === undefined) {
    return Column.fn("lead", column, false, int(offset), lit(defaultValue));
  } else {
    return Column.fn("lead", column, false, int(offset), lit(defaultValue), lit(ignoreNulls));
  }
}

export function nth_value(column: string | Column, offset: number): Column;
export function nth_value(column: string | Column, offset: number, ignoreNulls: boolean): Column;
export function nth_value(column: string | Column, offset: number, ignoreNulls?: boolean): Column {
  return Column.fn("nth_value", column, false, int(offset), lit(ignoreNulls ?? false));
}

export function ntile(n: number): Column {
  return Column.fn("ntile", int(n), false);
}

export function percent_rank(): Column {
  return new Column(b => b.withUnresolvedFunction("percent_rank", [], false, false));
}

export function rank(): Column {
  return new Column(b => b.withUnresolvedFunction("rank", [], false, false));
}

export function row_number(): Column {
  return new Column(b => b.withUnresolvedFunction("row_number", [], false, false));
}

export function array(...columns: (string | Column)[]): Column {
  return new Column(b => b.withUnresolvedFunction("array", columns.map(c => typeof c === "string" ? col(c).expr : c.expr), false, false));
}

export function map(...columns: Column[]): Column {
  return new Column(b => b.withUnresolvedFunction("map", columns.map(c => c.expr), false, false));
}

export function named_struct(...columns: Column[]): Column {
  return new Column(b => b.withUnresolvedFunction("named_struct", columns.map(c => c.expr), false, false));
}

export function map_from_arrays(key: Column, value: Column): Column {
  return new Column(b => b.withUnresolvedFunction("map_from_arrays", [key.expr, value.expr], false, false));
}

export function str_to_map(text: Column): Column;
export function str_to_map(text: Column, pairDelim: Column): Column;
export function str_to_map(text: Column, pairDelim: Column, keyValueDelim: Column): Column;
export function str_to_map(text: Column, pairDelim?: Column, keyValueDelim?: Column): Column {
  if (pairDelim === undefined) {
    return Column.fn("str_to_map", text, false);
  } else if (keyValueDelim === undefined) {
    return Column.fn("str_to_map", text, false, pairDelim);
  } else {
    return Column.fn("str_to_map", text, false, pairDelim, keyValueDelim);
  }
}

export function coalesce(...columns: Column[]): Column {
  return new Column(b => b.withUnresolvedFunction("coalesce", columns.map(c => c.expr), false, false));
}

export function input_file_name(): Column {
  return new Column(b => b.withUnresolvedFunction("input_file_name", [], false, false));
}

export function isnan(column: Column): Column {
  return column.isNaN;
}

export function isnull(column: Column): Column {
  return column.isNull;
}

export function monotonically_increasing_id(): Column {
  return new Column(b => b.withUnresolvedFunction("monotonically_increasing_id", [], false, false));
}

export function nanvl(column1: Column, column2: Column): Column {
  return Column.fn("nanvl", column1, false, column2);
}

export function negate(column: Column): Column {
  return new Column(b => b.withUnresolvedFunction("negate", [column.expr], false, false));
}

export function not(column: Column): Column {
  return new Column(b => b.withUnresolvedFunction("!", [column.expr], false, false));
}

export function positive(column: Column): Column {
  return new Column(b => b.withUnresolvedFunction("+", [column.expr], false, false));
}

export function rand(seed?: number): Column {
  return seed === undefined ? new Column(b => b.withUnresolvedFunction("rand", [], false, false)) : Column.fn("rand", long(seed), false);
}

export function randn(seed?: number): Column {
  return seed === undefined ? new Column(b => b.withUnresolvedFunction("randn", [], false, false)) : Column.fn("randn", long(seed), false);
}

export function randstr(length: Column, seed?: Column): Column {
  return seed === undefined ? Column.fn("randstr", length, false) : Column.fn("randstr", length, false, seed);
}

export function spark_partition_id(): Column {
  return new Column(b => b.withUnresolvedFunction("spark_partition_id", [], false, false));
}

export function sqrt(column: string | Column): Column {
  return Column.fn("sqrt", column, false);
}

export function pow(l: Column, r: Column): Column;
export function pow(l: Column, r: number): Column;
export function pow(l: Column, r: string): Column;
export function pow(l: number, r: Column): Column;
export function pow(l: number, r: string): Column;
export function pow(l: number, r: number): Column;
export function pow(l: string, r: Column): Column;
export function pow(l: string, r: number): Column;
export function pow(l: string, r: string): Column;
export function pow(l: Column | number | string, r: Column | number | string): Column {
  return Column.fn(
    "pow",
    typeof l === "number" ? lit(l) : typeof l === "string" ? col(l) : l,
    false,
    typeof r === "number" ? lit(r) : typeof r === "string" ? col(r) : r);
}

export function power(l: Column, r: Column): Column {
  return Column.fn("power", l, false, r);
}

export function pmod(dividend: Column, divisor: Column): Column {
  return Column.fn("pmod", dividend, false, divisor);
}

export function rint(column: Column): Column {
  return Column.fn("rint", column, false);
}

// TODO: add more functions

/**
 * Parses the expression string into the column that it represents, similar to
 * [[DataFrame#selectExpr]].
 *
 * {{{
 *   // get the number of words of each length
 *   df.groupBy(expr("length(word)")).count()
 * }}}
 *
 */
export function expr(expr: string): Column {
  return new Column(b => b.withExpressionString(expr));
}
