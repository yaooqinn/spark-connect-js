/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { DataFrame } from './DataFrame';
import { Column } from './Column';

/**
 * Statistic functions for DataFrames.
 */
export class DataFrameStatFunctions {
  constructor(private df: DataFrame) {}

  /**
   * Calculate the sample covariance of two numerical columns of a DataFrame.
   * 
   * @param col1 the name of the first column
   * @param col2 the name of the second column
   * @return the covariance of the two columns
   */
  async cov(col1: string, col2: string): Promise<number> {
    return this.df.spark.relationBuilderToDF(b => b.withStatCov(col1, col2, this.df.plan.relation))
      .head()
      .then(row => row.getDouble(0));
  }

  /**
   * Calculates the correlation of two columns of a DataFrame. 
   * Currently only supports the Pearson Correlation Coefficient.
   * 
   * @param col1 the name of the first column
   * @param col2 the name of the second column
   * @param method Optional. Currently only supports 'pearson'
   * @return the Pearson Correlation Coefficient as a double
   */
  async corr(col1: string, col2: string, method?: string): Promise<number> {
    return this.df.spark.relationBuilderToDF(b => b.withStatCorr(col1, col2, method, this.df.plan.relation))
      .head()
      .then(row => row.getDouble(0));
  }

  /**
   * Computes a pair-wise frequency table of the given columns. Also known as a contingency table.
   * The first column of each row will be the distinct values of `col1` and the column names will 
   * be the distinct values of `col2`. The name of the first column will be `col1_col2`. Counts 
   * will be returned as `Long`s. Pairs that have no occurrences will have zero as their counts.
   * 
   * @param col1 The name of the first column. Distinct items will make the first item of each row.
   * @param col2 The name of the second column. Distinct items will make the column names.
   * @return A DataFrame containing for the contingency table.
   */
  crosstab(col1: string, col2: string): DataFrame {
    return this.df.spark.relationBuilderToDF(b => b.withStatCrosstab(col1, col2, this.df.plan.relation));
  }

  /**
   * Finding frequent items for columns, possibly with false positives. Using the 
   * frequent element count algorithm described in "https://doi.org/10.1145/762471.762473, 
   * proposed by Karp, Schenker, and Papadimitriou". The `support` should be greater than 1e-4.
   * 
   * @param cols the names of the columns to search frequent items in
   * @param support Optional. The minimum frequency for an item to be considered `frequent`. 
   *                Should be greater than 1e-4. Default is 1% (0.01).
   * @return A Local DataFrame with the frequent items in each column.
   */
  freqItems(cols: string[], support?: number): DataFrame {
    return this.df.spark.relationBuilderToDF(b => b.withStatFreqItems(cols, support, this.df.plan.relation));
  }

  /**
   * Returns a stratified sample without replacement based on the fraction given on each stratum.
   * 
   * @param col column that defines strata
   * @param fractions sampling fraction for each stratum. If a stratum is not specified, 
   *                  we treat its fraction as zero.
   * @param seed random seed
   * @return a new DataFrame that represents the stratified sample
   */
  sampleBy(col: Column, fractions: Map<any, number>, seed?: number): DataFrame {
    return this.df.spark.relationBuilderToDF(b => b.withStatSampleBy(col, fractions, seed, this.df.plan.relation));
  }

  /**
   * Calculates the approximate quantiles of numerical columns of a DataFrame.
   * 
   * The result will be a DataFrame with the same number of columns as `cols`, where each 
   * column contains the approximate quantiles for the corresponding input column.
   * 
   * @param cols the names of the numerical columns
   * @param probabilities a list of quantile probabilities. Each number must belong to [0, 1].
   *                      For example 0 is the minimum, 0.5 is the median, 1 is the maximum.
   * @param relativeError The relative target precision to achieve (greater than or equal to 0).
   *                      If set to zero, the exact quantiles are computed, which could be very expensive.
   *                      Note that values greater than 1 are accepted but give the same result as 1.
   * @return a DataFrame with the approximate quantiles
   */
  approxQuantile(cols: string[], probabilities: number[], relativeError: number): DataFrame {
    return this.df.spark.relationBuilderToDF(b => b.withStatApproxQuantile(cols, probabilities, relativeError, this.df.plan.relation));
  }
}
