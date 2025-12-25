/** @type {import('ts-jest').JestConfigWithTsJest} **/
export const testEnvironment = "node";
export const transform = {
  "^.+.tsx?$": ["ts-jest", {}],
};
export const collectCoverage = true;
export const logHeapUsage = true;
export const displayName = {
  name: "TEST",
  color: "blue",
};
export const verbose = true;
export const setupFilesAfterEnv = ["<rootDir>/jest.setup.js"];
const integrationEnabled = !!process.env.SPARK_CONNECT_TEST_URL || !!process.env.SPARK_CONNECT_URL;
export const testPathIgnorePatterns = integrationEnabled ? [] : [
  "tests/org/apache/spark/sql/DataFrame.test.ts",
  "tests/org/apache/spark/sql/DataFrameCollect.test.ts",
  "tests/org/apache/spark/sql/DataFrameAggregate.test.ts",
  "tests/org/apache/spark/sql/DataFrameReaderWriter.test.ts",
  "tests/org/apache/spark/sql/Limit.test.ts",
  "tests/org/apache/spark/sql/SparkSession.test.ts",
  "tests/org/apache/spark/sql/SparkSessionBuilder.test.ts",
  "tests/org/apache/spark/sql/catalog/Catalog.test.ts",
];
