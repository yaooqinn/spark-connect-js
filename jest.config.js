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
