/** @type {import('ts-jest').JestConfigWithTsJest} **/
module.exports = {
  testEnvironment: "node",
  transform: {
    "^.+.tsx?$": ["ts-jest",{}],
  },
  collectCoverage: true,
  logHeapUsage: true,
  displayName: {
    name: "TEST",
    color: "blue",
  },
  verbose: true
};