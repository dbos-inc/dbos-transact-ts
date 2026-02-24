/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  preset: 'ts-jest',
  transform: {
    '^.+\\.tsx?$': ['ts-jest', { diagnostics: { ignoreCodes: [151002] } }],
  },
  testEnvironment: 'node',
  testRegex: '((\\.|/)(test|spec))\\.(ts|js)?$',
  testPathIgnorePatterns: ['packages/*', 'tests/bundler-test/node_modules', 'tests/bundler-test/dist'],
  moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json', 'node'],
  modulePaths: ['./'],
  moduleNameMapper: {
    '^@dbos-inc/dbos-sdk$': `<rootDir>/src/index.ts`,
    '^@dbos-inc/dbos-sdk/(.*)$': '<rootDir>/src/$1',
  },
  modulePathIgnorePatterns: ['tests/proc-test'],
  collectCoverageFrom: ['src/**/*.ts', '!src/**/index.ts'],
  setupFiles: ['./jest.setup.ts'],
};
