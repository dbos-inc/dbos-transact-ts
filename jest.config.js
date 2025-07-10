/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  testRegex: '((\\.|/)(test|spec))\\.(ts|js)?$',
  testPathIgnorePatterns: ['packages/*', 'tests/bundler-test/node_modules', 'tests/bundler-test/dist'],
  moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json', 'node'],
  modulePaths: ['./'],
  modulePathIgnorePatterns: ['tests/proc-test'],
  collectCoverageFrom: ['src/**/*.ts', '!src/**/index.ts'],
  setupFiles: ['./jest.setup.ts'],
};
