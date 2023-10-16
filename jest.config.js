/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  testRegex: '((\\.|/)(test|spec))\\.ts?$',
  testPathIgnorePatterns: ['examples/*', 'tests/provenance/*'],
  moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json', 'node'],
  modulePaths: ["./"],
  collectCoverageFrom: [
    'src/**/*.ts',
    '!src/**/index.ts',
  ],
  setupFiles: [
    './jest.setup.ts'
  ],
};
