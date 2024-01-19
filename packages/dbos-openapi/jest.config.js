/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  testRegex: '((\\.|/)(test|spec))\\.ts?$',
  testPathIgnorePatterns: ['examples/*'],
  moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json', 'node'],
  modulePaths: ["./"],
  collectCoverageFrom: [
    '*.ts',
  ],
  setupFiles: [
  ],
};
