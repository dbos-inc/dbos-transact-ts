/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  // Note, we're using "check" instead of the more typical "test|spec" here so that the test project tests
  // don't get picked up by the main project's tests.
  testRegex: '((\\.|/)(check))\\.ts?$',
  moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json', 'node'],
  modulePaths: ["./"],
};
