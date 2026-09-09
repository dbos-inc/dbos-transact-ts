/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  preset: 'ts-jest',
  transform: {
    '^.+\\.tsx?$': ['ts-jest', { diagnostics: { ignoreCodes: [151002] } }],
    // ESM-only dependencies still have to reach jest's CommonJS runtime, which has no require(esm).
    '^.+\\.m?js$': ['ts-jest', { diagnostics: false, tsconfig: { allowJs: true, module: 'CommonJS' } }],
  },
  transformIgnorePatterns: ['/node_modules/(?!(serialize-error|non-error)/)'],
  testEnvironment: 'node',
  testRegex: '((\\.|/)(test|spec))\\.(ts|js)?$',
  testPathIgnorePatterns: [
    'packages/*',
    'tests/bundler-test/node_modules',
    'tests/bundler-test/dist',
    'tests/esm-test/node_modules',
    'tests/esm-test/dist',
  ],
  moduleFileExtensions: ['ts', 'tsx', 'js', 'jsx', 'json', 'node'],
  modulePaths: ['./'],
  moduleNameMapper: {
    '^@dbos-inc/dbos-sdk$': `<rootDir>/src/index.ts`,
    '^@dbos-inc/dbos-sdk/(.*)$': '<rootDir>/src/$1',
  },
  modulePathIgnorePatterns: ['tests/proc-test'],
  collectCoverageFrom: ['src/**/*.ts', '!src/**/index.ts'],
  setupFiles: ['./jest.setup.ts'],
  testTimeout: 60000,
};
