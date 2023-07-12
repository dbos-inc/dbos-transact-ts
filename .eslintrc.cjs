/* eslint-env node */
module.exports = {
    extends: ['eslint:recommended',
              'plugin:@typescript-eslint/recommended',
              'plugin:@typescript-eslint/recommended-type-checked'],
    parser: '@typescript-eslint/parser',
    "parserOptions": { "project": ["./tsconfig.json"] },
    plugins: ['@typescript-eslint'],
    parserOptions: {
      project: true,
      tsconfigRootDir: __dirname,
    },
    root: true,
    ignorePatterns: ['dist/'],
    overrides: [
      {
        files: ['*.js'],
        extends: ['plugin:@typescript-eslint/disable-type-checked'],
      },
    ],
    rules: {
      'indent': ['error', 2],
    },
    "env": {
      "node": true
    },
};