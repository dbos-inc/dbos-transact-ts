const { FlatCompat } = require("@eslint/eslintrc");
const dbosIncEslintPlugin = require("@dbos-inc/eslint-plugin");
const typescriptEslint = require("typescript-eslint");
const typescriptEslintParser = require("@typescript-eslint/parser");
const globals = require("globals");
const js = require("@eslint/js");

/* TODO: for the future, should we still be pulling in this other config stuff?
Since originally, we only used the DBOS recommended config... */
const compat = new FlatCompat({
  baseDirectory: __dirname,
  recommendedConfig: js.configs.recommended
});

module.exports = typescriptEslint.config({
  extends: compat.extends("plugin:@dbos-inc/dbosRecommendedConfig"),
  plugins: { "@dbos-inc": dbosIncEslintPlugin },

  languageOptions: {
    parser: typescriptEslintParser,
    parserOptions: { project: "./tsconfig.json" },
    globals: { ...globals.node, ...globals.es6 }
  },

  rules: {},

  // TODO: somehow, try avoid repeating part of this in `tsconfig.json`
  ignores: [
    "dist",
    "**/*.test.ts",
    "jest.config.js"
  ]
});
