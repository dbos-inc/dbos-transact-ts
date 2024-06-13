const { FlatCompat } = require("@eslint/eslintrc");
const dbosIncEslintPlugin = require("@dbos-inc/eslint-plugin");
const typescriptEslint = require("typescript-eslint");
const typescriptEslintParser = require("@typescript-eslint/parser");
const globals = require("globals");
const js = require("@eslint/js");

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

  ignores: ["**/*.test.ts"]
});
