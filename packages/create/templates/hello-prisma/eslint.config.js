const { FlatCompat } = require("@eslint/eslintrc");
const dbosIncEslintPlugin = require("@dbos-inc/eslint-plugin");
const typescriptEslintParser = require("@typescript-eslint/parser");
const globals = require("globals");
const js = require("@eslint/js");

const compat = new FlatCompat({
  baseDirectory: __dirname,
  recommendedConfig: js.configs.recommended,
});

module.exports = [
  ...compat.extends("plugin:@dbos-inc/dbosRecommendedConfig"),
  { plugins: { "@dbos-inc": dbosIncEslintPlugin } },
  { languageOptions: {
    parser: typescriptEslintParser,
    parserOptions: { project: "./tsconfig.json" },
    globals: { ...globals.node, ...globals.es6 }
  } },
  { rules: {} },
  { ignores: [
    "dist",
    "**/*.test.ts",
    "jest.config.js",
    "generate_env.js",
    "start_postgres_docker.js"
  ] }
];
