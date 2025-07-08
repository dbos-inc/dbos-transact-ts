const path = require('path');

module.exports = {
  mode: 'production',
  target: 'node',
  entry: './src/main.ts',
  output: {
    filename: 'bundle.js',
    path: path.resolve(__dirname, 'dist'),
    library: {
      type: 'commonjs2',
    },
  },
  resolve: {
    extensions: ['.ts', '.js'],
    modules: ['node_modules'],
  },
  module: {
    rules: [
      {
        test: /\.ts$/,
        use: 'ts-loader',
        exclude: /node_modules/,
      },
    ],
  },
  externals: {
    // Keep these optional dependencies of Knex as external dependencies to avoid bundling issues
    sqlite3: 'commonjs sqlite3',
    mysql: 'commonjs mysql',
    mysql2: 'commonjs mysql2',
    oracledb: 'commonjs oracledb',
    mssql: 'commonjs mssql',
    'better-sqlite3': 'commonjs better-sqlite3',
    tedious: 'commonjs tedious',
    'pg-query-stream': 'commonjs pg-query-stream',
  },
  optimization: {
    minimize: false, // Disable minification for easier debugging
  },
  devtool: 'source-map',
};
