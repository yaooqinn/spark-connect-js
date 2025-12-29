const path = require('path');

module.exports = {
  entry: './src/org/apache/spark/sql/SparkSession.ts',
  output: {
    filename: 'spark-connect.js',
    path: path.resolve(__dirname, 'dist'),
    library: {
      name: 'SparkConnect',
      type: 'umd',
      export: 'default',
    },
    globalObject: 'this',
  },
  module: {
    rules: [
      {
        test: /\.tsx?$/,
        use: 'ts-loader',
        exclude: /node_modules/,
      },
    ],
  },
  resolve: {
    extensions: ['.tsx', '.ts', '.js'],
    fallback: {
      // Disable Node.js built-in modules for browser
      "fs": false,
      "path": false,
      "os": false,
      "crypto": false,
      "stream": false,
      "buffer": false,
    }
  },
  target: 'web',
  mode: 'production',
};
