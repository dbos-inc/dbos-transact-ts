const fs = require('fs');
const path = require('path');

// ESM consumers need a .d.mts so the declarations' own imports resolve through the "import" condition.
const declarations = fs.readFileSync('dist/index.d.ts', 'utf8');
// The copied sourceMappingURL would point at a map that names index.d.ts as its file.
fs.writeFileSync('dist/index.d.mts', declarations.replace(/^\/\/# sourceMappingURL=.*\n?/m, ''));

// Those declarations only tell the truth if the "import" condition also resolves to real ESM.
const names = Object.keys(require(path.resolve('dist/index.js'))).filter((name) => name !== '__esModule');
if (names.length === 0) {
  throw new Error('dist/index.js has no runtime exports to re-export');
}
// Re-exporting the CommonJS entry keeps one module instance, so data source registrations stay in one place.
fs.writeFileSync('dist/index.mjs', `export { ${names.sort().join(', ')} } from './index.js';\n`);

// Node discovers CommonJS named exports by static analysis, which can see fewer of them than require() does.
import(path.resolve('dist/index.mjs')).catch((error) => {
  console.error(`dist/index.mjs is not loadable: ${error.message}`);
  process.exit(1);
});
