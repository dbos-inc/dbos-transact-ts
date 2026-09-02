const fs = require('fs');

// ESM consumers need a .d.mts so the declarations' own imports resolve through the "import" condition.
const declarations = fs.readFileSync('dist/index.d.ts', 'utf8');
// The copied sourceMappingURL would point at a map that names index.d.ts as its file.
fs.writeFileSync('dist/index.d.mts', declarations.replace(/^\/\/# sourceMappingURL=.*\n?/m, ''));
