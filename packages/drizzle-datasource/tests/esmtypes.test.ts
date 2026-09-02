import { execFileSync } from 'child_process';
import { readFileSync } from 'fs';
import path from 'path';

const packageRoot = path.join(__dirname, '..');

describe('ESM type declarations', () => {
  test('an ESM consumer under NodeNext typechecks against the built declarations', () => {
    const tsc = require.resolve('typescript/bin/tsc');
    const project = path.join(__dirname, 'tsconfig.esmcheck.json');
    try {
      execFileSync(process.execPath, [tsc, '--project', project], { cwd: packageRoot, stdio: 'pipe' });
    } catch (error) {
      const { stdout } = error as { stdout?: Buffer };
      throw new Error(`ESM consumer failed to typecheck. Run 'npm run build' first.\n${stdout?.toString() ?? ''}`);
    }
  }, 120000);

  test('the ESM declarations carry no reference to the CommonJS declaration map', () => {
    const declarations = readFileSync(path.join(packageRoot, 'dist', 'index.d.mts'), 'utf8');
    expect(declarations).not.toContain('sourceMappingURL');
  });

  test('the exports map still resolves the package manifest', () => {
    expect(() => require.resolve('@dbos-inc/drizzle-datasource/package.json')).not.toThrow();
  });
});
