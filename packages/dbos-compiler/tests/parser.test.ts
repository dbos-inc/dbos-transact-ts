import { parseDbosMethodInfo, parseDecoratorArgument } from '../compiler';
import { sampleDbosClass, sampleDbosClassAliased, storedProcParam, testCodeTypes } from './test-code';
import { makeTestProject } from './test-utility';
import { suite, test } from 'node:test';
import assert from 'node:assert/strict';

suite('parser', async () => {
  const { project } = makeTestProject(sampleDbosClass);
  const { project: aliasProject } = makeTestProject(sampleDbosClassAliased);

  const projectTests = [
    {
      name: 'normal',
      project,
    },
    {
      name: 'aliased',
      project: aliasProject,
    },
  ];

  for (const { name, project } of projectTests) {
    await suite(`parseDbosMethodInfo ${name}`, async () => {
      const cls = project.getSourceFileOrThrow('operations.ts').getClassOrThrow('Test');
      const map = new Map(cls.getStaticMethods().map((m) => [m.getName(), parseDbosMethodInfo(m)]));
      for (const [name, type] of testCodeTypes) {
        await test(name, () => {
          const actual = map.get(name);
          assert.notEqual(actual, undefined, `Method ${name} not found`);
          assert.deepEqual(actual, { kind: type, version: 2 }, `Unexpected kind for method ${name}`);
        });
      }
    });
  }

  await suite('parseDecoratorArgument', async () => {
    const cls = project.getSourceFileOrThrow('operations.ts').getClassOrThrow('Test');

    for (const [name, param] of storedProcParam) {
      await test(`${name}`, () => {
        const method = cls.getStaticMethodOrThrow(name);
        const args = method.getDecorators().flatMap((d) => d.getCallExpressionOrThrow().getArguments());
        assert.equal(args.length, 1, `Expected one argument for ${name}`);
        const actual = parseDecoratorArgument(args[0]);
        assert.deepEqual(actual, param, `Unexpected argument for ${name}`);
      });
    }
  });
}).catch(assert.fail);
