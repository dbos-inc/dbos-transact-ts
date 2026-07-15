import { suite, test } from 'node:test';
import assert from 'node:assert/strict';

import { applyDBOSConsumerConfig } from '..';

// Offset-config coercion: pure config handling, so this suite touches neither a broker nor a
// database. It lives apart from the launch-validation suite for that reason — these are the
// cheapest, most deterministic tests in the package and shouldn't hinge on infrastructure.
const GENERATED_GROUP = 'generated-group';

suite('confluent-kafka-receive-config', async () => {
  await test('enable.auto.commit=false is overridden so stored offsets are actually committed', () => {
    for (const falsey of [false, 'false', 'FALSE', '0']) {
      const resolved = applyDBOSConsumerConfig(
        { 'group.id': 'g', 'enable.auto.commit': falsey } as never,
        250,
        GENERATED_GROUP,
      );
      assert.equal(resolved['enable.auto.commit'], true, `not overridden for ${JSON.stringify(falsey)}`);
    }
  });

  await test('a top-level enable.auto.commit=false wins over kafkaJS.autoCommit, so it is fixed', () => {
    // The client assigns top-level librdkafka config over the kafkaJS block, so a config that looks
    // like it commits (kafkaJS.autoCommit=true) would silently not.
    const resolved = applyDBOSConsumerConfig(
      { 'group.id': 'g', 'enable.auto.commit': false, kafkaJS: { autoCommit: true } } as never,
      250,
      GENERATED_GROUP,
    );
    assert.equal(resolved['enable.auto.commit'], true);
    assert.equal(resolved.kafkaJS?.autoCommit, true);
  });

  await test('kafkaJS.autoCommit=false is overridden too', () => {
    const resolved = applyDBOSConsumerConfig(
      { 'group.id': 'g', kafkaJS: { autoCommit: false } } as never,
      250,
      GENERATED_GROUP,
    );
    assert.equal(resolved.kafkaJS?.autoCommit, true);
    assert.equal(resolved['enable.auto.commit'], true);
  });

  await test('enable.auto.offset.store is dropped, since the client owns offset storage', () => {
    const resolved = applyDBOSConsumerConfig(
      { 'group.id': 'g', 'enable.auto.offset.store': true } as never,
      250,
      GENERATED_GROUP,
    );
    assert.ok(!('enable.auto.offset.store' in resolved));
  });

  await test('batchSize sets the client batch cap, which otherwise defaults to 32', () => {
    assert.equal(
      applyDBOSConsumerConfig({ 'group.id': 'g' } as never, 250, GENERATED_GROUP)['js.consumer.max.batch.size'],
      250,
    );
    // An explicit caller value wins.
    assert.equal(
      applyDBOSConsumerConfig({ 'group.id': 'g', 'js.consumer.max.batch.size': 7 } as never, 250, GENERATED_GROUP)[
        'js.consumer.max.batch.size'
      ],
      7,
    );
  });

  await test('auto.offset.reset defaults to earliest but the caller can override it', () => {
    assert.equal(
      applyDBOSConsumerConfig({ 'group.id': 'g' } as never, 250, GENERATED_GROUP)['auto.offset.reset'],
      'earliest',
    );
    assert.equal(
      applyDBOSConsumerConfig({ 'group.id': 'g', 'auto.offset.reset': 'latest' } as never, 250, GENERATED_GROUP)[
        'auto.offset.reset'
      ],
      'latest',
    );
    // The kafkaJS spelling counts as the caller having chosen, since the client resolves it into
    // this very key: defaulting over it would silently replay the backlog it asked to skip.
    assert.equal(
      applyDBOSConsumerConfig({ kafkaJS: { groupId: 'g', fromBeginning: false } } as never, 250, GENERATED_GROUP)[
        'auto.offset.reset'
      ],
      undefined,
    );
  });

  await test('a config without group.id gets the generated one the workflow IDs use', () => {
    // group.id is optional to this client, so a config omitting it typechecks. The consumer must
    // still join the group its workflow IDs are namespaced by.
    const resolved = applyDBOSConsumerConfig({ 'auto.offset.reset': 'latest' } as never, 250, GENERATED_GROUP);
    assert.equal(resolved['group.id'], GENERATED_GROUP);
    // A caller's own group.id is never overwritten.
    assert.equal(applyDBOSConsumerConfig({ 'group.id': 'mine' } as never, 250, GENERATED_GROUP)['group.id'], 'mine');
    // The kafkaJS-style spelling counts as the caller's too.
    assert.equal(
      applyDBOSConsumerConfig({ kafkaJS: { groupId: 'via-kafkajs' } } as never, 250, GENERATED_GROUP)['group.id'],
      'via-kafkajs',
    );
  });

  await test("the caller's config object is never mutated, nested objects included", () => {
    // kafkaJS is the only nested object the function touches, and it survives the top-level spread
    // by reference — so it must be in the fixture, and the snapshot must be deep, or the one
    // mutation that could actually happen would go undetected.
    const config = {
      'group.id': 'g',
      'enable.auto.commit': false,
      'enable.auto.offset.store': true,
      kafkaJS: { autoCommit: false },
    };
    const snapshot = structuredClone(config);
    applyDBOSConsumerConfig(config as never, 250, GENERATED_GROUP);
    assert.deepEqual(config, snapshot);
  });
}).catch(assert.fail);
