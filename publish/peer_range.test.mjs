import assert from 'node:assert/strict';
import { test } from 'node:test';
import { peerRange } from './peer_range.mjs';

test('a release accepts any patch of the same minor', () => {
  assert.equal(peerRange('5.3.0'), '~5.3.0');
  assert.equal(peerRange('5.3.7'), '~5.3.0');
  assert.equal(peerRange('10.12.1'), '~10.12.0');
});

test('previews and test builds accept anything', () => {
  assert.equal(peerRange('5.4.3-preview'), '*');
  assert.equal(peerRange('5.4.3-test.abc1234'), '*');
});

test('an unparseable version is refused', () => {
  assert.throws(() => peerRange('placeholder'), /Cannot parse/);
  assert.throws(() => peerRange('5.3'), /Cannot parse/);
});
