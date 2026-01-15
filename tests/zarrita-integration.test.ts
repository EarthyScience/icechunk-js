/**
 * Integration tests with zarrita library.
 *
 * Uses test-repo-v1 from icechunk-python/tests/data which contains:
 * - group1/big_chunks: 2D float32 array, shape=(10,10), chunks=(5,5), filled with 42.0
 * - group1/small_chunks: 1D int8 array, shape=(5,), chunks=(1,), filled with 84
 * - group2/group3/group4/group5/inner: 2D float32 array (only on my-branch)
 *
 * Branches: main, my-branch
 * Tags: "it works!", "it also works!"
 *
 * Known limitation: chunk [0,0] of big_chunks is a virtual reference to s3://testbucket.
 * Virtual chunk fetch behavior is untested because it requires external S3 access.
 * Tests read rows 5-10 to avoid the virtual chunk.
 */

import { describe, it, expect } from 'vitest';
import * as z from 'zarrita';
import { IcechunkStore } from '../src/index.js';
import { getFixtureUrl } from './helpers.js';

describe('Zarrita Integration', () => {
  describe('read array data', () => {
    it('should read array with correct shape, dtype, and values', async () => {
      const store = new IcechunkStore(getFixtureUrl('test-repo-v1'));
      const location = z.root(store).resolve('/group1/small_chunks');
      const arr = await z.open(location, { kind: 'array' });

      expect(arr.shape).toEqual([5]);
      expect(arr.dtype).toBe('int8');
      expect(arr.chunks).toEqual([1]);

      const data = await z.get(arr);
      expect(data.data.length).toBe(5);
      for (let i = 0; i < 5; i++) {
        expect(data.data[i]).toBe(84);
      }
    });

    it('should read 2D array slice', async () => {
      const store = new IcechunkStore(getFixtureUrl('test-repo-v1'));
      const location = z.root(store).resolve('/group1/big_chunks');
      const arr = await z.open(location, { kind: 'array' });

      // Read rows 5-10 (avoids virtual chunk [0,0])
      const data = await z.get(arr, [{ start: 5, stop: 10 }, null]);

      expect(data.data.length).toBe(50);
      for (let i = 0; i < 50; i++) {
        expect(data.data[i]).toBe(42.0);
      }
    });
  });

  describe('groups', () => {
    it('should open root group', async () => {
      const store = new IcechunkStore(getFixtureUrl('test-repo-v1'));
      const group = await z.open(store, { kind: 'group' });
      expect(group).toBeDefined();
    });

    it('should open deeply nested structure on branch', async () => {
      const store = new IcechunkStore(getFixtureUrl('test-repo-v1'), {
        branch: 'my-branch',
      });
      const location = z.root(store).resolve('/group2/group3/group4/group5/inner');
      const arr = await z.open(location, { kind: 'array' });

      expect(arr.shape).toEqual([10, 10]);
      expect(arr.dtype).toBe('float32');
    });
  });

  describe('time travel', () => {
    it('should access different structure on different branches', async () => {
      // main branch does NOT have group2
      const storeMain = new IcechunkStore(getFixtureUrl('test-repo-v1'), {
        branch: 'main',
      });
      const locationMain = z.root(storeMain).resolve('/group2');
      await expect(z.open(locationMain, { kind: 'group' })).rejects.toThrow();

      // my-branch DOES have group2
      const storeBranch = new IcechunkStore(getFixtureUrl('test-repo-v1'), {
        branch: 'my-branch',
      });
      const locationBranch = z.root(storeBranch).resolve('/group2');
      const group = await z.open(locationBranch, { kind: 'group' });
      expect(group).toBeDefined();
    });

    it('should open different snapshots via tags', async () => {
      const store1 = new IcechunkStore(getFixtureUrl('test-repo-v1'), {
        tag: 'it works!',
      });
      const store2 = new IcechunkStore(getFixtureUrl('test-repo-v1'), {
        tag: 'it also works!',
      });

      // Both should open successfully (they point to different snapshots)
      const group1 = await z.open(store1, { kind: 'group' });
      const group2 = await z.open(store2, { kind: 'group' });

      expect(group1).toBeDefined();
      expect(group2).toBeDefined();
    });
  });
});
