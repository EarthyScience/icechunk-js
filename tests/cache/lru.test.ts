import { describe, it, expect } from "vitest";
import { LRUCache } from "../../src/cache/lru.js";

describe("LRUCache", () => {
  it("should store and retrieve values", () => {
    const cache = new LRUCache<string, number>(10);
    cache.set("a", 1);
    cache.set("b", 2);

    expect(cache.get("a")).toBe(1);
    expect(cache.get("b")).toBe(2);
  });

  it("should return undefined for missing keys", () => {
    const cache = new LRUCache<string, number>(10);
    expect(cache.get("missing")).toBeUndefined();
  });

  it("should evict oldest items when at capacity", () => {
    const cache = new LRUCache<string, number>(3);
    cache.set("a", 1);
    cache.set("b", 2);
    cache.set("c", 3);
    cache.set("d", 4); // Should evict "a"

    expect(cache.get("a")).toBeUndefined();
    expect(cache.get("b")).toBe(2);
    expect(cache.get("c")).toBe(3);
    expect(cache.get("d")).toBe(4);
  });

  it("should update LRU order on get", () => {
    const cache = new LRUCache<string, number>(3);
    cache.set("a", 1);
    cache.set("b", 2);
    cache.set("c", 3);

    // Access "a" to make it most recently used
    cache.get("a");

    // Add new item, should evict "b" (now oldest)
    cache.set("d", 4);

    expect(cache.get("a")).toBe(1);
    expect(cache.get("b")).toBeUndefined();
    expect(cache.get("c")).toBe(3);
    expect(cache.get("d")).toBe(4);
  });

  it("should update existing keys", () => {
    const cache = new LRUCache<string, number>(3);
    cache.set("a", 1);
    cache.set("b", 2);
    cache.set("a", 10); // Update "a"

    expect(cache.get("a")).toBe(10);
    expect(cache.size).toBe(2);
  });

  it("should track size correctly", () => {
    const cache = new LRUCache<string, number>(10);
    expect(cache.size).toBe(0);

    cache.set("a", 1);
    expect(cache.size).toBe(1);

    cache.set("b", 2);
    expect(cache.size).toBe(2);

    cache.delete("a");
    expect(cache.size).toBe(1);

    cache.clear();
    expect(cache.size).toBe(0);
  });
});
