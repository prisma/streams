export class ByteLruCache<K, V> {
  private readonly map = new Map<K, { value: V; weight: number }>();
  private readonly capacityBytes: number;
  private readonly weigh: (value: V) => number;
  private usedBytes = 0;

  constructor(maxBytes: number, weigh: (value: V) => number) {
    this.capacityBytes = Math.max(0, Math.floor(maxBytes));
    this.weigh = weigh;
  }

  get maxBytes(): number {
    return this.capacityBytes;
  }

  get size(): number {
    return this.map.size;
  }

  get totalBytes(): number {
    return this.usedBytes;
  }

  get(key: K): V | undefined {
    const entry = this.map.get(key);
    if (!entry) return undefined;
    this.map.delete(key);
    this.map.set(key, entry);
    return entry.value;
  }

  set(key: K, value: V): boolean {
    const weight = Math.max(0, Math.floor(this.weigh(value)));
    if (this.capacityBytes <= 0 || weight > this.capacityBytes) {
      this.delete(key);
      return false;
    }
    const existing = this.map.get(key);
    if (existing) {
      this.usedBytes -= existing.weight;
      this.map.delete(key);
    }
    this.map.set(key, { value, weight });
    this.usedBytes += weight;
    this.evict();
    return true;
  }

  has(key: K): boolean {
    return this.map.has(key);
  }

  delete(key: K): boolean {
    const existing = this.map.get(key);
    if (!existing) return false;
    this.usedBytes -= existing.weight;
    this.map.delete(key);
    return true;
  }

  clear(): void {
    this.map.clear();
    this.usedBytes = 0;
  }

  private evict(): void {
    while (this.usedBytes > this.capacityBytes && this.map.size > 0) {
      const oldest = this.map.keys().next();
      if (oldest.done) break;
      this.delete(oldest.value);
    }
  }
}
