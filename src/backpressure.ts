export class BackpressureGate {
  private readonly maxBytes: number;
  private currentBytes: number;
  private reservedBytes: number;

  constructor(maxBytes: number, initialBytes: number) {
    this.maxBytes = maxBytes;
    this.currentBytes = Math.max(0, initialBytes);
    this.reservedBytes = 0;
  }

  enabled(): boolean {
    return this.maxBytes > 0;
  }

  reserve(bytes: number): boolean {
    if (this.maxBytes <= 0) return true;
    if (bytes <= 0) return true;
    if (this.currentBytes + this.reservedBytes + bytes > this.maxBytes) return false;
    this.reservedBytes += bytes;
    return true;
  }

  commit(bytes: number, reservedBytes: number = bytes): void {
    if (this.maxBytes <= 0) return;
    if (bytes <= 0) return;
    if (reservedBytes > 0) this.reservedBytes = Math.max(0, this.reservedBytes - reservedBytes);
    this.currentBytes += bytes;
  }

  release(bytes: number): void {
    if (this.maxBytes <= 0) return;
    if (bytes <= 0) return;
    this.reservedBytes = Math.max(0, this.reservedBytes - bytes);
  }

  adjustOnSeal(payloadBytes: number, segmentBytes: number): void {
    if (this.maxBytes <= 0) return;
    const delta = segmentBytes - payloadBytes;
    this.currentBytes = Math.max(0, this.currentBytes + delta);
  }

  adjustOnUpload(segmentBytes: number): void {
    if (this.maxBytes <= 0) return;
    this.currentBytes = Math.max(0, this.currentBytes - segmentBytes);
  }

  adjustOnWalTrim(payloadBytes: number): void {
    if (this.maxBytes <= 0) return;
    if (payloadBytes <= 0) return;
    this.currentBytes = Math.max(0, this.currentBytes - payloadBytes);
  }

  getCurrentBytes(): number {
    return this.currentBytes;
  }

  getMaxBytes(): number {
    return this.maxBytes;
  }

  isOverLimit(): boolean {
    if (this.maxBytes <= 0) return false;
    return this.currentBytes + this.reservedBytes >= this.maxBytes;
  }
}
