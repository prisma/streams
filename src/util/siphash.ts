import { dsError } from "./ds_error.ts";
const MASK_64 = 0xffffffffffffffffn;

function rotl(x: bigint, b: number): bigint {
  return ((x << BigInt(b)) | (x >> BigInt(64 - b))) & MASK_64;
}

function readU64LE(bytes: Uint8Array, offset: number): bigint {
  const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  return dv.getBigUint64(offset, true);
}

export function siphash24(key: Uint8Array, msg: Uint8Array): bigint {
  if (key.byteLength !== 16) throw dsError("siphash24 requires 16-byte key");
  const k0 = readU64LE(key, 0);
  const k1 = readU64LE(key, 8);

  let v0 = 0x736f6d6570736575n ^ k0;
  let v1 = 0x646f72616e646f6dn ^ k1;
  let v2 = 0x6c7967656e657261n ^ k0;
  let v3 = 0x7465646279746573n ^ k1;

  const msgLen = msg.byteLength;
  let off = 0;
  while (off + 8 <= msgLen) {
    const m = readU64LE(msg, off);
    v3 ^= m;
    sipRound();
    sipRound();
    v0 ^= m;
    off += 8;
  }

  let b = BigInt(msgLen) << 56n;
  for (let i = 0; i < msgLen - off; i++) {
    b |= BigInt(msg[off + i]) << BigInt(8 * i);
  }

  v3 ^= b;
  sipRound();
  sipRound();
  v0 ^= b;

  v2 ^= 0xffn;
  sipRound();
  sipRound();
  sipRound();
  sipRound();

  return (v0 ^ v1 ^ v2 ^ v3) & MASK_64;

  function sipRound(): void {
    v0 = (v0 + v1) & MASK_64;
    v1 = rotl(v1, 13);
    v1 ^= v0;
    v0 = rotl(v0, 32);

    v2 = (v2 + v3) & MASK_64;
    v3 = rotl(v3, 16);
    v3 ^= v2;

    v0 = (v0 + v3) & MASK_64;
    v3 = rotl(v3, 21);
    v3 ^= v0;

    v2 = (v2 + v1) & MASK_64;
    v1 = rotl(v1, 17);
    v1 ^= v2;
    v2 = rotl(v2, 32);
  }
}
