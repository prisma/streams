import { Buffer } from "node:buffer";
import type {
  BufferEncoding,
  CpOptions,
  FileContent,
  FsStat,
  IFileSystem,
  MkdirOptions,
  RmOptions,
} from "just-bash";
import type { ByteString } from "just-bash";
import { VfsClientError, type VfsWorkspace } from "./client";
import { canonicalizeVfsPath } from "./model";
import { Result } from "better-result";

type VfsDirentEntry = Awaited<ReturnType<NonNullable<IFileSystem["readdirWithFileTypes"]>>>[number];
type ReadFileOptions = { encoding?: BufferEncoding | null };
type WriteFileOptions = { encoding?: BufferEncoding };

export type PrismaStreamsVfsFsOptions = {
  mountPath?: string;
};

export class PrismaStreamsVfsFsError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PrismaStreamsVfsFsError";
  }
}

function encodingFromOptions(options?: ReadFileOptions | WriteFileOptions | BufferEncoding): BufferEncoding {
  if (typeof options === "string") return options;
  return (options?.encoding ?? "utf8") as BufferEncoding;
}

function normalizeFsPath(path: string): string {
  const res = canonicalizeVfsPath(path);
  if (Result.isError(res)) throw new PrismaStreamsVfsFsError(res.error.message);
  return res.value;
}

function dirname(path: string): string {
  if (path === "/") return "/";
  const idx = path.lastIndexOf("/");
  return idx <= 0 ? "/" : path.slice(0, idx);
}

function basename(path: string): string {
  if (path === "/") return "";
  const idx = path.lastIndexOf("/");
  return idx < 0 ? path : path.slice(idx + 1);
}

function bytesFromFileContent(content: FileContent, options?: WriteFileOptions | BufferEncoding): Uint8Array {
  if (content instanceof Uint8Array) return content;
  const encoding = encodingFromOptions(options);
  return new Uint8Array(Buffer.from(content, encoding));
}

function fsStatFromNode(node: Awaited<ReturnType<VfsWorkspace["stat"]>>): FsStat {
  return {
    isFile: node.type === "file",
    isDirectory: node.type === "dir",
    isSymbolicLink: node.type === "symlink",
    mode: node.mode,
    size: node.size,
    mtime: node.mtime ? new Date(node.mtime) : new Date(0),
  };
}

export class PrismaStreamsVfsFs implements IFileSystem {
  private wc: VfsWorkspace;
  private readonly mountPath: string;

  constructor(wc: VfsWorkspace, options: PrismaStreamsVfsFsOptions = {}) {
    this.wc = wc;
    this.mountPath = normalizeFsPath(options.mountPath ?? "/workspace");
  }

  setWorkspace(wc: VfsWorkspace): void {
    this.wc = wc;
  }

  private absolute(path: string): string {
    return normalizeFsPath(path);
  }

  private toVfsPath(path: string): string {
    const abs = this.absolute(path);
    if (this.mountPath === "/") return abs;
    if (abs === this.mountPath) return "/";
    if (abs.startsWith(`${this.mountPath}/`)) return normalizeFsPath(abs.slice(this.mountPath.length));
    throw new PrismaStreamsVfsFsError(`${path} is outside ${this.mountPath}`);
  }

  resolvePath(base: string, path: string): string {
    if (path.startsWith("/")) return this.absolute(path);
    return this.absolute(`${base}/${path}`);
  }

  async readFile(path: string, options?: ReadFileOptions | BufferEncoding): Promise<string> {
    const bytes = await this.readFileBuffer(path);
    return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength).toString(encodingFromOptions(options));
  }

  async readFileBytes(path: string): Promise<ByteString> {
    const bytes = await this.readFileBuffer(path);
    return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength).toString("latin1") as unknown as ByteString;
  }

  async readFileBuffer(path: string): Promise<Uint8Array> {
    return this.wc.readFileBuffer(this.toVfsPath(path));
  }

  async writeFile(path: string, content: FileContent, options?: WriteFileOptions | BufferEncoding): Promise<void> {
    await this.wc.writeFile(this.toVfsPath(path), bytesFromFileContent(content, options));
  }

  async appendFile(path: string, content: FileContent, options?: WriteFileOptions | BufferEncoding): Promise<void> {
    await this.wc.appendFile(this.toVfsPath(path), bytesFromFileContent(content, options));
  }

  async exists(path: string): Promise<boolean> {
    try {
      await this.wc.stat(this.toVfsPath(path));
      return true;
    } catch (error) {
      if (error instanceof VfsClientError && error.status === 404) return false;
      if (error instanceof PrismaStreamsVfsFsError) return false;
      throw error;
    }
  }

  async stat(path: string): Promise<FsStat> {
    return fsStatFromNode(await this.wc.stat(this.toVfsPath(path)));
  }

  async lstat(path: string): Promise<FsStat> {
    return this.stat(path);
  }

  async mkdir(path: string, _options?: MkdirOptions): Promise<void> {
    await this.wc.mkdir(this.toVfsPath(path));
  }

  async readdir(path: string): Promise<string[]> {
    const res = await this.wc.readdir(this.toVfsPath(path));
    return res.entries.map((entry) => basename(entry.path));
  }

  async readdirWithFileTypes(path: string): Promise<VfsDirentEntry[]> {
    const res = await this.wc.readdir(this.toVfsPath(path));
    return res.entries.map((entry) => ({
      name: basename(entry.path),
      isFile: entry.type === "file",
      isDirectory: entry.type === "dir",
      isSymbolicLink: entry.type === "symlink",
    }));
  }

  async rm(path: string, options: RmOptions = {}): Promise<void> {
    await this.wc.remove(this.toVfsPath(path), options);
  }

  async cp(src: string, dest: string, options: CpOptions = {}): Promise<void> {
    const source = this.toVfsPath(src);
    const target = this.toVfsPath(dest);
    const stat = await this.wc.stat(source);
    if (stat.type === "dir") {
      if (!options.recursive) throw new PrismaStreamsVfsFsError("cp: source is a directory");
      await this.wc.mkdir(target);
      const entries = await this.wc.readdir(source);
      for (const entry of entries.entries) {
        await this.cp(`${src}/${basename(entry.path)}`, `${dest}/${basename(entry.path)}`, options);
      }
      return;
    }
    if (stat.type === "symlink") {
      await this.wc.symlink(stat.symlinkTarget ?? "", target);
      return;
    }
    const bytes = await this.wc.readFileBuffer(source);
    await this.wc.writeFile(target, bytes);
  }

  async mv(src: string, dest: string): Promise<void> {
    await this.wc.rename(this.toVfsPath(src), this.toVfsPath(dest));
  }

  getAllPaths(): string[] {
    return [];
  }

  async chmod(path: string, _mode: number): Promise<void> {
    await this.wc.stat(this.toVfsPath(path));
  }

  async symlink(target: string, linkPath: string): Promise<void> {
    await this.wc.symlink(target, this.toVfsPath(linkPath));
  }

  async link(existingPath: string, newPath: string): Promise<void> {
    const bytes = await this.readFileBuffer(existingPath);
    await this.writeFile(newPath, bytes);
  }

  async readlink(path: string): Promise<string> {
    const node = await this.wc.stat(this.toVfsPath(path));
    if (node.type !== "symlink") throw new PrismaStreamsVfsFsError("path is not a symlink");
    return node.symlinkTarget ?? "";
  }

  async realpath(path: string): Promise<string> {
    const abs = this.absolute(path);
    await this.stat(abs);
    return abs;
  }

  async utimes(path: string, _atime: Date, _mtime: Date): Promise<void> {
    if (!(await this.exists(path))) {
      await this.writeFile(path, "");
    }
  }

  mountRoot(): string {
    return this.mountPath;
  }
}
