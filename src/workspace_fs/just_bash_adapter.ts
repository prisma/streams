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
import { WorkspaceFsClientError, type WorkspaceFsWorkspace } from "./client";
import { canonicalizeVfsPath } from "./model";
import { Result } from "better-result";

type WorkspaceFsDirentEntry = Awaited<ReturnType<NonNullable<IFileSystem["readdirWithFileTypes"]>>>[number];
type ReadFileOptions = { encoding?: BufferEncoding | null };
type WriteFileOptions = { encoding?: BufferEncoding };

export type PrismaStreamsWorkspaceFsOptions = {
  mountPath?: string;
};

export class PrismaStreamsWorkspaceFsError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PrismaStreamsWorkspaceFsError";
  }
}

function encodingFromOptions(options?: ReadFileOptions | WriteFileOptions | BufferEncoding): BufferEncoding {
  if (typeof options === "string") return options;
  return (options?.encoding ?? "utf8") as BufferEncoding;
}

function normalizeFsPath(path: string): string {
  const res = canonicalizeVfsPath(path);
  if (Result.isError(res)) throw new PrismaStreamsWorkspaceFsError(res.error.message);
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

function fsStatFromNode(node: Awaited<ReturnType<WorkspaceFsWorkspace["stat"]>>): FsStat {
  return {
    isFile: node.type === "file",
    isDirectory: node.type === "dir",
    isSymbolicLink: node.type === "symlink",
    mode: node.mode,
    size: node.size,
    mtime: node.mtime ? new Date(node.mtime) : new Date(0),
  };
}

export class PrismaStreamsWorkspaceFs implements IFileSystem {
  private wc: WorkspaceFsWorkspace;
  private readonly mountPath: string;

  constructor(wc: WorkspaceFsWorkspace, options: PrismaStreamsWorkspaceFsOptions = {}) {
    this.wc = wc;
    this.mountPath = normalizeFsPath(options.mountPath ?? "/workspace");
  }

  setWorkspace(wc: WorkspaceFsWorkspace): void {
    this.wc = wc;
  }

  private absolute(path: string): string {
    return normalizeFsPath(path);
  }

  private toWorkspacePath(path: string): string {
    const abs = this.absolute(path);
    if (this.mountPath === "/") return abs;
    if (abs === this.mountPath) return "/";
    if (abs.startsWith(`${this.mountPath}/`)) return normalizeFsPath(abs.slice(this.mountPath.length));
    throw new PrismaStreamsWorkspaceFsError(`${path} is outside ${this.mountPath}`);
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
    return this.wc.readFileBuffer(this.toWorkspacePath(path));
  }

  async writeFile(path: string, content: FileContent, options?: WriteFileOptions | BufferEncoding): Promise<void> {
    await this.wc.writeFile(this.toWorkspacePath(path), bytesFromFileContent(content, options));
  }

  async appendFile(path: string, content: FileContent, options?: WriteFileOptions | BufferEncoding): Promise<void> {
    await this.wc.appendFile(this.toWorkspacePath(path), bytesFromFileContent(content, options));
  }

  async exists(path: string): Promise<boolean> {
    try {
      await this.wc.stat(this.toWorkspacePath(path));
      return true;
    } catch (error) {
      if (error instanceof WorkspaceFsClientError && error.status === 404) return false;
      if (error instanceof PrismaStreamsWorkspaceFsError) return false;
      throw error;
    }
  }

  async stat(path: string): Promise<FsStat> {
    return fsStatFromNode(await this.wc.stat(this.toWorkspacePath(path)));
  }

  async lstat(path: string): Promise<FsStat> {
    return this.stat(path);
  }

  async mkdir(path: string, _options?: MkdirOptions): Promise<void> {
    await this.wc.mkdir(this.toWorkspacePath(path));
  }

  async readdir(path: string): Promise<string[]> {
    const res = await this.wc.readdir(this.toWorkspacePath(path));
    return res.entries.map((entry) => basename(entry.path));
  }

  async readdirWithFileTypes(path: string): Promise<WorkspaceFsDirentEntry[]> {
    const res = await this.wc.readdir(this.toWorkspacePath(path));
    return res.entries.map((entry) => ({
      name: basename(entry.path),
      isFile: entry.type === "file",
      isDirectory: entry.type === "dir",
      isSymbolicLink: entry.type === "symlink",
    }));
  }

  async rm(path: string, options: RmOptions = {}): Promise<void> {
    await this.wc.remove(this.toWorkspacePath(path), options);
  }

  async cp(src: string, dest: string, options: CpOptions = {}): Promise<void> {
    const source = this.toWorkspacePath(src);
    const target = this.toWorkspacePath(dest);
    const stat = await this.wc.stat(source);
    if (stat.type === "dir") {
      if (!options.recursive) throw new PrismaStreamsWorkspaceFsError("cp: source is a directory");
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
    await this.wc.rename(this.toWorkspacePath(src), this.toWorkspacePath(dest));
  }

  getAllPaths(): string[] {
    return [];
  }

  async chmod(path: string, _mode: number): Promise<void> {
    await this.wc.stat(this.toWorkspacePath(path));
  }

  async symlink(target: string, linkPath: string): Promise<void> {
    await this.wc.symlink(target, this.toWorkspacePath(linkPath));
  }

  async link(existingPath: string, newPath: string): Promise<void> {
    const bytes = await this.readFileBuffer(existingPath);
    await this.writeFile(newPath, bytes);
  }

  async readlink(path: string): Promise<string> {
    const node = await this.wc.stat(this.toWorkspacePath(path));
    if (node.type !== "symlink") throw new PrismaStreamsWorkspaceFsError("path is not a symlink");
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
