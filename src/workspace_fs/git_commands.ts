import { defineCommand, type CustomCommand } from "just-bash";
import type { WorkspaceFsWorkspace } from "./client";
import { WorkspaceFsClientError } from "./client";
import { normalizeRef } from "./model";
import type { VfsCommit } from "./types";

export type WorkspaceGitCommandOptions = {
  getWorkspace: () => WorkspaceFsWorkspace;
  setWorkspace?: (workspace: WorkspaceFsWorkspace) => void;
  author: {
    id: string;
    name?: string;
  };
};

function ok(stdout = "") {
  return { stdout, stderr: "", exitCode: 0 };
}

function fail(message: string, exitCode = 1) {
  return { stdout: "", stderr: `${message}\n`, exitCode };
}

function parseCommitMessage(args: string[]): string | null {
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "-m" || arg === "--message") return args[i + 1] ?? null;
    if (arg?.startsWith("-m") && arg.length > 2) return arg.slice(2);
  }
  return null;
}

function shortId(id: string | null | undefined): string {
  if (!id) return "(none)";
  const raw = id.includes(":") ? id.slice(id.indexOf(":") + 1) : id;
  return raw.slice(0, 12);
}

function formatCommit(commit: VfsCommit): string {
  const lines = [
    `commit ${commit.id}`,
    ...(commit.git ? [`Git-Commit: ${commit.git.newOid}`] : []),
    `Author: ${commit.author.name ? `${commit.author.name} <${commit.author.id}>` : commit.author.id}`,
    `Date:   ${commit.createdAt}`,
    "",
    `    ${commit.message}`,
  ];
  if (commit.changeSummary) {
    lines.push("", `    ${commit.changeSummary.added} added, ${commit.changeSummary.modified} modified, ${commit.changeSummary.deleted} deleted`);
  }
  return `${lines.join("\n")}\n`;
}

async function currentHead(workspace: WorkspaceFsWorkspace): Promise<string | null> {
  const ref = await workspace.repo.getRef(workspace.ref);
  return ref.commitId;
}

export function createWorkspaceGitCommands(options: WorkspaceGitCommandOptions): CustomCommand[] {
  const git = defineCommand("git", async (args) => {
    const workspace = options.getWorkspace();
    const subcommand = args[0] ?? "help";
    const rest = args.slice(1);
    try {
      if (subcommand === "status") {
        const status = await workspace.status();
        const head = await currentHead(workspace);
        const out = [
          `On branch ${workspace.ref.replace(/^refs\/heads\//, "")}`,
          `HEAD ${shortId(head)}`,
          "",
        ];
        if (status.changedPaths.length === 0) {
          out.push("nothing to commit, working tree clean");
        } else {
          out.push("Changes not staged for commit:");
          for (const path of status.changedPaths) out.push(`  modified: ${path.replace(/^\//, "")}`);
          out.push("", 'use "git commit -m <message>" to commit the durable workspace overlay');
        }
        return ok(`${out.join("\n")}\n`);
      }

      if (subcommand === "diff") {
        return ok(await workspace.diff());
      }

      if (subcommand === "add") {
        return ok("");
      }

      if (subcommand === "commit") {
        const message = parseCommitMessage(rest);
        if (!message) return fail("git commit requires -m <message>");
        const res = await workspace.commit({
          ref: workspace.ref,
          message,
          author: options.author,
        });
        const gitSuffix = res.git ? ` git:${shortId(res.git.newOid)}` : "";
        return ok(`[${workspace.ref.replace(/^refs\/heads\//, "")} ${shortId(res.newCommitId)}${gitSuffix}] ${message}\n`);
      }

      if (subcommand === "log") {
        const commits = await workspace.repo.log(workspace.ref, 20);
        if (commits.length === 0) return ok("");
        return ok(commits.map(formatCommit).join("\n"));
      }

      if (subcommand === "show") {
        const commitId = rest[0] ?? (await currentHead(workspace));
        if (!commitId) return fail("no commits");
        return ok(formatCommit(await workspace.repo.show(commitId)));
      }

      if (subcommand === "checkout") {
        const ref = rest[0];
        if (!ref) return fail("git checkout requires a ref");
        if (!options.setWorkspace) return fail("git checkout is not wired for this shell");
        const next = await workspace.repo.checkout({ ref: normalizeRef(ref) });
        options.setWorkspace(next);
        return ok(`Switched to ${next.ref}\n`);
      }

      if (subcommand === "help" || subcommand === "--help") {
        return ok("git status|diff|add|commit|log|show|checkout\n");
      }

      return fail(`unsupported git command: ${subcommand}`);
    } catch (error) {
      if (error instanceof WorkspaceFsClientError) return fail(error.message);
      return fail(String(error));
    }
  });

  return [git];
}
