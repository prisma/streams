import { PrismaStreamsVfsFs, PrismaStreamsVfsFsError, type PrismaStreamsVfsFsOptions } from "../vfs/just_bash_adapter";

export type PrismaStreamsWorkspaceFsOptions = PrismaStreamsVfsFsOptions;

export class PrismaStreamsWorkspaceFsError extends PrismaStreamsVfsFsError {}

export class PrismaStreamsWorkspaceFs extends PrismaStreamsVfsFs {}
