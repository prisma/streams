import type { Config } from "./config";
import { createAppCore, type App } from "./app_core";
import type { ObjectStore } from "./objectstore/interface";
import { NullObjectStore } from "./objectstore/null";
import { StreamReader } from "./reader";
import type { StatsCollector } from "./stats";
import type { UploaderController } from "./uploader";
import type { SegmenterController } from "./segment/segmenter_workers";

class NoopUploader implements UploaderController {
  start(): void {}
  stop(_hard?: boolean): void {}
  countSegmentsWaiting(): number {
    return 0;
  }
  setHooks(_hooks: { onSegmentsUploaded?: (stream: string) => void } | undefined): void {}
  async publishManifest(_stream: string): Promise<void> {}
}

const noopSegmenter: SegmenterController = {
  start(): void {},
  stop(_hard?: boolean): void {},
};

export type CreateLocalAppOptions = {
  stats?: StatsCollector;
};

export function createLocalApp(cfg: Config, os?: ObjectStore, opts: CreateLocalAppOptions = {}): App {
  return createAppCore(cfg, {
    stats: opts.stats,
    createRuntime: ({ config, db }) => {
      const store = os ?? new NullObjectStore();
      const reader = new StreamReader(config, db, store);

      return {
        store,
        reader,
        segmenter: noopSegmenter,
        uploader: new NoopUploader(),
        uploadSchemaRegistry: async (): Promise<void> => {},
        start: (): void => {},
      };
    },
  });
}
