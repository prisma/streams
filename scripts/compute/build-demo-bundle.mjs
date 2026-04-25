import { access, mkdir, readFile, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import process from "node:process";
import { fileURLToPath, pathToFileURL } from "node:url";
import { parseArgs } from "node:util";

export const DEFAULT_COMPUTE_DEMO_BUNDLE_OUTDIR = ".compute-demo-build/bundle";
export const COMPUTE_DEMO_ENTRYPOINTS = [
  "src/compute/demo_entry.ts",
  "src/segment/segmenter_worker.ts",
  "src/touch/processor_worker.ts",
];

function contentTypeForExt(ext) {
  const types = {
    ".css": "text/css; charset=utf-8",
    ".gif": "image/gif",
    ".html": "text/html; charset=utf-8",
    ".jpeg": "image/jpeg",
    ".jpg": "image/jpeg",
    ".js": "application/javascript; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".png": "image/png",
    ".svg": "image/svg+xml; charset=utf-8",
    ".woff": "font/woff",
    ".woff2": "font/woff2",
  };
  return types[ext] ?? "application/octet-stream";
}

function assertStudioRoot(studioRoot) {
  const requiredPaths = [
    "package.json",
    "demo/ppg-dev/client.tsx",
    "ui/index.css",
    "postcss.config.mjs",
  ];
  for (const requiredPath of requiredPaths) {
    const candidate = path.join(studioRoot, requiredPath);
    if (!Bun.file(candidate).exists()) {
      throw new Error(
        `missing Studio asset source at ${candidate}; set PRISMA_STUDIO_ROOT to the Studio repository root`,
      );
    }
  }
}

function resolveStudioRoot(cwd, explicitStudioRoot) {
  const candidate = path.resolve(
    explicitStudioRoot ?? process.env.PRISMA_STUDIO_ROOT ?? path.join(cwd, "..", "studio"),
  );
  assertStudioRoot(candidate);
  return candidate;
}

async function buildStudioAssets(studioRoot) {
  const studioPackageJson = path.join(studioRoot, "package.json");
  const studioPackage = await Bun.file(studioPackageJson).json();
  const clientBuild = await Bun.build({
    define: {
      VERSION_INJECTED_AT_BUILD_TIME: JSON.stringify(studioPackage.version),
    },
    entrypoints: [path.join(studioRoot, "demo/ppg-dev/client.tsx")],
    format: "esm",
    minify: true,
    sourcemap: "inline",
    splitting: false,
    target: "browser",
    write: false,
  });

  if (!clientBuild.success) {
    const details = clientBuild.logs.map((log) => log.message).join("\n");
    throw new Error(`Studio client build failed:\n${details}`);
  }

  const jsOutput = clientBuild.outputs.find((output) => output.path.endsWith(".js"));
  if (!jsOutput) {
    throw new Error("Studio client build produced no JavaScript output");
  }

  const assetEntries = [];
  for (const output of clientBuild.outputs) {
    if (output === jsOutput) continue;
    assetEntries.push({
      base64: Buffer.from(await output.arrayBuffer()).toString("base64"),
      contentType: contentTypeForExt(path.extname(output.path)),
      path: `/${path.basename(output.path)}`,
    });
  }

  const studioRequire = createRequire(studioPackageJson);
  const postcssModuleUrl = pathToFileURL(studioRequire.resolve("postcss")).href;
  const postcssConfigUrl = pathToFileURL(path.join(studioRoot, "postcss.config.mjs")).href;
  const postcssModule = await import(postcssModuleUrl);
  const postcss = postcssModule.default;
  const postcssConfig = await import(postcssConfigUrl);
  const cssEntrypoint = path.join(studioRoot, "ui/index.css");
  const cssSource = await Bun.file(cssEntrypoint).text();
  const cssResult = await postcss(postcssConfig.default.plugins).process(cssSource, {
    from: cssEntrypoint,
  });
  const sharedFontDir = path.resolve(
    studioRoot,
    "../web/packages/eclipse/dist/static/fonts",
  );
  const sharedFontNames = [
    "MonaSansMonoVF[wght].woff2",
    "MonaSansVF[wdth,wght,opsz,ital].woff2",
  ];

  for (const fontName of sharedFontNames) {
    const absolutePath = path.join(sharedFontDir, fontName);
    try {
      const bytes = await readFile(absolutePath);
      assetEntries.push({
        base64: bytes.toString("base64"),
        contentType: "font/woff2",
        path: `/web/packages/eclipse/dist/static/fonts/${fontName}`,
      });
    } catch {
      // Keep the bundle build working when the sibling web repo is absent.
    }
  }

  return {
    appScript: await jsOutput.text(),
    appStyles: cssResult.css,
    assetEntries,
  };
}

function generateAssetsModule(studioAssets) {
  const mapEntries = [];

  for (const asset of studioAssets.assetEntries) {
    const entrySource = `{ bytes: new Uint8Array(Buffer.from(${JSON.stringify(asset.base64)}, "base64")), contentType: ${JSON.stringify(asset.contentType)} }`;
    mapEntries.push(`  [${JSON.stringify(asset.path)}, ${entrySource}]`);
    mapEntries.push(
      `  [${JSON.stringify(`/studio${asset.path}`)}, ${entrySource}]`,
    );
  }

  return [
    `export const appScript = ${JSON.stringify(studioAssets.appScript)};`,
    `export const appStyles = ${JSON.stringify(studioAssets.appStyles)};`,
    `export const builtAssets = new Map([\n${mapEntries.join(",\n")}\n]);`,
  ].join("\n");
}

export async function buildComputeDemoBundle(options = {}) {
  const cwd = path.resolve(options.cwd ?? process.cwd());
  const studioRoot = resolveStudioRoot(cwd, options.studioRoot);
  const outDir = path.resolve(
    cwd,
    options.outDir ?? DEFAULT_COMPUTE_DEMO_BUNDLE_OUTDIR,
  );
  const studioAssets = await buildStudioAssets(studioRoot);
  const assetsModuleSource = generateAssetsModule(studioAssets);
  const entrypoints = COMPUTE_DEMO_ENTRYPOINTS.map((entrypoint) =>
    path.resolve(cwd, entrypoint),
  );

  await rm(outDir, { force: true, recursive: true });
  await mkdir(outDir, { recursive: true });

  const buildResult = await Bun.build({
    entrypoints,
    outdir: outDir,
    plugins: [
      {
        name: "prebuilt-studio-assets",
        setup(build) {
          build.onResolve({ filter: /^virtual:prebuilt-studio-assets$/ }, () => ({
            namespace: "prebuilt-studio-assets",
            path: "virtual:prebuilt-studio-assets",
          }));
          build.onLoad({ filter: /.*/, namespace: "prebuilt-studio-assets" }, () => ({
            contents: assetsModuleSource,
            loader: "js",
          }));
        },
      },
    ],
    sourcemap: "external",
    target: "bun",
  });

  if (!buildResult.success) {
    const details = buildResult.logs.map((log) => log.message).join("\n");
    throw new Error(`Compute demo bundle build failed:\n${details}`);
  }

  await access(path.join(outDir, "compute", "demo_entry.js"));
  await access(path.join(outDir, "segment", "segmenter_worker.js"));
  await access(path.join(outDir, "touch", "processor_worker.js"));

  return {
    bundleDir: outDir,
    entrypoint: "compute/demo_entry.js",
    studioRoot,
  };
}

const isMain =
  process.argv[1] != null &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);

if (isMain) {
  const { values } = parseArgs({
    args: process.argv.slice(2),
    allowPositionals: false,
    options: {
      outdir: { type: "string" },
      "studio-root": { type: "string" },
    },
  });

  const result = await buildComputeDemoBundle({
    outDir: values.outdir,
    studioRoot: values["studio-root"],
  });
  console.log(
    `Built Compute demo bundle at ${result.bundleDir} (entrypoint ${result.entrypoint}, studio root ${result.studioRoot})`,
  );
}
