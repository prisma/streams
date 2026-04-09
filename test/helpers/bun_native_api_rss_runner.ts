import {
  BUN_NATIVE_API_RSS_SCENARIOS,
  runBunNativeApiRssScenario,
  type BunNativeApiRssScenario,
} from "./bun_native_api_rss_scenarios";

function isScenario(value: string): value is BunNativeApiRssScenario {
  return (BUN_NATIVE_API_RSS_SCENARIOS as readonly string[]).includes(value);
}

async function main(): Promise<void> {
  const scenario = process.argv[2];
  if (!scenario || !isScenario(scenario)) {
    throw new Error(`expected one of: ${BUN_NATIVE_API_RSS_SCENARIOS.join(", ")}`);
  }
  const measurement = await runBunNativeApiRssScenario(scenario);
  // eslint-disable-next-line no-console
  console.log(`BUN_NATIVE_API_RSS_SUMMARY ${JSON.stringify(measurement)}`);
}

try {
  await main();
  process.exit(0);
} catch (error) {
  // eslint-disable-next-line no-console
  console.error(error);
  process.exit(1);
}
