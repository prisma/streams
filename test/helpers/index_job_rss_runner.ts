import {
  INDEX_JOB_RSS_SCENARIOS,
  runIndexJobRssScenario,
  type IndexJobRssScenario,
} from "./index_job_rss_scenarios";

function isIndexJobRssScenario(value: string): value is IndexJobRssScenario {
  return (INDEX_JOB_RSS_SCENARIOS as readonly string[]).includes(value);
}

async function main(): Promise<void> {
  const scenario = process.argv[2];
  if (!scenario || !isIndexJobRssScenario(scenario)) {
    const known = INDEX_JOB_RSS_SCENARIOS.join(", ");
    throw new Error(`expected one of: ${known}`);
  }
  const measurement = await runIndexJobRssScenario(scenario);
  // eslint-disable-next-line no-console
  console.log(`INDEX_JOB_RSS_SUMMARY ${JSON.stringify(measurement)}`);
}

try {
  await main();
  process.exit(0);
} catch (error) {
  // eslint-disable-next-line no-console
  console.error(error);
  process.exit(1);
}
