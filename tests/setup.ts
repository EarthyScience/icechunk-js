import { spawn, type ChildProcess } from "node:child_process";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));

// Global fixture server process
let fixtureServer: ChildProcess | null = null;

const FIXTURE_SERVER_PORT = 8765;
const HEALTH_CHECK_URL = `http://localhost:${FIXTURE_SERVER_PORT}/_health`;
const STARTUP_TIMEOUT_MS = 10000;
const HEALTH_CHECK_INTERVAL_MS = 100;

/**
 * Wait for the server to be ready by polling the health endpoint.
 */
async function waitForServer(): Promise<void> {
  const startTime = Date.now();

  while (Date.now() - startTime < STARTUP_TIMEOUT_MS) {
    try {
      const response = await fetch(HEALTH_CHECK_URL);
      if (response.ok) {
        return;
      }
    } catch {
      // Server not ready yet, continue polling
    }
    await new Promise((resolve) =>
      setTimeout(resolve, HEALTH_CHECK_INTERVAL_MS),
    );
  }

  throw new Error(
    `Fixture server failed to start within ${STARTUP_TIMEOUT_MS}ms`,
  );
}

/**
 * Global setup - runs once before all tests.
 */
export default async function setup(): Promise<() => Promise<void>> {
  // Start fixture server - serve from synced test data directory
  const serverPath = resolve(__dirname, "fixtures/serve.ts");
  const fixturesDir = resolve(__dirname, "data");

  fixtureServer = spawn(
    "npx",
    ["tsx", serverPath, String(FIXTURE_SERVER_PORT), fixturesDir],
    {
      stdio: "pipe",
      detached: false,
      cwd: resolve(__dirname, ".."),
    },
  );

  // Log stderr for debugging (but don't use it for readiness)
  fixtureServer.stderr?.on("data", (data: Buffer) => {
    console.error("Fixture server stderr:", data.toString());
  });

  // Handle early exit
  let earlyExit = false;
  fixtureServer.on("exit", (code) => {
    if (code !== 0 && code !== null) {
      earlyExit = true;
    }
  });

  // Wait for server to be ready via health check
  try {
    await waitForServer();
  } catch (error) {
    if (earlyExit) {
      throw new Error("Fixture server exited before becoming ready");
    }
    throw error;
  }

  return async () => {
    if (fixtureServer) {
      fixtureServer.kill("SIGTERM");
      fixtureServer = null;
    }
  };
}
