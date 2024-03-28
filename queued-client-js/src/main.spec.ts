import asyncTimeout from "@xtjs/lib/js/asyncTimeout";
import { ChildProcess, spawn } from "child_process";
import { randomBytes, randomUUID } from "crypto";
import { mkdirSync } from "fs";
import { rm } from "fs/promises";
import { tmpdir } from "os";
import { join } from "path";
import { QueuedClient } from "./main";

const API_KEY = "123";
const PORT = 3333;
const ENDPOINT = `http://127.0.0.1:${PORT}`;

describe("QueuedClient", () => {
  const dataDir = join(tmpdir(), `queued-client-js-test-${randomUUID()}`);
  mkdirSync(dataDir);
  let proc: ChildProcess;
  const client = new QueuedClient({
    apiKey: API_KEY,
    endpoint: ENDPOINT,
  });
  beforeAll(async () => {
    proc = spawn(
      "cargo",
      [
        "run",
        "--bin",
        "queued",
        "--",
        "--data-dir",
        dataDir,
        "--api-key",
        API_KEY,
        "--port",
        `${PORT}`,
      ],
      {
        cwd: join(__dirname, "..", ".."),
        stdio: ["ignore", "inherit", "inherit"],
      },
    );
    while (true) {
      await asyncTimeout(1000 * 2);
      try {
        await fetch(ENDPOINT);
        break;
      } catch (err) {
        console.warn("Server not ready:", err.message);
      }
    }
    console.log("Server is ready");
  });

  test("flow", async () => {
    await client.createQueue("test");
    expect(await client.listQueues()).toEqual({
      queues: [
        {
          name: "test",
        },
      ],
    });
    const contents = new Uint8Array(randomBytes(400));
    await client.pushQueueMessages("test", [
      { contents, visibilityTimeoutSecs: 1 },
    ]);
    await asyncTimeout(2000);
    const polled = await client.pollQueueMessages("test", 10, 15);
    expect(polled.length).toEqual(1);
    const [p] = polled;
    expect(p.contents).toEqual(contents);
    await client.updateQueueMessage("test", p, 10);
    await client.deleteQueueMessages("test", [p]);
    await client.deleteQueue("test");
  });

  afterAll(async () => {
    proc.kill();
    // Wait for process to exit.
    await asyncTimeout(3000);
    await rm(dataDir, { recursive: true, force: true });
    console.log("Deleted data dir");
  });
});
