import { encode } from "@msgpack/msgpack";
import asyncTimeout from "@xtjs/lib/js/asyncTimeout";
import { ChildProcess, spawn } from "child_process";
import { randomUUID } from "crypto";
import { mkdirSync } from "fs";
import { rm } from "fs/promises";
import { tmpdir } from "os";
import { join } from "path";
import { QueuedClient } from "./main";

const PORT = 3333;
const ENDPOINT = `http://127.0.0.1:${PORT}`;

describe("QueuedClient", () => {
  const dataDir = join(tmpdir(), `queued-client-js-test-${randomUUID()}`);
  mkdirSync(dataDir);
  let proc: ChildProcess;
  const client = new QueuedClient({
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

  test(
    "flow",
    async () => {
      await client.createQueue("test");
      console.log("Queue created");
      expect(await client.listQueues()).toEqual({
        queues: [
          {
            name: "test",
          },
        ],
      });
      const contents = { a: "true", b: [1, { false: null }] };
      const contentsRaw = encode(contents);
      const q = client.queue("test");
      await q.pushMessages([{ contents, visibilityTimeoutSecs: 1 }]);
      console.log("Message pushed");
      await asyncTimeout(2000);
      const polled = await q.pollMessagesRaw(10, 15);
      console.log("Message polled");
      expect(polled.length).toEqual(1);
      const [p] = polled;
      expect(p.contents).toEqual(contentsRaw);
      await q.updateMessage(p, 10);
      console.log("Message updated");
      await q.deleteMessages([p]);
      console.log("Message deleted");
      await client.deleteQueue("test");
      console.log("Queue deleted");
    },
    1000 * 20,
  );

  afterAll(async () => {
    proc.kill();
    // Wait for process to exit.
    await asyncTimeout(3000);
    await rm(dataDir, { recursive: true, force: true });
    console.log("Deleted data dir");
  });
});
