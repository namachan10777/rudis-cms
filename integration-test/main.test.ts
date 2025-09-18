import * as v from "valibot";
import * as post from "./generated/posts/posts-valibot";
import { beforeAll, describe, expect, test } from "bun:test";
import * as sqlite from "bun:sqlite";

async function createDatabases() {
  const database = new sqlite.Database("./integration-test/database.sqlite", {
    create: true,
  });
  const storage = new sqlite.Database("./integration-test/storage.sqlite", {
    create: true,
  });
  database.close();
  storage.close();
}

let database: sqlite.Database | null = null;
let storage: sqlite.Database | null = null;

beforeAll(async () => {
  Bun.spawnSync({
    cmd: [
      "cargo",
      "run",
      "--",
      "--config",
      "integration-test/config.yaml",
      "dump",
      "--db",
      "integration-test/database.sqlite",
      "--storage",
      "integration-test/storage.sqlite",
    ],
    env: {
      RUST_LOG: "rudis_cms=Trace",
    },
  });
  database = new sqlite.Database("./integration-test/database.sqlite");
  storage = new sqlite.Database("./integration-test/storage.sqlite");
});

describe("check post table sanity", () => {
  test("validate", async () => {
    const rows = database!.query("SELECT * FROM posts;").all();
    v.parse(v.array(post.table), rows);
  });
});
