import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";

const execFileAsync = promisify(execFile);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const CLI_PATH = path.resolve(__dirname, "../../bin/tencere.js");

test("CLI - Skill path and show subcommands", async () => {
  const { stdout: pathOut } = await execFileAsync(process.execPath, [CLI_PATH, "skill", "path"]);
  assert.ok(pathOut.trim().endsWith("skills/tencere/SKILL.md"));

  const { stdout: showOut } = await execFileAsync(process.execPath, [CLI_PATH, "skill", "show"]);
  assert.ok(showOut.includes("name: tencere"));
  assert.ok(showOut.includes("Tencere Embedded & Distributed Database"));
});

test("CLI - Skill install to custom target directory", async () => {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "tencere-skill-test-"));
  try {
    const targetDir = path.join(tmpDir, "custom-agent");
    const { stdout: installOut } = await execFileAsync(process.execPath, [
      CLI_PATH,
      "skill",
      "install",
      "--target",
      targetDir
    ]);

    assert.ok(installOut.includes("Custom target"));
    const installedFile = path.join(targetDir, "skills/tencere/SKILL.md");
    const exists = await fs.stat(installedFile).then(() => true).catch(() => false);
    assert.equal(exists, true);

    const content = await fs.readFile(installedFile, "utf-8");
    assert.ok(content.includes("name: tencere"));
    assert.ok(content.includes("durability"));
  } finally {
    await fs.rm(tmpDir, { recursive: true, force: true });
  }
});
