// E2E smoke against the BUILT app: boot (wasm broker in a real browser),
// auto-demo, React Flow board populated, tenant sheet + filters, history
// panel, request log, API console, then the phone layout at 390 px.
// Run: npm run e2e (SHOTS=dir picks where the screenshots go)
import { spawn } from "node:child_process";
import { createRequire } from "node:module";
import { chromium } from "playwright-core";

const AXE = createRequire(import.meta.url).resolve("axe-core/axe.min.js");

// screenshots of every run land here (e2e/*.png is git-ignored)
const SHOTS = process.env.SHOTS ?? "e2e";

// --host 127.0.0.1: vite's default "localhost" can bind ::1 only, and the
// test (and CI runners) dial IPv4.
// detached → its own process group, so killPreview can take down the npx
// wrapper AND the vite child (kill(preview.pid) orphans vite, which then
// holds the port and this process's stdio pipes open forever).
const preview = spawn("npx", ["vite", "preview", "--port", "42096", "--strictPort", "--host", "127.0.0.1"], {
  cwd: new URL("..", import.meta.url).pathname,
  stdio: "pipe",
  detached: true,
});
const killPreview = () => { try { process.kill(-preview.pid, "SIGKILL"); } catch {} };
await new Promise((resolve, reject) => {
  preview.stdout.on("data", (d) => d.toString().includes("42096") && resolve());
  preview.stderr.on("data", (d) => process.stderr.write(d));
  preview.on("exit", (c) => reject(new Error(`vite preview exited ${c}`)));
  setTimeout(() => reject(new Error("preview server timeout")), 20_000);
});

const browser = await chromium.launch();
const page = await (await browser.newContext({ viewport: { width: 1500, height: 950 } })).newPage();
page.on("pageerror", (e) => console.log("[pageerror]", e.message));
let active = page; // the page a failure screenshot shows

const fail = async (msg) => {
  console.error(`FAIL: ${msg}`);
  await browser.close().catch(() => {});
  killPreview();
  process.exit(1);
};

// axe-core, WCAG 2.1 A and AA: any violation fails the run
const audit = async (p, label) => {
  await p.addScriptTag({ path: AXE });
  const found = await p.evaluate(async () =>
    (await axe.run(document, { runOnly: ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"] })).violations.map(
      (v) => `${v.impact} ${v.id}: ${v.help} at ${v.nodes.slice(0, 3).map((n) => n.target.join(" ")).join(" | ")}`));
  if (found.length) await fail(`${label}: WCAG violations\n  ${found.join("\n  ")}`);
  console.log(`${label}: axe WCAG 2.1 A/AA clean`);
};

try {
  await page.goto("http://127.0.0.1:42096/", { waitUntil: "load" });

  // boot: mode pill leaves "booting"; auto-demo builds the city
  await page.waitForFunction(
    () => document.querySelector("[data-mode]")?.dataset.mode !== "booting",
    { timeout: 60_000 },
  );
  const mode = await page.evaluate(() => document.querySelector("[data-mode]").dataset.mode);
  console.log("mode:", mode);
  if (!["opfs-worker", "in-page"].includes(mode)) await fail(`unexpected mode ${mode}`);

  // board: 9 space nodes + 13 device nodes from the demo
  await page.waitForFunction(
    () => document.querySelectorAll(".react-flow__node").length >= 20,
    { timeout: 60_000 },
  );
  const nodes = await page.evaluate(() => document.querySelectorAll(".react-flow__node").length);
  const edges = await page.evaluate(() => document.querySelectorAll(".react-flow__edge").length);
  console.log(`board: ${nodes} nodes, ${edges} edges`);
  if (edges < 10) await fail(`expected ≥10 edges (7 CSR + pipes), got ${edges}`);
  // the minimap draws from the measured node sizes the board hands back
  await page.waitForFunction(
    () => document.querySelectorAll(".react-flow__minimap-node").length >= 20,
    { timeout: 10_000 },
  ).catch(() => fail("minimap draws no nodes"));

  // click the hub → sheet shows federated rows with origin chips
  await page.click('[data-id="s:smart-city"]');
  await page.waitForFunction(
    () => document.querySelectorAll('[data-testid="sheet-row"]').length >= 3,
    { timeout: 30_000 },
  );
  const before = await page.evaluate(
    () => document.querySelectorAll('[data-testid="sheet-row"]').length);
  console.log("sheet rows (smart-city):", before);

  // origin filter: pick a federated peer → only its rows remain
  const fedOrigin = await page.evaluate(() => {
    const sel = document.querySelector('[data-testid="filter-origin"]');
    const opt = [...sel.options].find((o) => o.value !== "all" && o.value !== "local");
    if (!opt) return null;
    sel.value = opt.value;
    sel.dispatchEvent(new Event("change", { bubbles: true }));
    return opt.value;
  });
  if (!fedOrigin) await fail("no federated origin option in the sheet");
  await page.waitForFunction(
    (n) => document.querySelectorAll('[data-testid="sheet-row"]').length < n,
    before,
    { timeout: 10_000 },
  );
  console.log(`origin filter "${fedOrigin}" narrows the sheet`);
  await page.evaluate(() => {
    const sel = document.querySelector('[data-testid="filter-origin"]');
    sel.value = "all";
    sel.dispatchEvent(new Event("change", { bubbles: true }));
  });

  // row click → temporal history panel appears (chart or "no history yet")
  await page.click('[data-testid="sheet-row"]');
  await page.waitForSelector('[data-testid="history"]', { timeout: 10_000 });
  console.log("history panel opens on row click");
  await audit(page, "desktop");

  // request log carries real traffic
  await page.click('[data-testid="reqlog-toggle"]');
  await page.waitForFunction(
    () => document.querySelectorAll('[data-testid="reqlog-entries"] div').length >= 5,
    { timeout: 10_000 },
  );
  console.log("request log populated");

  // API console: Swagger UI loads the vendored ETSI yaml, renders operations
  await page.click('[data-testid="btn-api"]');
  await page.waitForFunction(
    () => document.querySelectorAll(".opblock").length > 20,
    { timeout: 60_000 },
  );
  const ops = await page.evaluate(() => document.querySelectorAll(".opblock").length);
  console.log(`API console renders the ETSI OpenAPI spec (${ops} operations)`);

  // …and "Execute" round-trips into the wasm broker: GET /types → 200
  const typesBlock = page
    .locator(".opblock")
    .filter({ has: page.locator(".opblock-summary-path", { hasText: /^\/types$/ }) })
    .first();
  await typesBlock.locator(".opblock-summary").click();
  await page.waitForSelector(".opblock.is-open", { timeout: 10_000 });
  // tryItOutEnabled pre-activates try-out; fall back to the button if not
  if (!(await page.$(".opblock.is-open .btn.execute")))
    await page.click(".opblock.is-open .try-out__btn");
  await page.click(".opblock.is-open .btn.execute");
  await page.waitForFunction(
    () => [...document.querySelectorAll(
      ".opblock.is-open .live-responses-table .response-col_status",
    )].some((n) => n.textContent.trim().startsWith("200")),
    { timeout: 30_000 },
  );
  console.log("Execute GET /types via the console → 200 from the wasm broker");
  await page.click('[data-testid="api-close"]');

  await page.screenshot({ path: `${SHOTS}/desktop.png` });

  // phone: one pane at a time behind the bottom tabs, actions behind ☰,
  // and nothing wider than the screen
  const phone = await (await browser.newContext({
    viewport: { width: 390, height: 844 }, deviceScaleFactor: 2, isMobile: true, hasTouch: true,
  })).newPage();
  phone.on("pageerror", (e) => console.log("[pageerror phone]", e.message));
  active = phone;
  await phone.goto("http://127.0.0.1:42096/", { waitUntil: "load" });
  await phone.waitForFunction(
    () => document.querySelectorAll(".react-flow__node").length >= 20,
    { timeout: 60_000 },
  );
  const overflow = async () => phone.evaluate(
    () => document.documentElement.scrollWidth - window.innerWidth);
  if (await overflow() > 0) await fail(`phone: page ${await overflow()}px wider than the screen`);
  if (await phone.isVisible('[data-testid="drawer"]')) await fail("phone: board and sheet both shown");
  const clash = await phone.evaluate(() => {
    const a = document.querySelector(".react-flow__controls")?.getBoundingClientRect();
    const b = document.querySelector(".reqlog")?.getBoundingClientRect();
    return a && b && a.left < b.right && b.left < a.right && a.top < b.bottom && b.top < a.bottom;
  });
  if (clash) await fail("phone: the request log covers the zoom controls");
  await phone.screenshot({ path: `${SHOTS}/phone-board.png` });
  await audit(phone, "phone board");

  await phone.tap('[data-testid="btn-menu"]');
  await phone.waitForSelector('[data-testid="btn-demo"]', { state: "visible", timeout: 5_000 });
  await phone.screenshot({ path: `${SHOTS}/phone-menu.png` });
  await phone.tap('[data-testid="btn-space"]');
  await phone.fill('[data-testid="space-name"]', "old town");
  await phone.waitForSelector('[role="alert"]', { timeout: 5_000 });
  await phone.screenshot({ path: `${SHOTS}/phone-new-space.png` });
  await phone.fill('[data-testid="space-name"]', "harbor-e2e");
  await phone.tap('[data-testid="space-add"]');
  await phone.waitForSelector('[data-id="s:harbor-e2e"]', { timeout: 10_000 });
  console.log("phone: ☰ menu, new space dialog validates and adds");

  // tapping a space opens its data: on a phone the board and sheet share the screen
  await phone.tap('[data-id="s:smart-city"]');
  await phone.waitForSelector('[data-testid="drawer"]', { state: "visible", timeout: 5_000 });
  await phone.waitForFunction(
    () => document.querySelectorAll('[data-testid="sheet-row"]').length >= 3,
    { timeout: 30_000 },
  );
  if (await phone.isVisible('[data-testid="board"]')) await fail("phone: board still shown on the data tab");
  await phone.tap('[data-testid="sheet-row"]');
  await phone.waitForSelector('[data-testid="history"]', { timeout: 10_000 });
  if (await overflow() > 0) await fail(`phone data tab: ${await overflow()}px too wide`);
  await phone.screenshot({ path: `${SHOTS}/phone-data.png` });
  await audit(phone, "phone data");
  await phone.tap('[data-testid="tab-board"]');
  await phone.waitForSelector('[data-testid="board"]', { state: "visible", timeout: 5_000 });
  console.log("phone: a tapped space opens its sheet, tabs switch back, history opens, no horizontal overflow");

  // dark theme: the same contrast rules on the dark palette
  const dark = await (await browser.newContext({
    viewport: { width: 390, height: 844 }, deviceScaleFactor: 2, isMobile: true, hasTouch: true, colorScheme: "dark",
  })).newPage();
  active = dark;
  await dark.goto("http://127.0.0.1:42096/", { waitUntil: "load" });
  await dark.waitForFunction(
    () => document.querySelectorAll(".react-flow__node").length >= 20,
    { timeout: 60_000 },
  );
  await audit(dark, "phone dark board");
  await dark.screenshot({ path: `${SHOTS}/phone-dark.png` });

  console.log("PASS: react app e2e — boot, demo board, sheet + filters, history, request log, api console, phone layout, WCAG 2.1 AA (light + dark)");
  await browser.close();
  killPreview();
  process.exit(0);
} catch (e) {
  await active.screenshot({ path: `${SHOTS}/last-fail.png` }).catch(() => {});
  await fail(String(e));
}
