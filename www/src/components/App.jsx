import React, { useEffect, useState } from "react";
import { bootTransport, brokerFetch, onNotification, transport } from "../broker/transport.js";
import { installVirtualHost } from "../broker/virtualhost.js";
import * as apiMod from "../broker/api.js";
import { board, createDemo, refreshAll, startPolling, toast } from "../state/board.js";
import { load, save } from "../persist.js";
import { startAllPipes, startPipe } from "../state/pipes.js";
import { useBoard, useTransport } from "../hooks.js";
import TopBar from "./TopBar.jsx";
import Board from "./Board.jsx";
import TenantSheet from "./TenantSheet.jsx";
import History from "./History.jsx";
import RequestLog from "./RequestLog.jsx";

// RapiDoc (~350 kB gz) rides along only when the console is opened.
const ApiConsole = React.lazy(() => import("./ApiConsole.jsx"));

export default function App() {
  useBoard();
  useTransport();
  const [health, setHealth] = useState(null);
  const [selectedSpace, setSelectedSpace] = useState("smart-city");
  const [picked, setPicked] = useState(null); // {space,id,attr,emoji,type}
  const [apiOpen, setApiOpen] = useState(false);
  const [drawerW, setDrawerW] = useState(() => Number(load("antares.drawerw")) || 420);
  // phones show one pane at a time; on a wide screen both, and this is unused
  const [pane, setPane] = useState("board");

  useEffect(() => {
    let alive = true;
    (async () => {
      await bootTransport();
      installVirtualHost(brokerFetch);
      const h = await apiMod.health().catch(() => null);
      if (!alive) return;
      setHealth(h);
      // First visit: the board demos itself — once, pristine boards only.
      if (!load("antares.demoed")) {
        await refreshAll();
        const pristine =
          !board.pipes.length &&
          ![...board.links.values()].some((ls) => ls.length) &&
          ![...board.ents.values()].some((c) => c.local.length);
        if (pristine) await createDemo({ startPipe });
        save("antares.demoed", true);
      }
      await refreshAll();
      startAllPipes();
      startPolling();
    })();
    const off = onNotification((n) => {
      const ids = [...new Set((n.data ?? []).map((e) => e.id.split(":").pop()))].join(", ");
      toast(`🔔 ${ids || n.id || "notification"}`);
    });
    return () => {
      alive = false;
      off();
    };
  }, []);

  const spaceExists = board.spaces.some((s) => s.name === selectedSpace);
  const shownSpace = spaceExists ? selectedSpace : "default";

  return (
    <div className="app" style={{ "--drawer-w": `${drawerW}px` }}>
      <TopBar health={health} onApi={() => setApiOpen(true)} />
      {apiOpen && (
        <React.Suspense fallback={null}>
          <ApiConsole onClose={() => setApiOpen(false)} />
        </React.Suspense>
      )}
      <main className={`stage show-${pane}`}>
        <Board
          selected={shownSpace}
          onSelectSpace={(name) => {
            setSelectedSpace(name);
            setPicked(null);
            // a phone shows one pane: a tapped space opens its data
            if (window.matchMedia?.("(max-width: 900px)").matches) setPane("data");
          }}
        />
        <div
          className="drawer-resize"
          title="drag to resize"
          onPointerDown={(e) => {
            e.preventDefault();
            const startX = e.clientX, startW = drawerW;
            const move = (ev) => {
              const w = Math.min(900, Math.max(300, startW + startX - ev.clientX));
              setDrawerW(w);
              save("antares.drawerw", w);
            };
            const up = () => {
              window.removeEventListener("pointermove", move);
              window.removeEventListener("pointerup", up);
            };
            window.addEventListener("pointermove", move);
            window.addEventListener("pointerup", up);
          }}
        />
        <aside className="drawer" data-testid="drawer">
          <TenantSheet space={shownSpace} picked={picked} onPick={setPicked} />
          {picked && <History key={`${picked.space}/${picked.id}`} {...picked} />}
        </aside>
      </main>
      <nav className="tabs" aria-label="view">
        <button className={pane === "board" ? "primary" : ""} aria-pressed={pane === "board"}
          data-testid="tab-board" onClick={() => setPane("board")}>🗺 board</button>
        <button className={pane === "data" ? "primary" : ""} aria-pressed={pane === "data"}
          data-testid="tab-data" onClick={() => setPane("data")}>📋 {shownSpace}</button>
      </nav>
      <div className="toasts" role="status" aria-live="polite">
        {board.toasts.map((t) => (
          <div key={t.id} className="toast">{t.text}</div>
        ))}
      </div>
      <RequestLog />
      {transport.mode !== "opfs-worker" && transport.mode !== "booting" && (
        <div className="banner" title={transport.bootError ?? ""}>
          ⚠ ephemeral store — no persistence in {transport.mode} mode; close the
          tab owning antares.redb and reload
        </div>
      )}
    </div>
  );
}
