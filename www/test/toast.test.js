// A burst of notifications must not cover a phone screen with toasts.
import { expect, it, vi } from "vitest";
import { board, toast } from "../src/state/board.js";

it("keeps the newest three toasts and drops each after 4 s", () => {
  vi.useFakeTimers();
  for (let i = 0; i < 5; i++) toast(`n${i}`);
  expect(board.toasts.map((t) => t.text)).toEqual(["n2", "n3", "n4"]);
  vi.advanceTimersByTime(4000);
  expect(board.toasts).toEqual([]);
  vi.useRealTimers();
});
