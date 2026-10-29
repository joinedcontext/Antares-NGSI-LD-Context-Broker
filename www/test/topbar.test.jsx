// TopBar: the ☰ menu that folds the actions on a phone, and the new-space
// dialog that validates the tenant name before it reaches the broker.
import React from "react";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("../src/state/board.js", async (orig) => ({
  ...(await orig()),
  addSpace: vi.fn(() => true),
}));
import { addSpace } from "../src/state/board.js";
import TopBar from "../src/components/TopBar.jsx";

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

describe("<TopBar> menu", () => {
  it("opens and closes, and an action closes it", () => {
    render(<TopBar health={null} onApi={() => {}} />);
    const btn = screen.getByTestId("btn-menu");
    const actions = screen.getByTestId("actions");
    expect(btn).toHaveAttribute("aria-expanded", "false");
    fireEvent.click(btn);
    expect(btn).toHaveAttribute("aria-expanded", "true");
    expect(actions).toHaveClass("open");
    fireEvent.click(screen.getByTestId("btn-arrange"));
    expect(actions).not.toHaveClass("open");
  });
});

describe("<TopBar> new space", () => {
  it("rejects a name the broker would refuse, inline", () => {
    render(<TopBar health={null} onApi={() => {}} />);
    fireEvent.click(screen.getByTestId("btn-space"));
    fireEvent.change(screen.getByTestId("space-name"), { target: { value: "old town" } });
    expect(screen.getByRole("alert")).toHaveTextContent("A-Z, a-z, 0-9 and - only");
    expect(screen.getByTestId("space-add")).toBeDisabled();
    fireEvent.submit(screen.getByTestId("space-dialog"));
    expect(addSpace).not.toHaveBeenCalled();
  });

  it("adds a valid name, trimmed, and closes", () => {
    render(<TopBar health={null} onApi={() => {}} />);
    fireEvent.click(screen.getByTestId("btn-space"));
    expect(screen.getByTestId("space-add")).toBeDisabled(); // empty
    fireEvent.change(screen.getByTestId("space-name"), { target: { value: " harbor-2 " } });
    expect(screen.queryByRole("alert")).toBeNull();
    fireEvent.click(screen.getByTestId("space-add"));
    expect(addSpace).toHaveBeenCalledWith("harbor-2");
    expect(screen.queryByTestId("space-dialog")).toBeNull();
  });
});
