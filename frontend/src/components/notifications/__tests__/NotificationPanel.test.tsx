import { fireEvent, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { NotificationPanel } from "@/components/notifications/NotificationPanel";
import { renderWithProviders } from "@/test/renderWithProviders";

vi.mock("@/config/runtime", () => ({
  ENABLE_MOCK_API: true,
  MOCK_API_DELAY_MS: 0,
}));

describe("NotificationPanel", () => {
  it("hien thi 2 subtabs cho chua doc va da doc", async () => {
    renderWithProviders(<NotificationPanel />);

    expect(await screen.findByRole("tab", { name: /Chưa đọc \(2\)/ })).toBeInTheDocument();
    expect(await screen.findByRole("tab", { name: /Đã đọc \(1\)/ })).toBeInTheDocument();
  });

  it("subtab chua doc chi hien thong bao chua doc", async () => {
    renderWithProviders(<NotificationPanel />);

    expect(await screen.findByText("FPT vượt ngưỡng 140000")).toBeInTheDocument();
    expect(screen.queryByText("Bảo mật tài khoản")).not.toBeInTheDocument();
  });

  it("subtab da doc hien thong bao da doc", async () => {
    renderWithProviders(<NotificationPanel />);

    fireEvent.mouseDown(await screen.findByRole("tab", { name: /Đã đọc \(1\)/ }), {
      button: 0,
      ctrlKey: false,
    });

    expect(await screen.findByText("Bảo mật tài khoản")).toBeInTheDocument();
    expect(screen.queryByText("FPT vượt ngưỡng 140000")).not.toBeInTheDocument();
  });
});
