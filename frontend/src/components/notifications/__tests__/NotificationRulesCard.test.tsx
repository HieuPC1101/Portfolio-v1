import { screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { renderWithProviders } from "@/test/renderWithProviders";
import { NotificationRulesCard } from "@/components/notifications/NotificationRulesCard";

vi.mock("@/config/runtime", () => ({
  ENABLE_MOCK_API: true,
  MOCK_API_DELAY_MS: 0,
}));

describe("NotificationRulesCard", () => {
  it("hiển thị danh sách rule thông báo", async () => {
    renderWithProviders(<NotificationRulesCard />);

    expect(await screen.findByText("Quy tắc thông báo")).toBeInTheDocument();
    expect(await screen.findByText(/FPT/)).toBeInTheDocument();
  });
});
