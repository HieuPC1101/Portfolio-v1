import { screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import DashboardPage from "@/pages/DashboardPage";
import { renderWithProviders } from "@/test/renderWithProviders";

vi.mock("@/config/runtime", () => ({
  ENABLE_MOCK_API: true,
  MOCK_API_DELAY_MS: 0,
}));

describe("DashboardPage", () => {
  it("render heading dashboard và dữ liệu top mover", async () => {
    renderWithProviders(<DashboardPage />);

    expect(await screen.findByText("Tổng quan")).toBeInTheDocument();
    expect(await screen.findByText("VCB")).toBeInTheDocument();
  });
});
