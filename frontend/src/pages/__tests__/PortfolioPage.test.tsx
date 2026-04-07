import { screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import PortfolioPage from "@/pages/PortfolioPage";
import { renderWithProviders } from "@/test/renderWithProviders";

vi.mock("@/config/env", () => ({
  isMockMode: true,
  MOCK_DELAY_MS: 0,
}));

describe("PortfolioPage", () => {
  it("render portfolio và một dòng holding", async () => {
    renderWithProviders(<PortfolioPage />);

    expect(await screen.findByText("Danh mục đầu tư")).toBeInTheDocument();
    expect(await screen.findByText("Danh mục chính")).toBeInTheDocument();
    expect(screen.getAllByText("VCB").length).toBeGreaterThan(0);
  });
});
