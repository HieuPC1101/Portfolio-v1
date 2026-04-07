import { screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import MarketPage from "@/pages/MarketPage";
import { renderWithProviders } from "@/test/renderWithProviders";

vi.mock("@/config/env", () => ({
  isMockMode: true,
  MOCK_DELAY_MS: 0,
}));

describe("MarketPage", () => {
  it("render dữ liệu thị trường qua query hook", async () => {
    renderWithProviders(<MarketPage />);

    expect(await screen.findByText("Thị trường")).toBeInTheDocument();
    expect(await screen.findByText("MWG")).toBeInTheDocument();
  });
});
