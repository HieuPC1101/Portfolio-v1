import { screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import WatchlistPage from "@/pages/WatchlistPage";
import { renderWithProviders } from "@/test/renderWithProviders";

vi.mock("@/config/runtime", () => ({
  ENABLE_MOCK_API: true,
  MOCK_API_DELAY_MS: 0,
}));

describe("WatchlistPage", () => {
  it("render watchlist card đại diện", async () => {
    renderWithProviders(<WatchlistPage />);

    expect(await screen.findByText("Watchlist")).toBeInTheDocument();
    expect(await screen.findByText("Vietcombank")).toBeInTheDocument();
  });
});
