import { screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";
import MarketPage from "@/pages/MarketPage";
import { renderWithProviders } from "@/test/renderWithProviders";

vi.mock("@/config/runtime", () => ({
  ENABLE_MOCK_API: true,
  MOCK_API_DELAY_MS: 0,
}));

describe("MarketPage", () => {
  it("render module cổ phiếu với tab tìm kiếm mặc định", async () => {
    renderWithProviders(
      <MemoryRouter initialEntries={["/thi-truong"]}>
        <MarketPage />
      </MemoryRouter>,
    );

    expect(await screen.findByText("Cổ phiếu")).toBeInTheDocument();
    expect(await screen.findByRole("tab", { name: "Tìm kiếm", selected: true })).toBeInTheDocument();
    expect(await screen.findByRole("tab", { name: "Theo dõi" })).toBeInTheDocument();
  });
});
