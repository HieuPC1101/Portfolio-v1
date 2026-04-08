import { screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";
import OptimizePage from "@/pages/OptimizePage";
import { renderWithProviders } from "@/test/renderWithProviders";

vi.mock("@/config/env", () => ({
  isMockMode: true,
  MOCK_DELAY_MS: 0,
}));

describe("OptimizePage", () => {
  it("render form và panels tối ưu hóa", async () => {
    renderWithProviders(
      <MemoryRouter>
        <OptimizePage />
      </MemoryRouter>,
    );

    expect(screen.getByText("Tối ưu hóa danh mục")).toBeInTheDocument();
    expect(await screen.findByText("Phân bổ tỷ trọng")).toBeInTheDocument();
  });
});
