import { fireEvent, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";
import OptimizePage from "@/pages/OptimizePage";
import { renderWithProviders } from "@/test/renderWithProviders";

vi.mock("@/config/env", () => ({
  isMockMode: true,
  MOCK_DELAY_MS: 0,
}));

describe("OptimizePage", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it("render form và panels tối ưu hóa", async () => {
    renderWithProviders(
      <MemoryRouter initialEntries={["/toi-uu"]}>
        <OptimizePage />
      </MemoryRouter>,
    );

    expect(screen.getByText("Tối ưu hóa danh mục")).toBeInTheDocument();
    expect(await screen.findByText("Danh mục chính")).toBeInTheDocument();
    expect(screen.getByText("VCB")).toBeInTheDocument();
    fireEvent.click(await screen.findByRole("button", { name: "Bắt đầu tối ưu hóa" }));
    expect(await screen.findByText("Phân bổ tỷ trọng")).toBeInTheDocument();
  });

  it("chọn danh mục theo portfolioId trên URL", async () => {
    renderWithProviders(
      <MemoryRouter initialEntries={["/toi-uu?portfolioId=2"]}>
        <OptimizePage />
      </MemoryRouter>,
    );

    expect(await screen.findByText("Danh mục tăng trưởng")).toBeInTheDocument();
    expect(screen.getByText("TCB")).toBeInTheDocument();
    expect(screen.getByText("MBB")).toBeInTheDocument();
  });
});
