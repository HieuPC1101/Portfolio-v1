import { screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import PortfolioPage from "@/pages/PortfolioPage";
import { renderWithProviders } from "@/test/renderWithProviders";

vi.mock("@/config/runtime", () => ({
  ENABLE_MOCK_API: true,
  MOCK_API_DELAY_MS: 0,
}));

vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual<typeof import("react-router-dom")>("react-router-dom");
  return {
    ...actual,
    useNavigate: () => vi.fn(),
    useSearchParams: () => [new URLSearchParams(), vi.fn()],
  };
});

describe("PortfolioPage", () => {
  it("render portfolio và một dòng holding", async () => {
    renderWithProviders(<PortfolioPage />);

    expect(await screen.findByText("Danh mục đầu tư")).toBeInTheDocument();
    expect((await screen.findAllByText("Danh mục chính")).length).toBeGreaterThan(0);
    expect(screen.getAllByText("VCB").length).toBeGreaterThan(0);
    expect(screen.getByText("Tạo danh mục")).toBeInTheDocument();
    expect(screen.getAllByText("Thêm").length).toBeGreaterThan(0);
  });
});
