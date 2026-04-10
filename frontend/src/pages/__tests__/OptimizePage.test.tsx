import { fireEvent, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";
import OptimizePage from "@/pages/OptimizePage";
import { renderWithProviders } from "@/test/renderWithProviders";

vi.mock("@/config/runtime", () => ({
  ENABLE_MOCK_API: true,
  MOCK_API_DELAY_MS: 0,
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
    expect(await screen.findByText("Phân bổ chi tiết & đề xuất rebalance")).toBeInTheDocument();
    expect(screen.getByText("Báo cáo trực quan")).toBeInTheDocument();
    expect(screen.getByText("Phân bổ giá trị đề xuất")).toBeInTheDocument();
    expect(screen.getByText("Tổng quan chỉ số tối ưu")).toBeInTheDocument();
    expect(screen.getByText("Khối lượng cổ phiếu mục tiêu")).toBeInTheDocument();
    expect(screen.getByText("So sánh tỷ trọng và giá trị theo mã")).toBeInTheDocument();
    expect(screen.getByText("Khoảng cách lợi nhuận với benchmark")).toBeInTheDocument();
    expect(screen.queryByText("Lưu theo dõi")).not.toBeInTheDocument();
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

  it("đề xuất số cổ phiếu theo ngân sách danh mục", async () => {
    renderWithProviders(
      <MemoryRouter initialEntries={["/toi-uu"]}>
        <OptimizePage />
      </MemoryRouter>,
    );

    expect(await screen.findByText("Danh mục chính")).toBeInTheDocument();
    fireEvent.click(await screen.findByRole("button", { name: "Bắt đầu tối ưu hóa" }));

    expect(await screen.findByText("Phân bổ chi tiết & đề xuất rebalance")).toBeInTheDocument();
    expect((await screen.findAllByText("1194 cp")).length).toBeGreaterThan(0);
    expect(screen.queryAllByText(/Hien tai/i)).toHaveLength(0);
  });
});
