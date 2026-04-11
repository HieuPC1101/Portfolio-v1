import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import WatchlistPage from "@/pages/WatchlistPage";
import { showPortfolioMiniToast } from "@/components/notifications/PortfolioMiniToast";
import { renderWithProviders } from "@/test/renderWithProviders";

vi.mock("@/components/notifications/PortfolioMiniToast", () => ({
  showPortfolioMiniToast: vi.fn(),
}));

vi.mock("@/config/runtime", () => ({
  ENABLE_MOCK_API: true,
  MOCK_API_DELAY_MS: 0,
}));

describe("WatchlistPage", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  function activateTabByName(name: string) {
    const tab = screen.getByRole("tab", { name });
    fireEvent.mouseDown(tab, { button: 0, ctrlKey: false });
  }

  function renderPage() {
    return renderWithProviders(
      <MemoryRouter initialEntries={["/thi-truong"]}>
        <Routes>
          <Route path="/thi-truong" element={<WatchlistPage />} />
          <Route path="/co-phieu/:symbol" element={<div>Stock detail page</div>} />
        </Routes>
      </MemoryRouter>,
    );
  }

  it("render tabs và tab mặc định là Tìm kiếm", async () => {
    renderPage();

    expect(await screen.findByText("Cổ phiếu")).toBeInTheDocument();
    const searchTab = screen.getByRole("tab", { name: "Tìm kiếm" });
    const watchlistTab = screen.getByRole("tab", { name: "Theo dõi" });

    expect(searchTab).toHaveAttribute("aria-selected", "true");
    expect(watchlistTab).toHaveAttribute("aria-selected", "false");
    expect(screen.queryByRole("tab", { name: "Gợi ý" })).not.toBeInTheDocument();
    expect(screen.getByText("Tìm mã cổ phiếu")).toBeInTheDocument();
  });

  it("thêm mã từ tab Tìm kiếm và hiển thị ở tab Theo dõi", async () => {
    renderPage();

    await screen.findByText("Cổ phiếu");
    fireEvent.change(screen.getByRole("combobox"), { target: { value: "DGC" } });

    const searchResultOption = await screen.findByRole("option", { name: /DGC/i });
    fireEvent.click(within(searchResultOption).getAllByRole("button")[0]);

    fireEvent.click(screen.getAllByRole("button", { name: "Theo dõi ngay" })[0]);

    expect(await screen.findByRole("tab", { name: "Theo dõi", selected: true })).toBeInTheDocument();
    expect(await screen.findByLabelText("Xoa DGC khoi danh sach theo doi")).toBeInTheDocument();

    await waitFor(() => {
      expect(showPortfolioMiniToast).toHaveBeenCalledWith(expect.objectContaining({
        tone: "success",
      }));
    });
  });

  it("hiển thị danh sách gợi ý ngay trong tab Tìm kiếm", async () => {
    renderPage();

    await screen.findByText("Cổ phiếu");
    expect(await screen.findByText("Mã gợi ý cho bạn")).toBeInTheDocument();
  });

  it("xóa mã khỏi tab Theo dõi", async () => {
    renderPage();

    await screen.findByText("Cổ phiếu");
    activateTabByName("Theo dõi");
    const removeButtons = await screen.findAllByRole("button", { name: /Xoa .* khoi danh sach theo doi/i });
    const removeLabel = removeButtons[0].getAttribute("aria-label");
    expect(removeLabel).toBeTruthy();

    const matched = removeLabel?.match(/^Xoa\s+(.+)\s+khoi danh sach theo doi$/i);
    const removedSymbol = matched?.[1];
    expect(removedSymbol).toBeTruthy();

    const removeButton = removeButtons[0];
    fireEvent.click(removeButton);
    fireEvent.click(await screen.findByRole("button", { name: "Xóa mã" }));

    await waitFor(() => {
      expect(screen.queryByLabelText(`Xoa ${removedSymbol} khoi danh sach theo doi`)).not.toBeInTheDocument();
    });
  });

  it("đi tới trang chi tiết từ tab Tìm kiếm", async () => {
    renderPage();

    await screen.findByText("Cổ phiếu");
    fireEvent.change(screen.getByRole("combobox"), { target: { value: "DGC" } });
    const searchResultOption = await screen.findByRole("option", { name: /DGC/i });
    fireEvent.click(within(searchResultOption).getAllByRole("button")[0]);
    fireEvent.click(screen.getAllByRole("button", { name: "Xem chi tiết" })[0]);

    expect(await screen.findByText("Stock detail page")).toBeInTheDocument();
  });

  it("đi tới trang chi tiết từ card tab Theo dõi", async () => {
    renderPage();

    await screen.findByText("Cổ phiếu");
    activateTabByName("Theo dõi");
    fireEvent.click(await screen.findByText("FPT"));

    expect(await screen.findByText("Stock detail page")).toBeInTheDocument();
  });

  it("render danh sách theo dõi có card đại diện", async () => {
    renderPage();

    await screen.findByText("Cổ phiếu");
    activateTabByName("Theo dõi");
    const removeButtons = await screen.findAllByRole("button", { name: /Xoa .* khoi danh sach theo doi/i });
    expect(removeButtons.length).toBeGreaterThan(0);
  });
});
