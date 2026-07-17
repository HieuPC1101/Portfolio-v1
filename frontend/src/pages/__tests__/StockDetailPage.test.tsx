import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import StockDetailPage from "@/pages/StockDetailPage";

vi.mock("@/components/common/CandlestickChart", () => ({
  CandlestickChart: () => <div data-testid="candlestick-chart" />,
}));

vi.mock("@/repositories/marketRepository", () => ({
  getStockDetail: vi.fn().mockResolvedValue({
    symbol: "FPT",
    name: "FPT Corporation",
    exchange: "HOSE",
    sector: "Công nghệ",
    price: 74,
    percent: 1.2,
    history30d: [
      { date: "2026-04-01", close: 73 },
      { date: "2026-04-02", close: 74 },
    ],
    ohlc30d: [
      { date: "2026-04-01", open: 72, high: 74, low: 71, close: 73, volume: 1230000 },
      { date: "2026-04-02", open: 73, high: 75, low: 72, close: 74, volume: 1510000 },
    ],
  }),
  getStockNews: vi.fn().mockResolvedValue([]),
  getStockOverview: vi.fn().mockResolvedValue({
    symbol: "FPT",
    companyName: "CTCP FPT",
    exchange: "HOSE",
    sector: "Công nghệ và thông tin",
    industry: "Công nghệ và thông tin",
    marketCap: null,
    sharesOutstanding: 1704000000,
    freeFloat: null,
    listingDate: null,
    headquarters: null,
    employeeCount: null,
    businessSummary: null,
    latestHighlights: [],
    asOfDate: null,
    source: "vnstock",
  }),
  getStockRatios: vi.fn().mockResolvedValue({
    symbol: "FPT",
    pe: null,
    pb: null,
    evEbitda: null,
    grossMargin: null,
    netMargin: null,
    roe: null,
    roa: null,
    debtToEquity: null,
    asOfDate: null,
    reportingPeriod: null,
    qualityFlags: [],
    source: "vnstock",
  }),
}));

function renderPage() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });

  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={["/co-phieu/FPT"]}>
        <Routes>
          <Route path="/co-phieu/:symbol" element={<StockDetailPage />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("StockDetailPage", () => {
  it("hiển thị giá đúng đơn vị và có tab biểu đồ nến", async () => {
    renderPage();

    expect(await screen.findByText("74.000 ₫")).toBeInTheDocument();
    expect(await screen.findByRole("tab", { name: "Biểu đồ nến" })).toBeInTheDocument();
  });

  it("ẩn mô tả và card chỉ số khi không có dữ liệu, nhưng vẫn hiển thị vốn hóa tính từ số cổ phiếu", async () => {
    renderPage();

    expect(await screen.findByText("Phân tích doanh nghiệp")).toBeInTheDocument();
    expect(screen.getByText("Giá gần nhất")).toBeInTheDocument();
    expect(screen.getByText("Ngành")).toBeInTheDocument();
    expect(screen.getByText("Sàn")).toBeInTheDocument();
    expect(screen.getByText("Số cổ phiếu lưu hành")).toBeInTheDocument();
    expect(await screen.findByText("Vốn hóa")).toBeInTheDocument();
    expect(screen.getByText("126.096 tỷ đ")).toBeInTheDocument();
    expect(screen.queryByText("Mô tả doanh nghiệp")).not.toBeInTheDocument();
    expect(screen.queryByText("P/E")).not.toBeInTheDocument();
  });
});
