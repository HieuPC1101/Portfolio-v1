import { fireEvent, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import MarketPage from "@/pages/MarketPage";
import { renderWithProviders } from "@/test/renderWithProviders";

vi.mock("@/config/runtime", () => ({
  ENABLE_MOCK_API: true,
  MOCK_API_DELAY_MS: 0,
}));

describe("MarketPage", () => {
  function activateSubTabByName(name: string) {
    const tab = screen.getByRole("tab", { name });
    fireEvent.mouseDown(tab, { button: 0, ctrlKey: false });
  }

  it("render tổng quan thị trường với các khối chính", async () => {
    renderWithProviders(<MarketPage />);

    expect(await screen.findByText("Thị trường")).toBeInTheDocument();
    expect(await screen.findByText("Bản đồ ngành")).toBeInTheDocument();
    expect(await screen.findByText("MWG")).toBeInTheDocument();
    expect(await screen.findByText("Snapshot khối ngoại")).toBeInTheDocument();
    expect(await screen.findByText("Dòng tiền khối ngoại (tỷ VND)")).toBeInTheDocument();
  });

  it("chuyển được các sub tabs trong phần tổng quan", async () => {
    renderWithProviders(<MarketPage />);
    await screen.findByText("Thị trường");

    activateSubTabByName("Giảm mạnh");
    expect(await screen.findByText("HPG")).toBeInTheDocument();

    activateSubTabByName("1 tháng");
    expect(await screen.findByRole("tab", { name: "5 phiên" })).toBeInTheDocument();
    expect(await screen.findByRole("tab", { name: "Mua ròng" })).toBeInTheDocument();

    activateSubTabByName("Bán ròng");
    expect(await screen.findByRole("tab", { name: "Bán ròng" })).toHaveAttribute("aria-selected", "true");
  });
});
