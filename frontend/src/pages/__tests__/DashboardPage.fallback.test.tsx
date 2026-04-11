import { screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { DashboardData } from "@/types/dashboard";
import { dashboardMock } from "@/mocks/dashboard.mock";
import DashboardPage from "@/pages/DashboardPage";
import { renderWithProviders } from "@/test/renderWithProviders";

const emptyMoversData: DashboardData = {
  ...dashboardMock,
  topGainers: [],
  topLosers: [],
  topMostActive: [],
};

vi.mock("@/hooks/useDashboardQuery", () => ({
  useDashboardQuery: () => ({
    data: emptyMoversData,
    isPending: false,
    isError: false,
    refetch: vi.fn(),
    isFetching: false,
    dataUpdatedAt: Date.now(),
  }),
}));

describe("DashboardPage fallback movers", () => {
  it("hiển thị fallback movers khi dữ liệu biến động trả rỗng", async () => {
    renderWithProviders(<DashboardPage />);

    expect(await screen.findByText("Biến động nổi bật")).toBeInTheDocument();
    expect(await screen.findByText("MWG")).toBeInTheDocument();
  });
});
