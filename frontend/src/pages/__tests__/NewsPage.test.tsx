import { screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import NewsPage from "@/pages/NewsPage";
import { renderWithProviders } from "@/test/renderWithProviders";

vi.mock("@/config/env", () => ({
  isMockMode: true,
  MOCK_DELAY_MS: 0,
}));

describe("NewsPage", () => {
  it("render card tin tức đại diện", async () => {
    renderWithProviders(<NewsPage />);

    expect(await screen.findByText("Tin tức thị trường")).toBeInTheDocument();
    expect(await screen.findByText("VN-Index vượt mốc 1.280 điểm, dòng tiền ngoại quay trở lại")).toBeInTheDocument();
  });
});
