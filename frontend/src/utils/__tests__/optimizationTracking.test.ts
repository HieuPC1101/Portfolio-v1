import { beforeEach, describe, expect, it } from "vitest";
import {
  clearOptimizationTracks,
  pushOptimizationTrack,
  readOptimizationTracks,
} from "@/utils/optimizationTracking";

describe("optimizationTracking", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it("pushOptimizationTrack lưu snapshot mới nhất lên đầu", () => {
    pushOptimizationTrack({
      portfolioId: "1",
      portfolioName: "Danh mục chính",
      algorithmId: "max_sharpe",
      algorithmName: "Tối đa Sharpe Ratio",
      expectedReturn: 11.46,
      volatility: 31.92,
      sharpeRatio: 0.2,
      topWeightsSummary: "VNM 100%, FPT 0%",
      createdAt: "2026-04-10T03:30:00.000Z",
    });

    const entries = readOptimizationTracks();
    expect(entries).toHaveLength(1);
    expect(entries[0]).toMatchObject({
      portfolioName: "Danh mục chính",
      algorithmId: "max_sharpe",
      sharpeRatio: 0.2,
    });
  });

  it("giới hạn tối đa 20 bản ghi theo dõi", () => {
    for (let index = 0; index < 22; index += 1) {
      pushOptimizationTrack({
        portfolioId: "1",
        portfolioName: "Danh mục chính",
        algorithmId: "max_sharpe",
        algorithmName: "Tối đa Sharpe Ratio",
        expectedReturn: index,
        volatility: 10,
        sharpeRatio: 1,
        topWeightsSummary: "VNM 100%",
        createdAt: `2026-04-10T03:${String(index).padStart(2, "0")}:00.000Z`,
      });
    }

    const entries = readOptimizationTracks();
    expect(entries).toHaveLength(20);
    expect(entries[0].expectedReturn).toBe(21);
    expect(entries[19].expectedReturn).toBe(2);
  });

  it("clearOptimizationTracks xóa toàn bộ lịch sử", () => {
    pushOptimizationTrack({
      portfolioId: "1",
      portfolioName: "Danh mục chính",
      algorithmId: "markowitz",
      algorithmName: "Markowitz",
      expectedReturn: 8,
      volatility: 12,
      sharpeRatio: 0.66,
      topWeightsSummary: "VCB 50%, FPT 50%",
      createdAt: "2026-04-10T03:30:00.000Z",
    });

    clearOptimizationTracks();

    expect(readOptimizationTracks()).toEqual([]);
  });
});
