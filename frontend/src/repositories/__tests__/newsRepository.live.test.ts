import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/config/runtime", () => ({
  ENABLE_MOCK_API: false,
  MOCK_API_DELAY_MS: 0,
}));

const apiGetMock = vi.fn();

vi.mock("@/lib/api", () => ({
  apiGet: (path: string) => apiGetMock(path),
}));

import { getNewsFeed } from "@/repositories/newsRepository";

describe("getNewsFeed live mode", () => {
  beforeEach(() => {
    apiGetMock.mockReset();
  });

  it("thêm refresh query khi tải lại", async () => {
    apiGetMock.mockResolvedValue([]);

    await getNewsFeed({ limit: 10, refresh: true });

    expect(apiGetMock).toHaveBeenCalledWith("/api/v1/market/news?limit=10&refresh=true");
  });

  it("decode entity html cho tiêu đề và nội dung", async () => {
    apiGetMock.mockResolvedValue([
      {
        title: "C&#225;c ng&#226;n h&#224;ng đồng thuận giảm l&#227;i suất",
        summary: "&#272;&#7897;ng th&#225;i n#224;y được đưa ra trong bối cảnh thanh khoản",
        source: "VnEconomy",
        published_at: "2026-04-09T08:30:00Z",
        category: "T&#224;i ch&#237;nh",
        url: "https://example.com/a",
      },
    ]);

    const data = await getNewsFeed();

    expect(data.items[0].title).toBe("Các ngân hàng đồng thuận giảm lãi suất");
    expect(data.items[0].summary).toContain("Động thái này");
    expect(data.items[0].category).toBe("Tài chính");
  });

  it("xem timestamp không timezone là UTC", async () => {
    vi.useFakeTimers();
    try {
      vi.setSystemTime(new Date("2026-04-09T08:00:00Z"));

      apiGetMock.mockResolvedValue([
        {
          title: "Tin test",
          summary: "Nội dung test",
          source: "CafeF",
          published_at: "2026-04-09T08:00:00",
          category: "Thị trường",
          url: "https://example.com/b",
        },
      ]);

      const data = await getNewsFeed();
      expect(data.items[0].time).toBe("Vừa xong");
    } finally {
      vi.useRealTimers();
    }
  });

  it("ưu tiên symbols từ API và fallback từ tiêu đề", async () => {
    apiGetMock.mockResolvedValue([
      {
        title: "Khối ngoại mua mạnh FPT và HPG",
        summary: "Dòng tiền cải thiện",
        source: "CafeF",
        published_at: "2026-04-09T08:00:00Z",
        category: "Thị trường",
        symbols: ["fpt", "hpg", "ETF"],
        url: "https://example.com/c",
      },
      {
        title: "SSI bật tăng trần",
        summary: "Thanh khoản cao",
        source: "VietStock",
        published_at: "2026-04-09T09:00:00Z",
        category: "Thị trường",
        symbols: [],
        url: "https://example.com/d",
      },
    ]);

    const data = await getNewsFeed();
    expect(data.items[0].symbols).toEqual(["FPT", "HPG"]);
    expect(data.items[1].symbols).toContain("SSI");
  });
});
