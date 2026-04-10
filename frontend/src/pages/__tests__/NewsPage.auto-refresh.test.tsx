import { act, render } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import NewsPage from "@/pages/NewsPage";
import { useNewsQuery } from "@/hooks/useNewsQuery";

vi.mock("@/hooks/useNewsQuery", () => ({
  useNewsQuery: vi.fn(),
}));

const useNewsQueryMock = vi.mocked(useNewsQuery);

describe("NewsPage auto refresh", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-04-10T09:00:00.000Z"));
    useNewsQueryMock.mockReturnValue({
      data: { items: [] },
      isFetching: false,
      isPending: false,
      isError: false,
      dataUpdatedAt: Date.now(),
    } as never);
  });

  afterEach(() => {
    vi.clearAllMocks();
    vi.useRealTimers();
  });

  it("làm mới ngay khi quay lại tab nếu đã quá 5 phút", () => {
    render(<NewsPage />);
    expect(useNewsQueryMock).toHaveBeenLastCalledWith(0);

    act(() => {
      vi.setSystemTime(new Date("2026-04-10T09:06:00.000Z"));
      window.dispatchEvent(new Event("focus"));
    });

    expect(useNewsQueryMock).toHaveBeenLastCalledWith(1);
  });
});
