import { fireEvent, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { NotificationBell } from "@/components/notifications/NotificationBell";
import { renderWithProviders } from "@/test/renderWithProviders";

vi.mock("@/config/runtime", () => ({
  ENABLE_MOCK_API: true,
  MOCK_API_DELAY_MS: 0,
}));

describe("NotificationBell", () => {
  it("mo panel thong bao khi bam vao icon chuong", async () => {
    renderWithProviders(<NotificationBell />);

    fireEvent.click(screen.getByRole("button", { name: "Mở trung tâm thông báo" }));

    expect(await screen.findByText("Thông báo")).toBeInTheDocument();
  });
});
