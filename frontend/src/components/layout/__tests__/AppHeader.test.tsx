import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";
import { SidebarProvider } from "@/components/ui/sidebar";
import { AppHeader } from "@/components/layout/AppHeader";

vi.mock("@/contexts/AuthContext", () => ({
  useAuth: () => ({
    user: {
      username: "tester",
      full_name: "Test User",
      email: "test@example.com",
    },
    logout: vi.fn(),
  }),
}));

vi.mock("@/components/notifications/NotificationBell", () => ({
  NotificationBell: () => <div data-testid="notification-bell" />,
}));

describe("AppHeader", () => {
  it("không render ô tìm mã cổ phiếu trên header", () => {
    render(
      <MemoryRouter>
        <SidebarProvider>
          <AppHeader />
        </SidebarProvider>
      </MemoryRouter>,
    );

    expect(screen.queryByPlaceholderText("Tìm mã cổ phiếu...")).not.toBeInTheDocument();
  });
});
