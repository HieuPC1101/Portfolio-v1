import { describe, expect, it } from "vitest";
import { MemoryRouter } from "react-router-dom";
import { render, screen } from "@testing-library/react";
import { SidebarProvider } from "@/components/ui/sidebar";
import { AppSidebar } from "@/components/layout/AppSidebar";

describe("AppSidebar", () => {
  it("giữ padding gọn khi sidebar ở trạng thái thu gọn", () => {
    const { container } = render(
      <MemoryRouter>
        <SidebarProvider defaultOpen={false}>
          <AppSidebar />
        </SidebarProvider>
      </MemoryRouter>,
    );

    const header = container.querySelector('[data-sidebar="header"]');
    expect(header).not.toBeNull();
    expect(header).toHaveClass("group-data-[collapsible=icon]:p-2");
  });

  it("hiển thị nhãn Cổ phiếu trong menu", () => {
    render(
      <MemoryRouter>
        <SidebarProvider defaultOpen>
          <AppSidebar />
        </SidebarProvider>
      </MemoryRouter>,
    );

    expect(screen.getByText("Cổ phiếu")).toBeInTheDocument();
  });
});
