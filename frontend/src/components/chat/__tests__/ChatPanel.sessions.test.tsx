import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ChatPanel } from "@/components/chat/ChatPanel";

const repositoryMocks = vi.hoisted(() => ({
  listChatConversations: vi.fn(),
  getChatConversation: vi.fn(),
  deleteChatConversation: vi.fn(),
}));

vi.mock("@/repositories/chatRepository", () => ({
  listChatConversations: repositoryMocks.listChatConversations,
  getChatConversation: repositoryMocks.getChatConversation,
  deleteChatConversation: repositoryMocks.deleteChatConversation,
  sendChatMessage: vi.fn(),
}));

describe("ChatPanel sessions", () => {
  it("supports new chat, select session, and delete session", async () => {
    repositoryMocks.listChatConversations.mockResolvedValueOnce([
      {
        id: "conv-1",
        user_id: 1,
        created_at: "2026-04-10T08:00:00Z",
        updated_at: "2026-04-10T08:10:00Z",
        messages: [
          { id: "m1", role: "user", content: "Phan tich VNM", timestamp: "2026-04-10T08:00:00Z" },
          { id: "m2", role: "assistant", content: "Da phan tich", timestamp: "2026-04-10T08:00:08Z" },
        ],
      },
    ]);
    repositoryMocks.getChatConversation.mockResolvedValueOnce({
      id: "conv-1",
      user_id: 1,
      created_at: "2026-04-10T08:00:00Z",
      updated_at: "2026-04-10T08:10:00Z",
      messages: [
        { id: "m1", role: "user", content: "Phan tich VNM", timestamp: "2026-04-10T08:00:00Z" },
        { id: "m2", role: "assistant", content: "Da phan tich", timestamp: "2026-04-10T08:00:08Z" },
      ],
    });
    repositoryMocks.deleteChatConversation.mockResolvedValueOnce(undefined);

    render(<ChatPanel onMinimize={() => {}} onClose={() => {}} />);

    fireEvent.click(screen.getByRole("button", { name: "Mở lịch sử chat" }));
    await waitFor(() => expect(screen.getByText("Phan tich VNM")).toBeInTheDocument());

    fireEvent.click(screen.getByRole("button", { name: "Chat mới" }));
    expect(screen.getByText(/Ensa đã sẵn sàng/i)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Mở lịch sử chat" }));
    await waitFor(() => expect(screen.getByText("Phan tich VNM")).toBeInTheDocument());
    fireEvent.click(screen.getByText("Phan tich VNM"));
    await waitFor(() => {
      expect(repositoryMocks.getChatConversation).toHaveBeenCalledWith("conv-1");
      expect(screen.getByText("Da phan tich")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("button", { name: "Mở lịch sử chat" }));
    fireEvent.click(screen.getByRole("button", { name: /Xóa hội thoại conv-1/i }));
    await waitFor(() => {
      expect(repositoryMocks.deleteChatConversation).toHaveBeenCalledWith("conv-1");
      expect(screen.queryByText("Phan tich VNM")).not.toBeInTheDocument();
    });
  });
});
