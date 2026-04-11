import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ChatPanel } from "@/components/chat/ChatPanel";

const repositoryMocks = vi.hoisted(() => ({
  sendChatMessage: vi.fn(),
  listChatConversations: vi.fn(),
  getChatConversation: vi.fn(),
  deleteChatConversation: vi.fn(),
}));

vi.mock("@/repositories/chatRepository", () => ({
  listChatConversations: repositoryMocks.listChatConversations,
  getChatConversation: repositoryMocks.getChatConversation,
  deleteChatConversation: repositoryMocks.deleteChatConversation,
  sendChatMessage: repositoryMocks.sendChatMessage,
}));

describe("ChatPanel thinking overlay", () => {
  it("shows thinking overlay and renders backend processing duration", async () => {
    repositoryMocks.listChatConversations.mockResolvedValueOnce([]);
    repositoryMocks.sendChatMessage.mockResolvedValueOnce({
      message: "Ket qua phan tich",
      conversationId: "conv-1",
      suggestedActions: [],
      sources: [],
      timestamp: "2026-04-10T08:00:08Z",
      processingStartedAt: "2026-04-10T08:00:00Z",
      processingFinishedAt: "2026-04-10T08:00:08Z",
      processingDurationMs: 8000,
      processingSteps: ["Dang phan tich yeu cau", "Dang tong hop ngu canh thi truong"],
    });

    render(<ChatPanel onMinimize={() => {}} onClose={() => {}} />);

    fireEvent.change(screen.getByPlaceholderText("Bạn muốn hỏi điều gì?"), {
      target: { value: "Phan tich VNINDEX" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Gửi tin nhắn" }));

    expect(screen.getByText(/đang phân tích/i)).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.getByText("Ket qua phan tich")).toBeInTheDocument();
      expect(screen.queryByText(/Xem quá trình phân tích/i)).not.toBeInTheDocument();
    });
  });

  it("does not render thinking trace card after response", async () => {
    repositoryMocks.listChatConversations.mockResolvedValueOnce([]);
    repositoryMocks.sendChatMessage.mockResolvedValueOnce({
      message: "fallback case",
      conversationId: "conv-fallback",
      suggestedActions: [],
      sources: [],
      timestamp: "2026-04-10T08:00:08Z",
      processingStartedAt: "2026-04-10T08:00:00Z",
      processingFinishedAt: "2026-04-10T08:00:08Z",
      processingDurationMs: Number.NaN,
      processingSteps: [],
    });

    render(<ChatPanel onMinimize={() => {}} onClose={() => {}} />);

    fireEvent.change(screen.getByPlaceholderText("Bạn muốn hỏi điều gì?"), {
      target: { value: "Fallback duration" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Gửi tin nhắn" }));

    await waitFor(() => {
      expect(screen.getByText("fallback case")).toBeInTheDocument();
      expect(screen.queryByText(/Xem quá trình phân tích/i)).not.toBeInTheDocument();
    });
  });
});
