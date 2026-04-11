import { afterEach, describe, expect, it, vi } from "vitest";

import {
  deleteChatConversation,
  getChatConversation,
  listChatConversations,
  sendChatMessage,
} from "@/repositories/chatRepository";

describe("chatRepository", () => {
  afterEach(() => {
    localStorage.clear();
    vi.unstubAllGlobals();
  });

  it("gui message den backend chat API voi token", async () => {
    localStorage.setItem("finstock_access_token", "test-token");

    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue({
          message: "Xin chao ban",
          conversation_id: "conv-123",
          sources: [],
          suggested_actions: [],
          timestamp: "2026-04-10T08:00:00Z",
          processing_started_at: "2026-04-10T07:59:50Z",
          processing_finished_at: "2026-04-10T08:00:00Z",
          processing_duration_ms: 10000,
          processing_steps: ["step-1"],
        }),
      }),
    );

    const result = await sendChatMessage({
      message: "Xin chao",
      conversationId: "conv-123",
      includePortfolioContext: true,
      context: { mode: "demo" },
    });

    expect(fetch).toHaveBeenCalledWith("/api/v1/chat/message", {
      method: "POST",
      headers: {
        Authorization: "Bearer test-token",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        message: "Xin chao",
        conversation_id: "conv-123",
        include_portfolio_context: true,
        context: { mode: "demo" },
      }),
    });

    expect(result).toEqual({
      message: "Xin chao ban",
      conversationId: "conv-123",
      sources: [],
      suggestedActions: [],
      timestamp: "2026-04-10T08:00:00Z",
      processingStartedAt: "2026-04-10T07:59:50Z",
      processingFinishedAt: "2026-04-10T08:00:00Z",
      processingDurationMs: 10000,
      processingSteps: ["step-1"],
    });
  });

  it("map processing metadata from /chat/message response", async () => {
    localStorage.setItem("finstock_access_token", "test-token");

    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue({
          message: "done",
          conversation_id: "conv-123",
          sources: [],
          suggested_actions: ["A"],
          timestamp: "2026-04-10T08:00:08Z",
          processing_started_at: "2026-04-10T08:00:00Z",
          processing_finished_at: "2026-04-10T08:00:08Z",
          processing_duration_ms: 8000,
          processing_steps: ["step-1", "step-2"],
        }),
      }),
    );

    const result = await sendChatMessage({ message: "Xin chao" });

    expect(result.processingDurationMs).toBe(8000);
    expect(result.processingSteps).toEqual(["step-1", "step-2"]);
  });

  it("fetches conversation list", async () => {
    localStorage.setItem("finstock_access_token", "test-token");

    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue([
          {
            id: "conv-1",
            user_id: 1,
            messages: [],
            created_at: "2026-04-10T08:00:00Z",
            updated_at: "2026-04-10T08:00:00Z",
          },
        ]),
      }),
    );

    const result = await listChatConversations();

    expect(result).toHaveLength(1);
    expect(result[0].id).toBe("conv-1");
    expect(fetch).toHaveBeenCalledWith("/api/v1/chat/conversations", {
      method: "GET",
      headers: {
        Authorization: "Bearer test-token",
      },
      body: undefined,
    });
  });

  it("fetches single conversation", async () => {
    localStorage.setItem("finstock_access_token", "test-token");

    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        json: vi.fn().mockResolvedValue({
          id: "conv-1",
          user_id: 1,
          messages: [],
          created_at: "2026-04-10T08:00:00Z",
          updated_at: "2026-04-10T08:00:00Z",
        }),
      }),
    );

    const result = await getChatConversation("conv-1");

    expect(result.id).toBe("conv-1");
    expect(fetch).toHaveBeenCalledWith("/api/v1/chat/conversations/conv-1", {
      method: "GET",
      headers: {
        Authorization: "Bearer test-token",
      },
      body: undefined,
    });
  });

  it("deletes conversation", async () => {
    localStorage.setItem("finstock_access_token", "test-token");

    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({ ok: true, status: 204, json: vi.fn() }),
    );

    await expect(deleteChatConversation("conv-1")).resolves.toBeUndefined();
    expect(fetch).toHaveBeenCalledWith("/api/v1/chat/conversations/conv-1", {
      method: "DELETE",
      headers: {
        Authorization: "Bearer test-token",
      },
      body: undefined,
    });
  });

  it("nem loi detail tu backend khi request that bai", async () => {
    localStorage.setItem("finstock_access_token", "test-token");

    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: false,
        status: 500,
        json: vi.fn().mockResolvedValue({ detail: "Chat failed" }),
      }),
    );

    await expect(sendChatMessage({ message: "Xin chao" })).rejects.toThrow("Chat failed");
  });
});
