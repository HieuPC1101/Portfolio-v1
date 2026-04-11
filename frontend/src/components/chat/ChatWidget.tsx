import { useState } from "react";
import { Bot, MessageCircle, X } from "lucide-react";
import { ChatPanel } from "@/components/chat/ChatPanel";

type ChatWidgetState = "closed" | "minimized" | "open";

export function ChatWidget() {
  const [chatState, setChatState] = useState<ChatWidgetState>("closed");

  return (
    <>
      {chatState === "open" && (
        <ChatPanel
          onMinimize={() => setChatState("minimized")}
          onClose={() => setChatState("closed")}
        />
      )}

      {chatState === "minimized" && (
        <div className="fixed bottom-5 right-5 z-50 flex h-12 min-w-[220px] items-center justify-between rounded-full border border-zinc-700 bg-black/95 px-3 shadow-[0_8px_20px_rgba(0,0,0,0.35)] animate-in fade-in slide-in-from-bottom-4 duration-200">
          <button
            type="button"
            onClick={() => setChatState("open")}
            className="flex items-center gap-2 text-sm font-medium text-zinc-100"
          >
            <span className="flex h-7 w-7 items-center justify-center rounded-full bg-zinc-800 text-zinc-100">
              <Bot className="h-4 w-4" />
            </span>
            <span>Ensa</span>
          </button>
          <button
            type="button"
            aria-label="Đóng chatbox"
            onClick={() => setChatState("closed")}
            className="flex h-7 w-7 items-center justify-center rounded-full text-zinc-400 transition-colors hover:bg-zinc-800 hover:text-zinc-100"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
      )}

      {chatState === "closed" && (
        <button
          type="button"
          onClick={() => setChatState("open")}
          aria-label="Mở chatbox Ensa"
          className="fixed bottom-6 right-6 z-50 flex h-12 w-12 items-center justify-center rounded-full bg-[#e74c3c] text-white shadow-lg transition-transform hover:scale-105 hover:bg-[#d43d2f]"
        >
          <MessageCircle className="h-5 w-5" />
        </button>
      )}
    </>
  );
}
