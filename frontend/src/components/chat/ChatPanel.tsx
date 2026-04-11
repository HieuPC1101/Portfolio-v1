import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { ArrowRight, Bot, Check, Copy, MessageCircle, Minus, Send, Sparkles, Trash2, X } from "lucide-react";
import { cn } from "@/lib/utils";
import {
  deleteChatConversation,
  getChatConversation,
  listChatConversations,
  sendChatMessage,
  type ConversationDto,
} from "@/repositories/chatRepository";

interface Message {
  id: string;
  role: "user" | "assistant";
  content: string;
  timestamp: Date;
}

interface ChatPanelProps {
  onMinimize: () => void;
  onClose: () => void;
}

const initialQuickReplies = [
  "Phân tích kỹ thuật VNINDEX",
  "Đánh giá cổ phiếu mã VLC",
  "Phân tích hợp đồng VN30F1M",
];

const followUpReplies = [
  "Top cổ phiếu tăng mạnh hôm nay",
  "Danh mục cho người mới bắt đầu",
  "Tin tức thị trường đáng chú ý",
  "Cảnh báo rủi ro ngắn hạn",
];

const initialMessage: Message = {
  id: "assistant-welcome",
  role: "assistant",
  content:
    "Chào bạn, Ensa đã sẵn sàng! Tôi có thể hỗ trợ phân tích kỹ thuật, tổng hợp tin tức và gợi ý hướng quản lý danh mục theo mục tiêu của bạn.",
  timestamp: new Date(),
};

const createMessageId = () => `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;

function normalizeQuery(value: string) {
  return value
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/đ/g, "d");
}

function formatTimestamp(timestamp: Date) {
  return new Intl.DateTimeFormat("vi-VN", {
    hour: "2-digit",
    minute: "2-digit",
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
  }).format(timestamp);
}

function buildQuickReplies(input: string) {
  const normalizedInput = normalizeQuery(input);

  if (normalizedInput.includes("vnindex")) {
    return [
      "Phân tích hỗ trợ và kháng cự VNINDEX",
      "Nhóm ngành đang dẫn dắt",
      "Dự báo xu hướng 3 phiên tới",
    ];
  }

  if (normalizedInput.includes("danh muc")) {
    return [
      "Tỷ trọng ngắn hạn và trung hạn",
      "Danh mục cho vốn 100 triệu",
      "Mức cắt lỗ tham khảo",
    ];
  }

  return followUpReplies;
}

function parseServerTimestamp(rawTimestamp: string) {
  const parsed = new Date(rawTimestamp);
  if (Number.isNaN(parsed.getTime())) {
    return new Date();
  }
  return parsed;
}

function formatRelativeTime(timestamp: string) {
  const date = parseServerTimestamp(timestamp);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffMins = Math.floor(diffMs / 60000);
  const diffHours = Math.floor(diffMs / 3600000);
  const diffDays = Math.floor(diffMs / 86400000);

  if (diffMins < 1) return "Vừa xong";
  if (diffMins < 60) return `${diffMins}p trước`;
  if (diffHours < 24) return `${diffHours}h trước`;
  if (diffDays < 7) return `${diffDays} ngày trước`;

  return date.toLocaleDateString("vi-VN", { day: "2-digit", month: "2-digit" });
}

function mapConversationToMessages(conversation: ConversationDto): Message[] {
  if (!Array.isArray(conversation.messages) || conversation.messages.length === 0) {
    return [initialMessage];
  }

  return conversation.messages.map((message) => ({
    id: message.id,
    role: message.role,
    content: message.content,
    timestamp: parseServerTimestamp(message.timestamp),
  }));
}

function getSessionPreview(conversation: ConversationDto): string {
  const latestUserMessage = conversation.messages
    .filter((message) => message.role === "user")
    .slice(-1)[0];

  return latestUserMessage?.content ?? "Hoi thoai moi";
}

function renderInlineRichText(text: string): ReactNode[] {
  const parts = text.split(/(\*\*[^*]+\*\*)/g).filter(Boolean);

  return parts.map((part, index) => {
    if (part.startsWith("**") && part.endsWith("**")) {
      return (
        <strong key={`${part}-${index}`} className="font-semibold text-zinc-100">
          {part.slice(2, -2)}
        </strong>
      );
    }

    return <span key={`${part}-${index}`}>{part}</span>;
  });
}

function renderAssistantMessageContent(content: string) {
  const lines = content.replace(/\r\n/g, "\n").split("\n");

  return (
    <div className="space-y-1.5 text-[14px] leading-6">
      {lines.map((rawLine, index) => {
        const trimmed = rawLine.trim();

        if (!trimmed) {
          return <div key={`empty-${index}`} className="h-1" />;
        }

        const numberedMatch = trimmed.match(/^(\d+)\.\s*(.*)$/);
        if (numberedMatch) {
          return (
            <div key={`numbered-${index}`} className="flex items-start gap-2">
              <span className="mt-0.5 text-zinc-400">{numberedMatch[1]}.</span>
              <p className="text-zinc-100">{renderInlineRichText(numberedMatch[2])}</p>
            </div>
          );
        }

        const bulletMatch = trimmed.match(/^[-*]\s+(.*)$/);
        if (bulletMatch) {
          return (
            <div key={`bullet-${index}`} className="flex items-start gap-2">
              <span className="mt-0.5 text-zinc-400">•</span>
              <p className="text-zinc-100">{renderInlineRichText(bulletMatch[1])}</p>
            </div>
          );
        }

        const headingMatch = trimmed.match(/^\*\*(.+)\*\*$/);
        if (headingMatch) {
          return (
            <p key={`heading-${index}`} className="font-semibold text-zinc-50">
              {renderInlineRichText(headingMatch[1])}
            </p>
          );
        }

        return (
          <p key={`paragraph-${index}`} className="text-zinc-100">
            {renderInlineRichText(trimmed)}
          </p>
        );
      })}
    </div>
  );
}

export function ChatPanel({ onMinimize, onClose }: ChatPanelProps) {
  const [messages, setMessages] = useState<Message[]>([initialMessage]);
  const [input, setInput] = useState("");
  const [isTyping, setIsTyping] = useState(false);
  const [copiedMessageId, setCopiedMessageId] = useState<string | null>(null);
  const [quickReplies, setQuickReplies] = useState<string[]>(initialQuickReplies);
  const [conversationId, setConversationId] = useState<string | undefined>(undefined);
  const [sessions, setSessions] = useState<ConversationDto[]>([]);
  const [isSessionPopupOpen, setIsSessionPopupOpen] = useState(false);
  const [isThinkingVisible, setIsThinkingVisible] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const sessionPopupRef = useRef<HTMLDivElement>(null);
  const sessionToggleButtonRef = useRef<HTMLButtonElement>(null);

  const sendDisabled = useMemo(() => !input.trim() || isTyping, [input, isTyping]);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [messages, isTyping, quickReplies, isThinkingVisible]);

  useEffect(() => {
    let cancelled = false;

    listChatConversations()
      .then((conversations) => {
        if (cancelled) return;
        setSessions(conversations);
      })
      .catch(() => {
        if (cancelled) return;
        setSessions([]);
      });

    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!isSessionPopupOpen) {
      return;
    }

    const handleClickOutside = (event: MouseEvent) => {
      const targetNode = event.target as Node;
      const isInsidePopup = sessionPopupRef.current?.contains(targetNode);
      const isToggleButton = sessionToggleButtonRef.current?.contains(targetNode);
      if (isInsidePopup || isToggleButton) {
        return;
      }
      setIsSessionPopupOpen(false);
    };

    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setIsSessionPopupOpen(false);
      }
    };

    document.addEventListener("mousedown", handleClickOutside);
    document.addEventListener("keydown", handleEscape);

    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
      document.removeEventListener("keydown", handleEscape);
    };
  }, [isSessionPopupOpen]);

  const handleNewChat = () => {
    setConversationId(undefined);
    setMessages([initialMessage]);
    setInput("");
    setQuickReplies(initialQuickReplies);
    setIsThinkingVisible(false);
    setIsSessionPopupOpen(false);
  };

  const handleSelectSession = async (sessionId: string) => {
    const conversation = await getChatConversation(sessionId);
    setConversationId(conversation.id);
    setMessages(mapConversationToMessages(conversation));
    setQuickReplies([]);
    setIsThinkingVisible(false);
    setIsSessionPopupOpen(false);
  };

  const handleDeleteSession = async (sessionId: string) => {
    await deleteChatConversation(sessionId);
    setSessions((prev) => prev.filter((session) => session.id !== sessionId));
    if (conversationId === sessionId) {
      handleNewChat();
    }
  };

  const sendMessage = async (rawText: string) => {
    const content = rawText.trim();
    if (!content || isTyping) return;

    const userMessage: Message = {
      id: createMessageId(),
      role: "user",
      content,
      timestamp: new Date(),
    };

    setMessages((prev) => [...prev, userMessage]);
    setInput("");
    setIsTyping(true);
    setIsThinkingVisible(true);
    setQuickReplies([]);

    try {
      const response = await sendChatMessage({
        message: content,
        conversationId,
        includePortfolioContext: false,
      });

      const assistantMessage: Message = {
        id: createMessageId(),
        role: "assistant",
        content: response.message,
        timestamp: parseServerTimestamp(response.timestamp),
      };

      setMessages((prev) => [...prev, assistantMessage]);
      setConversationId(response.conversationId);
      setQuickReplies(
        response.suggestedActions.length > 0
          ? response.suggestedActions
          : buildQuickReplies(content),
      );
      setIsThinkingVisible(false);
      setSessions((prev) => {
        const existing = prev.find((session) => session.id === response.conversationId);
        if (existing) {
          return prev.map((session) =>
            session.id === response.conversationId
              ? {
                  ...session,
                  updated_at: response.timestamp,
                }
              : session,
          );
        }

        return [
          {
            id: response.conversationId,
            user_id: 0,
            created_at: response.timestamp,
            updated_at: response.timestamp,
            messages: [
              {
                id: userMessage.id,
                role: "user",
                content,
                timestamp: userMessage.timestamp.toISOString(),
              },
              {
                id: assistantMessage.id,
                role: "assistant",
                content: assistantMessage.content,
                timestamp: assistantMessage.timestamp.toISOString(),
              },
            ],
          },
          ...prev,
        ];
      });
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Đã có lỗi xảy ra khi gửi tin nhắn.";

      const assistantMessage: Message = {
        id: createMessageId(),
        role: "assistant",
        content: `Xin lỗi, hiện chưa thể xử lý yêu cầu. ${errorMessage}`,
        timestamp: new Date(),
      };

      setMessages((prev) => [...prev, assistantMessage]);
      setIsThinkingVisible(false);
      setQuickReplies(buildQuickReplies(content));
      return;
    } finally {
      setIsTyping(false);
    }
  };

  const handleCopyMessage = async (message: Message) => {
    try {
      await navigator.clipboard.writeText(message.content);
      setCopiedMessageId(message.id);
      window.setTimeout(() => setCopiedMessageId(null), 1500);
    } catch {
      setCopiedMessageId(null);
    }
  };

  const useSuggestion = () => {
    const randomSuggestion = quickReplies[Math.floor(Math.random() * quickReplies.length)] ?? initialQuickReplies[0];
    setInput(randomSuggestion);
  };

  return (
    <div className="fixed inset-0 z-50 h-screen w-screen overflow-hidden bg-black sm:inset-auto sm:bottom-5 sm:right-5 sm:h-[min(760px,88vh)] sm:w-[min(560px,calc(100vw-2.5rem))] sm:rounded-2xl sm:border sm:border-zinc-800 sm:bg-black/95 sm:shadow-[0_8px_32px_rgba(0,0,0,0.45)] animate-in fade-in zoom-in-95 slide-in-from-bottom-6 duration-300">
      <div className="relative flex h-full min-w-0 overflow-hidden">
        <div className="flex min-w-0 flex-1 flex-col overflow-hidden">
          <div className="flex h-12 items-center justify-between border-b border-zinc-800 bg-black px-4">
            <div className="flex items-center gap-3">
              <div className="flex h-8 w-8 items-center justify-center rounded-full bg-zinc-800 text-zinc-100">
                <Bot className="h-4 w-4" />
              </div>
              <span className="text-sm font-semibold tracking-tight text-zinc-100">Ensa</span>
            </div>
            <div className="flex items-center gap-1">
              <button
                type="button"
                ref={sessionToggleButtonRef}
                onClick={() => setIsSessionPopupOpen((prev) => !prev)}
                aria-label={isSessionPopupOpen ? "Đóng lịch sử chat" : "Mở lịch sử chat"}
                className="hidden h-8 rounded-md px-2 text-xs text-zinc-300 transition-colors hover:bg-zinc-800 hover:text-zinc-100 sm:inline-flex sm:items-center"
              >
                Lịch sử
              </button>
              <button
                type="button"
                onClick={onMinimize}
                aria-label="Thu nhỏ chatbox"
                className="flex h-8 w-8 items-center justify-center rounded-md text-zinc-300 transition-colors hover:bg-zinc-800 hover:text-zinc-100"
              >
                <Minus className="h-4 w-4" />
              </button>
              <button
                type="button"
                onClick={onClose}
                aria-label="Đóng chatbox"
                className="flex h-8 w-8 items-center justify-center rounded-md text-zinc-300 transition-colors hover:bg-zinc-800 hover:text-zinc-100"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
          </div>

          <div className="flex-1 overflow-y-auto bg-zinc-950 py-4">
            <div className="space-y-3 px-4">
              {messages.map((message) => (
                <div
                  key={message.id}
                  className={cn("flex", message.role === "user" ? "justify-end" : "justify-start")}
                >
                  <div
                    className={cn(
                      "max-w-[85%] rounded-xl px-4 py-3 text-sm leading-6 animate-in fade-in slide-in-from-bottom-2 duration-200",
                      message.role === "assistant"
                        ? "bg-zinc-800 text-zinc-100"
                        : "bg-[#e74c3c] text-white",
                    )}
                  >
                    {message.role === "assistant" ? (
                      renderAssistantMessageContent(message.content)
                    ) : (
                      <p className="whitespace-pre-wrap break-words">{message.content}</p>
                    )}
                    <div className="mt-2 flex items-center justify-between gap-3 text-[11px]">
                      <span className="text-zinc-400">{formatTimestamp(message.timestamp)}</span>
                      {message.role === "assistant" && (
                        <button
                          type="button"
                          onClick={() => handleCopyMessage(message)}
                          className="inline-flex h-5 items-center text-zinc-400 transition-colors hover:text-zinc-100"
                          aria-label="Sao chép nội dung tin nhắn"
                        >
                          {copiedMessageId === message.id ? (
                            <Check className="h-3.5 w-3.5" />
                          ) : (
                            <Copy className="h-3.5 w-3.5" />
                          )}
                        </button>
                      )}
                    </div>
                  </div>
                </div>
              ))}

              {isThinkingVisible && (
                <div className="flex justify-start">
                  <div className="max-w-[85%] rounded-xl border border-zinc-700 bg-zinc-900 px-4 py-3 text-sm text-zinc-200">
                    <p className="font-medium">Ensa đang phân tích yêu cầu để trả lời...</p>
                  </div>
                </div>
              )}

              {isTyping && (
                <div className="flex justify-start">
                  <div className="w-fit rounded-xl bg-zinc-800 px-4 py-3">
                    <div className="flex items-center gap-1.5">
                      <span className="h-2 w-2 animate-bounce rounded-full bg-zinc-500 [animation-delay:0ms]" />
                      <span className="h-2 w-2 animate-bounce rounded-full bg-zinc-500 [animation-delay:150ms]" />
                      <span className="h-2 w-2 animate-bounce rounded-full bg-zinc-500 [animation-delay:300ms]" />
                    </div>
                  </div>
                </div>
              )}

              {quickReplies.length > 0 && !isTyping && (
                <div className="space-y-2 pt-1">
                  {quickReplies.map((reply) => (
                    <button
                      key={reply}
                      type="button"
                      onClick={() => sendMessage(reply)}
                      className="flex w-full items-center justify-between rounded-lg border border-zinc-700 bg-zinc-900 px-4 py-3 text-left text-sm font-medium text-zinc-100 transition-all hover:translate-x-0.5 hover:border-zinc-500 hover:bg-zinc-800"
                    >
                      <span>{reply}</span>
                      <ArrowRight className="h-4 w-4 text-zinc-400" />
                    </button>
                  ))}
                </div>
              )}
            </div>
            <div ref={messagesEndRef} />
          </div>

          <div className="flex h-[60px] items-center gap-2 border-t border-zinc-800 bg-black px-4">
            <button
              type="button"
              onClick={useSuggestion}
              aria-label="Gợi ý câu hỏi"
              className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full text-amber-300 transition-transform hover:scale-105"
            >
              <Sparkles className="h-4 w-4" />
            </button>

            <input
              value={input}
              onChange={(event) => setInput(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === "Enter") {
                  void sendMessage(input);
                }
              }}
              placeholder="Bạn muốn hỏi điều gì?"
              className="h-9 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-4 text-sm text-zinc-100 outline-none transition-colors placeholder:text-zinc-500 focus:border-zinc-500"
            />

            <button
              type="button"
              onClick={() => {
                void sendMessage(input);
              }}
              disabled={sendDisabled}
              aria-label="Gửi tin nhắn"
              className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-[#e74c3c] text-white transition-all hover:scale-105 hover:bg-[#d43d2f] disabled:cursor-not-allowed disabled:bg-zinc-700 disabled:hover:scale-100"
            >
              <Send className="h-4 w-4" />
            </button>
          </div>
        </div>

        {isSessionPopupOpen && (
          <div
            ref={sessionPopupRef}
            className="absolute right-3 top-14 z-20 hidden w-[280px] overflow-hidden rounded-xl border border-zinc-800/80 bg-zinc-950/95 shadow-[0_12px_40px_rgba(0,0,0,0.5)] backdrop-blur-md sm:block"
          >
            <div className="flex items-center justify-between border-b border-zinc-800/80 px-3 py-2">
              <span className="text-xs font-medium text-zinc-400">Lịch sử</span>
              <button
                type="button"
                onClick={handleNewChat}
                className="h-7 rounded-md bg-[#e74c3c]/90 px-2.5 text-xs font-medium text-white transition-colors hover:bg-[#d43d2f]"
              >
                + Mới
              </button>
            </div>
            <div className="max-h-[320px] space-y-1 overflow-y-auto p-1.5">
              {sessions.length === 0 && (
                <div className="py-6 text-center">
                  <MessageCircle className="mx-auto mb-2 h-6 w-6 text-zinc-700" />
                  <p className="text-xs text-zinc-500">Chưa có hội thoại nào</p>
                </div>
              )}
              {sessions.map((session) => {
                const active = session.id === conversationId;
                const preview = getSessionPreview(session);
                return (
                  <div
                    key={session.id}
                    className={cn(
                      "group flex items-start gap-2 rounded-md border border-zinc-800/60 px-2.5 py-2 transition-colors",
                      active
                        ? "border-zinc-600 bg-zinc-800 text-zinc-100"
                        : "border-zinc-800 bg-zinc-900 text-zinc-300 hover:border-zinc-700 hover:bg-zinc-800",
                    )}
                  >
                    <button
                      type="button"
                      onClick={() => {
                        void handleSelectSession(session.id);
                      }}
                      className="min-w-0 flex-1 cursor-pointer text-left"
                    >
                      <p className="truncate text-xs font-medium text-zinc-200">{preview}</p>
                      <span className="mt-0.5 block text-[10px] text-zinc-500">
                        {formatRelativeTime(session.updated_at)}
                      </span>
                    </button>
                    <button
                      type="button"
                      onClick={() => {
                        void handleDeleteSession(session.id);
                      }}
                      aria-label={`Xóa hội thoại ${session.id}`}
                      className="invisible inline-flex h-5 w-5 shrink-0 items-center justify-center rounded text-zinc-600 transition-colors hover:bg-zinc-700 hover:text-red-400 group-hover:visible group-focus-within:visible"
                    >
                      <Trash2 className="h-3 w-3" />
                    </button>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
