import { useEffect, useMemo, useRef, useState } from "react";
import { ArrowRight, Bot, Check, Copy, Minus, Send, Sparkles, X } from "lucide-react";
import { cn } from "@/lib/utils";

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

function buildAssistantReply(input: string) {
  const normalizedInput = normalizeQuery(input);

  if (normalizedInput.includes("vnindex") || normalizedInput.includes("thi truong")) {
    return "VNINDEX đang duy trì trạng thái giằng co quanh vùng cân bằng ngắn hạn. Bạn nên ưu tiên quan sát thanh khoản và nhóm dẫn dắt trước khi tăng tỷ trọng.";
  }

  if (normalizedInput.includes("danh muc") || normalizedInput.includes("toi uu")) {
    return "Với mục tiêu ổn định, bạn có thể chia danh mục theo tỷ lệ 50% cổ phiếu nền tăng trưởng, 30% cổ phiếu phòng thủ và 20% tiền mặt để linh hoạt xử lý biến động.";
  }

  if (normalizedInput.includes("tin") || normalizedInput.includes("news")) {
    return "Nhóm thông tin cần theo dõi trong phiên gồm: dòng tiền khối ngoại, kết quả kinh doanh quý gần nhất và biến động lãi suất. Tôi có thể tóm tắt theo từng nhóm ngành nếu bạn muốn.";
  }

  return "Tôi đã nhận được yêu cầu. Bạn có thể cho tôi thêm mã cổ phiếu, khung thời gian và mục tiêu đầu tư để phân tích chính xác hơn.";
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

export function ChatPanel({ onMinimize, onClose }: ChatPanelProps) {
  const [messages, setMessages] = useState<Message[]>([initialMessage]);
  const [input, setInput] = useState("");
  const [isTyping, setIsTyping] = useState(false);
  const [copiedMessageId, setCopiedMessageId] = useState<string | null>(null);
  const [quickReplies, setQuickReplies] = useState<string[]>(initialQuickReplies);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const typingTimerRef = useRef<number | null>(null);

  const sendDisabled = useMemo(() => !input.trim() || isTyping, [input, isTyping]);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [messages, isTyping, quickReplies]);

  useEffect(() => {
    return () => {
      if (typingTimerRef.current) {
        window.clearTimeout(typingTimerRef.current);
      }
    };
  }, []);

  const sendMessage = (rawText: string) => {
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
    setQuickReplies([]);

    if (typingTimerRef.current) {
      window.clearTimeout(typingTimerRef.current);
    }

    typingTimerRef.current = window.setTimeout(() => {
      const assistantMessage: Message = {
        id: createMessageId(),
        role: "assistant",
        content: buildAssistantReply(content),
        timestamp: new Date(),
      };

      setMessages((prev) => [...prev, assistantMessage]);
      setQuickReplies(buildQuickReplies(content));
      setIsTyping(false);
      typingTimerRef.current = null;
    }, 900);
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
    <div className="fixed inset-0 z-50 h-screen w-screen overflow-hidden bg-black sm:inset-auto sm:bottom-5 sm:right-5 sm:h-[min(600px,80vh)] sm:w-[400px] sm:rounded-2xl sm:border sm:border-zinc-800 sm:bg-black/95 sm:shadow-[0_8px_32px_rgba(0,0,0,0.45)] animate-in fade-in zoom-in-95 slide-in-from-bottom-6 duration-300">
      <div className="flex h-full flex-col overflow-hidden">
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
                      : "bg-[#e74c3c] text-white"
                  )}
                >
                  <p className="whitespace-pre-wrap break-words">{message.content}</p>
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
                sendMessage(input);
              }
            }}
            placeholder="Bạn muốn hỏi điều gì?"
            className="h-9 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-4 text-sm text-zinc-100 outline-none transition-colors placeholder:text-zinc-500 focus:border-zinc-500"
          />

          <button
            type="button"
            onClick={() => sendMessage(input)}
            disabled={sendDisabled}
            aria-label="Gửi tin nhắn"
            className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-[#e74c3c] text-white transition-all hover:scale-105 hover:bg-[#d43d2f] disabled:cursor-not-allowed disabled:bg-zinc-700 disabled:hover:scale-100"
          >
            <Send className="h-4 w-4" />
          </button>
        </div>
      </div>
    </div>
  );
}
