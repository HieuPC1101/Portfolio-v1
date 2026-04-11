import { apiAuthDelete, apiAuthGet, apiAuthPost } from "@/lib/apiAuth";

interface SendChatMessageInput {
  message: string;
  conversationId?: string;
  includePortfolioContext?: boolean;
  context?: Record<string, unknown>;
}

interface BackendChatMessageResponse {
  message: string;
  conversation_id: string;
  sources?: Array<Record<string, unknown>>;
  suggested_actions?: string[];
  timestamp: string;
  processing_started_at?: string;
  processing_finished_at?: string;
  processing_duration_ms?: number;
  processing_steps?: string[];
}

export interface ConversationMessageDto {
  id: string;
  role: "user" | "assistant";
  content: string;
  timestamp: string;
}

export interface ConversationDto {
  id: string;
  user_id: number;
  messages: ConversationMessageDto[];
  created_at: string;
  updated_at: string;
}

export interface ChatMessageResult {
  message: string;
  conversationId: string;
  sources: Array<Record<string, unknown>>;
  suggestedActions: string[];
  timestamp: string;
  processingStartedAt: string;
  processingFinishedAt: string;
  processingDurationMs: number;
  processingSteps: string[];
}

export async function sendChatMessage(input: SendChatMessageInput): Promise<ChatMessageResult> {
  const payload = {
    message: input.message,
    conversation_id: input.conversationId,
    include_portfolio_context: Boolean(input.includePortfolioContext),
    context: input.context,
  };

  const response = await apiAuthPost<BackendChatMessageResponse>("/api/v1/chat/message", payload);

  return {
    message: response.message,
    conversationId: response.conversation_id,
    sources: response.sources ?? [],
    suggestedActions: response.suggested_actions ?? [],
    timestamp: response.timestamp,
    processingStartedAt: response.processing_started_at ?? response.timestamp,
    processingFinishedAt: response.processing_finished_at ?? response.timestamp,
    processingDurationMs: response.processing_duration_ms ?? 0,
    processingSteps: response.processing_steps ?? [],
  };
}

export async function listChatConversations(): Promise<ConversationDto[]> {
  return apiAuthGet<ConversationDto[]>("/api/v1/chat/conversations");
}

export async function getChatConversation(conversationId: string): Promise<ConversationDto> {
  return apiAuthGet<ConversationDto>(`/api/v1/chat/conversations/${conversationId}`);
}

export async function deleteChatConversation(conversationId: string): Promise<void> {
  await apiAuthDelete(`/api/v1/chat/conversations/${conversationId}`);
}
