from typing import Any, Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field


class ChatMessageRequest(BaseModel):
    """Request model for sending a chat message"""

    message: str = Field(
        ..., min_length=1, max_length=5000, description="User's message"
    )
    conversation_id: Optional[str] = Field(
        None, description="Conversation ID to continue"
    )
    context: Optional[Dict[str, Any]] = Field(None, description="Additional context")
    include_portfolio_context: bool = Field(
        False, description="Include user's portfolio data in context"
    )


class ChatMessageResponse(BaseModel):
    """Response model for chat message"""

    message: str
    conversation_id: str
    sources: Optional[List[Dict[str, Any]]] = None
    suggested_actions: Optional[List[str]] = None
    timestamp: datetime
    processing_started_at: datetime
    processing_finished_at: datetime
    processing_duration_ms: int
    processing_steps: List[str] = Field(default_factory=list)

    class Config:
        from_attributes = True


class ConversationMessage(BaseModel):
    """Individual message in a conversation"""

    id: str
    role: str  # "user" or "assistant"
    content: str
    timestamp: datetime

    class Config:
        from_attributes = True


class ConversationResponse(BaseModel):
    """Response model for a conversation"""

    id: str
    user_id: int
    messages: List[ConversationMessage]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ChatFeedback(BaseModel):
    """Feedback for a chat message"""

    conversation_id: str
    message_id: str
    rating: int = Field(..., ge=1, le=5)
    feedback: Optional[str] = None

    class Config:
        from_attributes = True
