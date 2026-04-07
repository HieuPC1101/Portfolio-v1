from datetime import datetime
from typing import Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.chatbot.chatbot_service import GeminiChatbot
from app.database import get_db
from app.dependencies import get_current_user
from app.models.chat import ChatConversation, ChatFeedback, ChatMessage
from app.models.user import User, UserSettings
from app.schemas.chat import (
    ChatMessageRequest,
    ChatMessageResponse,
    ConversationResponse,
)

router = APIRouter(prefix="/chat", tags=["Chatbot"])


@router.post("/message", response_model=ChatMessageResponse)
def send_message(
    request: ChatMessageRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """Send a message to chatbot and persist conversation history."""
    try:
        chatbot = GeminiChatbot(user_id=current_user.id)

        user_settings = (
            db.query(UserSettings)
            .filter(UserSettings.user_id == current_user.id)
            .first()
        )
        context = request.context or {}

        if user_settings:
            context["user_preferences"] = {
                "risk_tolerance": user_settings.risk_tolerance,
                "investment_horizon": user_settings.investment_horizon,
                "preferred_sectors": user_settings.preferred_sectors,
            }

        if request.include_portfolio_context:
            from app.models.portfolio import Portfolio, PortfolioStock

            portfolios = (
                db.query(Portfolio).filter(Portfolio.user_id == current_user.id).all()
            )
            portfolio_data = []
            for portfolio in portfolios:
                stocks = (
                    db.query(PortfolioStock)
                    .filter(PortfolioStock.portfolio_id == portfolio.id)
                    .all()
                )
                portfolio_data.append(
                    {
                        "name": portfolio.name,
                        "value": getattr(portfolio, "current_value", None),
                        "stocks": [
                            {
                                "symbol": stock.symbol,
                                "quantity": getattr(
                                    stock, "quantity", getattr(stock, "shares", None)
                                ),
                                "purchase_price": stock.purchase_price,
                            }
                            for stock in stocks
                        ],
                    }
                )
            context["portfolios"] = portfolio_data

        conversation = None
        if request.conversation_id:
            conversation = (
                db.query(ChatConversation)
                .filter(
                    ChatConversation.id == request.conversation_id,
                    ChatConversation.user_id == current_user.id,
                )
                .first()
            )

        if conversation is None:
            if request.conversation_id:
                conversation = ChatConversation(
                    id=request.conversation_id, user_id=current_user.id
                )
            else:
                conversation = ChatConversation(user_id=current_user.id)
            db.add(conversation)
            db.flush()

        response = chatbot.chat(
            message=request.message,
            conversation_id=conversation.id,
            context=context,
        )

        assistant_timestamp = response.get("timestamp")
        if not isinstance(assistant_timestamp, datetime):
            assistant_timestamp = datetime.utcnow()

        user_message = ChatMessage(
            conversation_id=conversation.id,
            role="user",
            content=request.message,
            timestamp=datetime.utcnow(),
        )
        assistant_message = ChatMessage(
            conversation_id=conversation.id,
            role="assistant",
            content=response.get("response", ""),
            sources=response.get("sources", []),
            suggested_actions=response.get("suggested_actions", []),
            timestamp=assistant_timestamp,
        )

        conversation.updated_at = datetime.utcnow()
        db.add(user_message)
        db.add(assistant_message)
        db.commit()

        return {
            "message": assistant_message.content,
            "conversation_id": conversation.id,
            "sources": assistant_message.sources or [],
            "suggested_actions": assistant_message.suggested_actions or [],
            "timestamp": assistant_message.timestamp,
        }
    except Exception as exc:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Chat failed: {exc}",
        )


@router.get("/conversations", response_model=List[ConversationResponse])
def list_conversations(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """Get conversation history for current user."""
    return (
        db.query(ChatConversation)
        .filter(ChatConversation.user_id == current_user.id)
        .order_by(ChatConversation.updated_at.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )


@router.get("/conversations/{conversation_id}", response_model=ConversationResponse)
def get_conversation(
    conversation_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """Get one conversation with all messages."""
    conversation = (
        db.query(ChatConversation)
        .filter(
            ChatConversation.id == conversation_id,
            ChatConversation.user_id == current_user.id,
        )
        .first()
    )
    if not conversation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Conversation not found"
        )
    return conversation


@router.delete(
    "/conversations/{conversation_id}", status_code=status.HTTP_204_NO_CONTENT
)
def delete_conversation(
    conversation_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> None:
    """Delete one conversation."""
    conversation = (
        db.query(ChatConversation)
        .filter(
            ChatConversation.id == conversation_id,
            ChatConversation.user_id == current_user.id,
        )
        .first()
    )
    if not conversation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Conversation not found"
        )
    db.delete(conversation)
    db.commit()


@router.post("/feedback")
def submit_feedback(
    conversation_id: str,
    message_id: str,
    rating: int = Query(..., ge=1, le=5, description="Rating 1-5"),
    feedback: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """Submit feedback for a chatbot response."""
    conversation = (
        db.query(ChatConversation)
        .filter(
            ChatConversation.id == conversation_id,
            ChatConversation.user_id == current_user.id,
        )
        .first()
    )
    if not conversation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Conversation not found"
        )

    message = (
        db.query(ChatMessage)
        .filter(
            ChatMessage.id == message_id, ChatMessage.conversation_id == conversation_id
        )
        .first()
    )
    if not message:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Message not found"
        )

    feedback_row = ChatFeedback(
        conversation_id=conversation_id,
        message_id=message_id,
        user_id=current_user.id,
        rating=rating,
        feedback_text=feedback,
    )
    db.add(feedback_row)
    db.commit()

    return {
        "message": "Feedback received. Thank you!",
        "conversation_id": conversation_id,
        "message_id": message_id,
    }


@router.get("/suggestions")
def get_chat_suggestions(
    current_user: User = Depends(get_current_user), db: Session = Depends(get_db)
) -> Any:
    """Get suggested prompts for the chatbot."""
    suggestions = [
        {
            "category": "Market Analysis",
            "prompts": [
                "What are the top performing stocks today?",
                "Give me an overview of the Vietnamese stock market",
                "Which sectors are performing well this week?",
                "What are the latest market news and trends?",
            ],
        },
        {
            "category": "Portfolio Optimization",
            "prompts": [
                "How can I optimize my portfolio for maximum returns?",
                "What is the optimal asset allocation for my risk tolerance?",
                "Should I rebalance my portfolio?",
                "Compare different optimization strategies for my stocks",
            ],
        },
        {
            "category": "Stock Analysis",
            "prompts": [
                "Analyze the fundamentals of VNM stock",
                "What are the key financial metrics for VCB?",
                "Compare HPG and HSG stocks",
                "Is FPT stock a good buy right now?",
            ],
        },
        {
            "category": "Investment Strategy",
            "prompts": [
                "What is a good investment strategy for beginners?",
                "Explain the difference between value and growth investing",
                "How do I diversify my portfolio effectively?",
                "What are the risks of investing in Vietnamese stocks?",
            ],
        },
    ]

    from app.models.portfolio import Portfolio

    portfolio = db.query(Portfolio).filter(Portfolio.user_id == current_user.id).first()
    if portfolio:
        suggestions.insert(
            0,
            {
                "category": "Your Portfolio",
                "prompts": [
                    "Analyze my current portfolio performance",
                    "What are the strengths and weaknesses of my portfolio?",
                    "How can I reduce risk in my portfolio?",
                    "Suggest stocks to add to my portfolio",
                ],
            },
        )

    return {"suggestions": suggestions}
