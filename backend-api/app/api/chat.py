from typing import Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.dependencies import get_current_user
from app.models.user import User
from app.schemas.chat import (
    ChatMessageRequest,
    ChatMessageResponse,
    ConversationResponse,
)
from app.chatbot.gemini_chatbot import GeminiChatbot
from app.chatbot.market_data_adapter import MarketDataAdapter

router = APIRouter(prefix="/chat", tags=["Chatbot"])

# Initialize chatbot (will be created per request with user context)
market_adapter = MarketDataAdapter()


@router.post("/message", response_model=ChatMessageResponse)
def send_message(
    request: ChatMessageRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Send a message to the AI chatbot.

    The chatbot can answer questions about:
    - Market data and stock information
    - Portfolio optimization strategies
    - Investment advice and analysis
    - Vietnamese stock market insights

    Parameters:
    - **message**: User's message/question
    - **conversation_id**: Optional conversation ID to continue a conversation
    - **context**: Optional additional context (portfolio data, etc.)
    """
    try:
        # Initialize chatbot for this user
        chatbot = GeminiChatbot(user_id=current_user.id)

        # Get user settings for personalization
        from app.models.user import UserSettings

        user_settings = (
            db.query(UserSettings)
            .filter(UserSettings.user_id == current_user.id)
            .first()
        )

        # Prepare context
        context = request.context or {}

        # Add user preferences to context if available
        if user_settings:
            context["user_preferences"] = {
                "risk_tolerance": user_settings.risk_tolerance,
                "investment_horizon": user_settings.investment_horizon,
                "preferred_sectors": user_settings.preferred_sectors,
            }

        # Add portfolio context if requested
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
                        "value": portfolio.current_value,
                        "stocks": [
                            {
                                "symbol": stock.symbol,
                                "quantity": stock.quantity,
                                "purchase_price": stock.purchase_price,
                            }
                            for stock in stocks
                        ],
                    }
                )

            context["portfolios"] = portfolio_data

        # Get response from chatbot
        response = chatbot.chat(
            message=request.message,
            conversation_id=request.conversation_id,
            context=context,
        )

        # Store conversation in user settings for history
        # (In production, you might want a separate ConversationHistory table)

        return {
            "message": response.get("response"),
            "conversation_id": response.get("conversation_id"),
            "sources": response.get("sources", []),
            "suggested_actions": response.get("suggested_actions", []),
            "timestamp": response.get("timestamp"),
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Chat failed: {str(e)}",
        )


@router.get("/conversations", response_model=List[ConversationResponse])
def list_conversations(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Get conversation history for the current user.

    Returns list of past conversations with the chatbot.
    """
    # Note: This would require a ConversationHistory table
    # For now, returning empty list as placeholder
    # TODO: Implement conversation history storage

    return []


@router.get("/conversations/{conversation_id}", response_model=ConversationResponse)
def get_conversation(
    conversation_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Get a specific conversation by ID.

    Returns the full conversation history with all messages.
    """
    # Note: This would require a ConversationHistory table
    # For now, raising 404 as placeholder
    # TODO: Implement conversation retrieval

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Conversation history not yet implemented",
    )


@router.delete(
    "/conversations/{conversation_id}", status_code=status.HTTP_204_NO_CONTENT
)
def delete_conversation(
    conversation_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> None:
    """
    Delete a conversation.

    Removes the conversation history from storage.
    """
    # Note: This would require a ConversationHistory table
    # TODO: Implement conversation deletion
    pass


@router.post("/feedback")
def submit_feedback(
    conversation_id: str,
    message_id: str,
    rating: int = Query(..., ge=1, le=5, description="Rating 1-5"),
    feedback: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> Any:
    """
    Submit feedback for a chatbot response.

    Helps improve the chatbot's responses over time.

    Parameters:
    - **conversation_id**: Conversation ID
    - **message_id**: Specific message ID to rate
    - **rating**: Rating from 1 (poor) to 5 (excellent)
    - **feedback**: Optional text feedback
    """
    # TODO: Store feedback in database for analysis
    # This would help improve chatbot responses over time

    return {
        "message": "Feedback received. Thank you!",
        "conversation_id": conversation_id,
        "message_id": message_id,
    }


@router.get("/suggestions")
def get_chat_suggestions(
    current_user: User = Depends(get_current_user), db: Session = Depends(get_db)
) -> Any:
    """
    Get suggested questions/prompts for the chatbot.

    Returns contextual suggestions based on user's portfolio and market conditions.
    """
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

    # Personalize suggestions based on user's portfolios
    from app.models.portfolio import Portfolio

    portfolios = (
        db.query(Portfolio).filter(Portfolio.user_id == current_user.id).first()
    )

    if portfolios:
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
