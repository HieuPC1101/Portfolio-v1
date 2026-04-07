"""Chatbot service built on Google Gemini."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

import google.generativeai as genai

from app.chatbot.market_data_adapter import get_market_data_adapter
from app.config import settings


class PortfolioChatbot:
    """Chatbot service for portfolio advisory use-cases."""

    def __init__(self, user_id: Optional[int] = None, api_key: Optional[str] = None):
        self.user_id = user_id
        self.api_key = api_key or settings.gemini_api_key
        self.market_data = get_market_data_adapter()
        self.conversation_history: Dict[str, List[Dict[str, Any]]] = {}

        self.safety_settings = [
            {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
        ]

        self.model = None
        if self.api_key:
            genai.configure(api_key=self.api_key)
            self.model = genai.GenerativeModel(
                "gemini-flash-latest",
                safety_settings=self.safety_settings,
            )

    def get_system_prompt(self, portfolio_data: Optional[dict] = None) -> str:
        """Build system prompt with optional user context."""
        base_prompt = (
            "Bạn là một trợ lý AI tư vấn đầu tư chứng khoán Việt Nam. "
            "Trả lời ngắn gọn, thực tế, có cảnh báo rủi ro khi cần."
        )
        if portfolio_data:
            base_prompt += f"\n\nNgữ cảnh người dùng: {portfolio_data}"
        return base_prompt

    def _history_for(self, conversation_id: str) -> List[Dict[str, Any]]:
        return self.conversation_history.setdefault(conversation_id, [])

    def add_message_to_history(
        self, role: str, content: str, conversation_id: str
    ) -> None:
        """Append message to a specific conversation history."""
        history = self._history_for(conversation_id)
        history.append(
            {
                "role": role,
                "content": content,
                "timestamp": datetime.utcnow(),
            }
        )
        if len(history) > 10:
            self.conversation_history[conversation_id] = history[-10:]

    def generate_response(
        self,
        user_message: str,
        portfolio_context: Optional[dict] = None,
        conversation_id: str = "default",
    ) -> str:
        """Generate assistant response text."""
        if self.model is None:
            return (
                "Gemini API key chưa được cấu hình. Vui lòng kiểm tra GEMINI_API_KEY."
            )

        try:
            self.add_message_to_history("user", user_message, conversation_id)
            history = self._history_for(conversation_id)

            market_context = self.market_data.get_context_from_query(user_message)
            full_prompt = self.get_system_prompt(portfolio_context) + "\n\n"

            if market_context:
                full_prompt += f"Dữ liệu thị trường liên quan:\n{market_context}\n\n"

            for message in history[-6:]:
                role_label = "Người dùng" if message["role"] == "user" else "Trợ lý"
                full_prompt += f"{role_label}: {message['content']}\n"

            response = self.model.generate_content(
                full_prompt,
                generation_config={
                    "temperature": 0.7,
                    "top_p": 0.95,
                    "top_k": 40,
                    "max_output_tokens": 2048,
                },
            )

            assistant_message = getattr(response, "text", None)
            if not assistant_message:
                return "Xin lỗi, tôi không nhận được phản hồi từ AI."

            self.add_message_to_history("assistant", assistant_message, conversation_id)
            return assistant_message
        except Exception as exc:
            return f"Xin lỗi, đã có lỗi xảy ra: {exc}"

    def chat(
        self,
        message: str,
        conversation_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """High-level chat method used by API layer."""
        resolved_conversation_id = conversation_id or str(uuid4())
        response_text = self.generate_response(
            user_message=message,
            portfolio_context=context,
            conversation_id=resolved_conversation_id,
        )

        return {
            "response": response_text,
            "conversation_id": resolved_conversation_id,
            "sources": [],
            "suggested_actions": [],
            "timestamp": datetime.utcnow(),
        }

    def clear_history(self, conversation_id: Optional[str] = None) -> None:
        """Clear all history or one conversation history."""
        if conversation_id:
            self.conversation_history.pop(conversation_id, None)
            return
        self.conversation_history = {}

    def get_portfolio_context(
        self,
        selected_stocks: Optional[List[str]] = None,
        optimization_result: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Create compact text context from portfolio inputs."""
        context_parts: List[str] = []

        if selected_stocks:
            context_parts.append(
                f"Các cổ phiếu đang xem xét: {', '.join(selected_stocks)}"
            )

        if optimization_result:
            if "Trọng số danh mục" in optimization_result:
                weights_str = ", ".join(
                    [
                        f"{k}: {v:.2%}"
                        for k, v in optimization_result["Trọng số danh mục"].items()
                    ]
                )
                context_parts.append(f"Trọng số phân bổ: {weights_str}")

            if "Lợi nhuận kỳ vọng" in optimization_result:
                context_parts.append(
                    f"Lợi nhuận kỳ vọng: {optimization_result['Lợi nhuận kỳ vọng']:.2%}"
                )

            if "Rủi ro (Độ lệch chuẩn)" in optimization_result:
                context_parts.append(
                    f"Rủi ro: {optimization_result['Rủi ro (Độ lệch chuẩn)']:.2%}"
                )

            if "Tỷ lệ Sharpe" in optimization_result:
                context_parts.append(
                    f"Sharpe Ratio: {optimization_result['Tỷ lệ Sharpe']:.4f}"
                )

        return "\n".join(context_parts) if context_parts else None


GeminiChatbot = PortfolioChatbot


def create_quick_question_buttons() -> List[str]:
    """Return pre-defined quick prompts."""
    return [
        "Phân tích mã VCB",
        "Chỉ số thị trường hôm nay",
        "Giải thích mô hình Markowitz",
        "So sánh VNM và MSN",
        "Làm sao để giảm rủi ro?",
        "Tỷ lệ Sharpe là gì?",
    ]
