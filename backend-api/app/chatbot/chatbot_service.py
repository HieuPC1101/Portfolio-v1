"""Chatbot service built on OpenRouter chat completions API."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

import requests

from app.chatbot.market_data_adapter import get_market_data_adapter
from app.config import settings


class PortfolioChatbot:
    """Chatbot service for portfolio advisory use-cases."""

    def __init__(
        self,
        user_id: Optional[int] = None,
        api_key: Optional[str] = None,
        model_name: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        market_data_adapter: Optional[Any] = None,
    ):
        self.user_id = user_id
        self.api_key = api_key or settings.openrouter_api_key
        self.model_name = model_name or settings.openrouter_model
        self.base_url = (base_url or settings.openrouter_base_url).rstrip("/")
        self.timeout_seconds = timeout_seconds or settings.openrouter_timeout_seconds
        self.market_data = market_data_adapter or get_market_data_adapter()
        self.conversation_history: Dict[str, List[Dict[str, Any]]] = {}
        self.http_session = requests.Session()

    def get_system_prompt(self, portfolio_data: Optional[dict] = None) -> str:
        """Build system prompt with optional user context."""
        base_prompt = (
            "Bạn là một trợ lý AI tư vấn đầu tư chứng khoán Việt Nam. "
            "Trả lời rất ngắn gọn, thực tế, có cảnh báo rủi ro khi cần. "
            "Tối đa 6 dòng, ưu tiên gạch đầu dòng ngắn. "
            "Tuyệt đối không dùng bảng Markdown, không dùng ký tự '|' để trình bày dạng cột."
        )
        if portfolio_data:
            base_prompt += f"\n\nNgữ cảnh người dùng: {portfolio_data}"
        return base_prompt

    @staticmethod
    def _parse_pipe_row(line: str) -> List[str]:
        cleaned = line.strip().strip("|")
        return [cell.strip() for cell in cleaned.split("|")]

    @staticmethod
    def _is_table_separator(line: str) -> bool:
        stripped = line.strip().strip("|").replace(" ", "")
        return bool(stripped) and all(ch in "-:" for ch in stripped)

    def _rewrite_markdown_tables(self, text: str) -> str:
        lines = text.replace("\r\n", "\n").split("\n")
        rewritten: List[str] = []
        i = 0

        while i < len(lines):
            current = lines[i]
            next_line = lines[i + 1] if i + 1 < len(lines) else ""

            is_table_header = (
                "|" in current
                and "|" in next_line
                and self._is_table_separator(next_line)
            )
            if not is_table_header:
                rewritten.append(current)
                i += 1
                continue

            headers = [h for h in self._parse_pipe_row(current) if h]
            i += 2

            while i < len(lines) and "|" in lines[i]:
                row_cells = self._parse_pipe_row(lines[i])
                if not any(row_cells):
                    i += 1
                    continue

                pairs: List[str] = []
                for idx, cell in enumerate(row_cells):
                    value = cell.strip()
                    if not value:
                        continue
                    if idx < len(headers) and headers[idx]:
                        pairs.append(f"{headers[idx]}: {value}")
                    else:
                        pairs.append(value)

                if pairs:
                    rewritten.append(f"- {'; '.join(pairs)}")

                i += 1

        return "\n".join(rewritten)

    def _sanitize_assistant_response(self, text: str) -> str:
        compact = self._rewrite_markdown_tables(text)
        lines = [line.rstrip() for line in compact.split("\n")]

        kept: List[str] = []
        for line in lines:
            if not line.strip() and kept and not kept[-1].strip():
                continue
            kept.append(line)

        max_lines = 6
        trimmed = kept[:max_lines]
        return "\n".join(trimmed).strip()

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
        if not self.api_key:
            return "OpenRouter API key chưa được cấu hình. Vui lòng kiểm tra OPENROUTER_API_KEY."
        if not self.model_name:
            return "OpenRouter model chưa được cấu hình. Vui lòng kiểm tra OPENROUTER_MODEL."

        try:
            self.add_message_to_history("user", user_message, conversation_id)
            history = self._history_for(conversation_id)

            market_context = self.market_data.get_context_from_query(user_message)
            system_prompt = self.get_system_prompt(portfolio_context)

            if market_context:
                system_prompt += f"\n\nDữ liệu thị trường liên quan:\n{market_context}"

            messages: List[Dict[str, str]] = [
                {"role": "system", "content": system_prompt}
            ]

            for message in history[-6:]:
                role = "assistant" if message["role"] == "assistant" else "user"
                messages.append({"role": role, "content": message["content"]})

            payload = {
                "model": self.model_name,
                "messages": messages,
                "temperature": 0.7,
                "top_p": 0.95,
                "max_tokens": 2048,
            }

            response = self.http_session.post(
                f"{self.base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            data = response.json()

            choices = data.get("choices") or []
            assistant_message = None
            if choices:
                assistant_message = choices[0].get("message", {}).get("content")
                if isinstance(assistant_message, list):
                    assistant_message = "".join(
                        part.get("text", "")
                        for part in assistant_message
                        if isinstance(part, dict)
                    )

            if not assistant_message:
                return "Xin lỗi, tôi không nhận được phản hồi từ AI."

            assistant_message = self._sanitize_assistant_response(assistant_message)

            self.add_message_to_history("assistant", assistant_message, conversation_id)
            return assistant_message
        except requests.RequestException as exc:
            return f"Xin lỗi, không thể kết nối OpenRouter: {exc}"
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
        processing_started_at = datetime.utcnow()
        response_text = self.generate_response(
            user_message=message,
            portfolio_context=context,
            conversation_id=resolved_conversation_id,
        )
        processing_finished_at = datetime.utcnow()
        duration_ms = int(
            (processing_finished_at - processing_started_at).total_seconds() * 1000
        )

        return {
            "response": response_text,
            "conversation_id": resolved_conversation_id,
            "sources": [],
            "suggested_actions": [],
            "timestamp": processing_finished_at,
            "processing_started_at": processing_started_at,
            "processing_finished_at": processing_finished_at,
            "processing_duration_ms": max(duration_ms, 0),
            "processing_steps": [
                "Dang phan tich yeu cau",
                "Dang tong hop ngu canh thi truong",
                "Dang soan cau tra loi",
            ],
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
