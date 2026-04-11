from app.chatbot.chatbot_service import PortfolioChatbot
from app.config import settings


class _FakeMarketData:
    @staticmethod
    def get_context_from_query(_query: str):
        return "VNINDEX tang nhe"


def test_generate_response_requires_openrouter_api_key(monkeypatch):
    monkeypatch.setattr(settings, "openrouter_api_key", "")

    chatbot = PortfolioChatbot(
        api_key="",
        model_name="openrouter/test-model",
        market_data_adapter=_FakeMarketData(),
    )

    response = chatbot.generate_response("Xin chao")

    assert "OPENROUTER_API_KEY" in response


def test_generate_response_requires_openrouter_model(monkeypatch):
    monkeypatch.setattr(settings, "openrouter_model", "")

    chatbot = PortfolioChatbot(
        api_key="test-key",
        model_name="",
        market_data_adapter=_FakeMarketData(),
    )

    response = chatbot.generate_response("Xin chao")

    assert "OPENROUTER_MODEL" in response


def test_generate_response_calls_openrouter_and_returns_text(monkeypatch):
    captured = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"choices": [{"message": {"content": "Du lieu da duoc phan tich."}}]}

    def fake_post(self, url, headers=None, json=None, timeout=None):
        captured["url"] = url
        captured["headers"] = headers
        captured["json"] = json
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr("requests.Session.post", fake_post)

    chatbot = PortfolioChatbot(
        api_key="openrouter-key",
        model_name="openai/gpt-4o-mini",
        base_url="https://openrouter.ai/api/v1",
        market_data_adapter=_FakeMarketData(),
    )

    response = chatbot.generate_response(
        user_message="Phan tich VNINDEX",
        portfolio_context={"risk_tolerance": "medium"},
        conversation_id="conv-1",
    )

    assert response == "Du lieu da duoc phan tich."
    assert captured["url"] == "https://openrouter.ai/api/v1/chat/completions"
    assert captured["headers"]["Authorization"] == "Bearer openrouter-key"
    assert captured["json"]["model"] == "openai/gpt-4o-mini"
    assert captured["json"]["messages"][-1]["role"] == "user"
    assert captured["json"]["messages"][-1]["content"] == "Phan tich VNINDEX"


def test_chat_returns_processing_metadata(monkeypatch):
    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"choices": [{"message": {"content": "Da phan tich xong."}}]}

    def fake_post(self, url, headers=None, json=None, timeout=None):
        return FakeResponse()

    monkeypatch.setattr("requests.Session.post", fake_post)

    chatbot = PortfolioChatbot(
        api_key="openrouter-key",
        model_name="openai/gpt-4o-mini",
        base_url="https://openrouter.ai/api/v1",
        market_data_adapter=_FakeMarketData(),
    )

    result = chatbot.chat(
        message="Phan tich VNM", conversation_id="conv-42", context={}
    )

    assert result["response"] == "Da phan tich xong."
    assert "processing_started_at" in result
    assert "processing_finished_at" in result
    assert isinstance(result["processing_duration_ms"], int)
    assert result["processing_duration_ms"] >= 0
    assert result["processing_steps"] == [
        "Dang phan tich yeu cau",
        "Dang tong hop ngu canh thi truong",
        "Dang soan cau tra loi",
    ]
