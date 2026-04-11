from datetime import datetime

from app.schemas.chat import ChatMessageResponse


def test_chat_message_response_accepts_processing_metadata():
    payload = ChatMessageResponse(
        message="ok",
        conversation_id="conv-1",
        sources=[],
        suggested_actions=[],
        timestamp=datetime.utcnow(),
        processing_started_at=datetime.utcnow(),
        processing_finished_at=datetime.utcnow(),
        processing_duration_ms=8123,
        processing_steps=["step-1", "step-2"],
    )

    assert payload.processing_duration_ms == 8123
    assert payload.processing_steps == ["step-1", "step-2"]
