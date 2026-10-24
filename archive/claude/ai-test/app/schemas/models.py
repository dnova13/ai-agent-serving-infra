from pydantic import BaseModel
from typing import Optional


class AgentRequest(BaseModel):
    query: str
    provider: Optional[str] = "gemini"  # "gemini" 또는 "claude"


class AgentResponse(BaseModel):
    answer: str
    provider: str
