"""
User schemas for request/response validation.
"""

from pydantic import BaseModel, EmailStr, Field, ConfigDict
from typing import Optional
from datetime import datetime


# User Schemas
class UserBase(BaseModel):
    """Base user schema."""

    email: EmailStr
    full_name: Optional[str] = None


class UserCreate(UserBase):
    """Schema for user registration."""

    password: str = Field(..., min_length=8, max_length=100)


class UserLogin(BaseModel):
    """Schema for user login."""

    email: EmailStr
    password: str


class UserUpdate(BaseModel):
    """Schema for updating user profile."""

    full_name: Optional[str] = None
    email: Optional[EmailStr] = None


class UserResponse(UserBase):
    """Schema for user response."""

    id: int
    is_active: bool
    is_verified: bool
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class UserInDB(UserResponse):
    """Schema for user in database (includes hashed password)."""

    hashed_password: str


# Token Schemas
class Token(BaseModel):
    """JWT token response."""

    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class TokenData(BaseModel):
    """Data stored in JWT token."""

    user_id: Optional[int] = None
    email: Optional[str] = None


class RefreshTokenRequest(BaseModel):
    """Request to refresh access token."""

    refresh_token: str


# User Settings Schemas
class UserSettingsBase(BaseModel):
    """Base user settings schema."""

    default_investment: Optional[float] = 1000000
    preferred_market: Optional[str] = "HOSE"
    risk_tolerance: Optional[str] = "moderate"  # conservative, moderate, aggressive
    notifications_enabled: Optional[bool] = True
    theme: Optional[str] = "light"


class UserSettingsCreate(UserSettingsBase):
    """Schema for creating user settings."""

    pass


class UserSettingsUpdate(UserSettingsBase):
    """Schema for updating user settings."""

    pass


class UserSettingsResponse(UserSettingsBase):
    """Schema for user settings response."""

    id: int
    user_id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)
