from typing import Optional

from pydantic import BaseModel, Field


class ItemBase(BaseModel):
    name: str = Field(min_length=1)
    description: Optional[str] = None
    done: bool = False


class ItemCreate(ItemBase):
    pass


class ItemUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    done: Optional[bool] = None


class Item(ItemBase):
    id: int
