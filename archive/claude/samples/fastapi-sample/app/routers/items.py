from fastapi import APIRouter, HTTPException

from app.schemas import Item, ItemCreate, ItemUpdate

router = APIRouter(prefix="/items", tags=["items"])

# 데모용 인메모리 저장소 (실제 서비스에서는 DB 연동으로 교체)
_items: dict[int, Item] = {}
_next_id = 1


@router.get("", response_model=list[Item])
def list_items() -> list[Item]:
    return list(_items.values())


@router.get("/{item_id}", response_model=Item)
def get_item(item_id: int) -> Item:
    item = _items.get(item_id)
    if item is None:
        raise HTTPException(status_code=404, detail=f"Item #{item_id} not found")
    return item


@router.post("", response_model=Item, status_code=201)
def create_item(payload: ItemCreate) -> Item:
    global _next_id
    item = Item(id=_next_id, **payload.model_dump())
    _items[item.id] = item
    _next_id += 1
    return item


@router.patch("/{item_id}", response_model=Item)
def update_item(item_id: int, payload: ItemUpdate) -> Item:
    item = _items.get(item_id)
    if item is None:
        raise HTTPException(status_code=404, detail=f"Item #{item_id} not found")
    updated = item.model_copy(update=payload.model_dump(exclude_unset=True))
    _items[item_id] = updated
    return updated


@router.delete("/{item_id}", status_code=204)
def delete_item(item_id: int) -> None:
    if item_id not in _items:
        raise HTTPException(status_code=404, detail=f"Item #{item_id} not found")
    del _items[item_id]
