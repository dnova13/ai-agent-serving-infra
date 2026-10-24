import type { Item } from '../types';

interface ItemListProps {
  items: Item[];
  onToggle: (item: Item) => void;
  onDelete: (item: Item) => void;
}

export function ItemList({ items, onToggle, onDelete }: ItemListProps) {
  if (items.length === 0) {
    return <p>등록된 항목이 없습니다.</p>;
  }

  return (
    <ul>
      {items.map((item) => (
        <li key={item.id}>
          <label>
            <input type="checkbox" checked={item.done} onChange={() => onToggle(item)} />
            <span style={{ textDecoration: item.done ? 'line-through' : 'none' }}>
              {item.name}
            </span>
          </label>
          {item.description && <small> — {item.description}</small>}
          <button onClick={() => onDelete(item)}>삭제</button>
        </li>
      ))}
    </ul>
  );
}
