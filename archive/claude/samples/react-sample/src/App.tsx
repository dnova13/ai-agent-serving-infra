import { FormEvent, useEffect, useState } from 'react';
import { itemsApi } from './api';
import { ItemList } from './components/ItemList';
import type { Item } from './types';

export default function App() {
  const [items, setItems] = useState<Item[]>([]);
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [error, setError] = useState<string | null>(null);

  const loadItems = () => {
    itemsApi
      .list()
      .then(setItems)
      .catch((err) => setError(err.message));
  };

  useEffect(() => {
    loadItems();
  }, []);

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!name.trim()) return;
    try {
      await itemsApi.create({ name, description: description || undefined });
      setName('');
      setDescription('');
      loadItems();
    } catch (err) {
      setError((err as Error).message);
    }
  };

  const handleToggle = async (item: Item) => {
    await itemsApi.update(item.id, { done: !item.done });
    loadItems();
  };

  const handleDelete = async (item: Item) => {
    await itemsApi.remove(item.id);
    loadItems();
  };

  return (
    <main style={{ maxWidth: 480, margin: '40px auto', fontFamily: 'sans-serif' }}>
      <h1>Items</h1>
      {error && <p style={{ color: 'red' }}>{error}</p>}

      <form onSubmit={handleSubmit} style={{ display: 'flex', gap: 8, marginBottom: 16 }}>
        <input
          placeholder="이름"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />
        <input
          placeholder="설명 (선택)"
          value={description}
          onChange={(e) => setDescription(e.target.value)}
        />
        <button type="submit">추가</button>
      </form>

      <ItemList items={items} onToggle={handleToggle} onDelete={handleDelete} />
    </main>
  );
}
