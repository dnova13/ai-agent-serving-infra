import type { Item } from './types';

// NestJS 샘플(3000번 포트)과 FastAPI 샘플(8000번 포트) 모두 동일한 /items 스펙을 제공
const BASE_URL = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:3000';

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  if (!res.ok) {
    throw new Error(`Request failed: ${res.status} ${res.statusText}`);
  }
  if (res.status === 204) {
    return undefined as T;
  }
  return res.json() as Promise<T>;
}

export const itemsApi = {
  list: () => request<Item[]>('/items'),
  create: (data: Pick<Item, 'name' | 'description'>) =>
    request<Item>('/items', { method: 'POST', body: JSON.stringify(data) }),
  update: (id: number, data: Partial<Item>) =>
    request<Item>(`/items/${id}`, { method: 'PATCH', body: JSON.stringify(data) }),
  remove: (id: number) => request<void>(`/items/${id}`, { method: 'DELETE' }),
};
