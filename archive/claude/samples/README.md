# Samples

동일한 "Items" CRUD 스펙(`GET/POST/PATCH/DELETE /items`)을 각각 NestJS와 FastAPI로 구현하고,
React 프론트엔드에서 두 백엔드 중 하나에 붙여서 사용할 수 있도록 구성한 샘플 모음입니다.

## nestjs-sample (포트 3000)

```bash
cd nestjs-sample
npm install
npm run start:dev
```

## fastapi-sample (포트 8000)

```bash
cd fastapi-sample
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

## react-sample (포트 5173)

```bash
cd react-sample
npm install
npm run dev
```

기본적으로 NestJS(`http://localhost:3000`)를 바라봅니다. FastAPI에 붙이려면
`react-sample` 디렉터리에 `.env` 파일을 만들고 아래처럼 지정하세요.

```
VITE_API_BASE_URL=http://localhost:8000
```
