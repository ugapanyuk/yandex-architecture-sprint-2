from contextlib import asynccontextmanager
import os
import random
import httpx
from fastapi import FastAPI
from fastapi import Request
from fastapi.responses import StreamingResponse, Response

# ************************ Конфигурируем сервис *******************************
# Загружаем URL-адреса других сервисов из переменных окружения
MONOLITH_URL = os.getenv("MONOLITH_URL", "http://localhost:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://localhost:8081")
# Загружаем конфигурацию для постепенной миграции
GRADUAL_MIGRATION = str(os.getenv("GRADUAL_MIGRATION", "false"))
MOVIES_MIGRATION_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", "50"))


# ************************ Конфигурируем и создаем приложение FastAPI *******************************

# Создаем один HTTP-клиент для всего приложения для переиспользования соединений
# Это более эффективно, чем создавать новый клиент для каждого запроса
# Используем context manager для управления жизненным циклом
clients = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    # При старте приложения создаем асинхронный HTTP-клиент
    clients["httpx"] = httpx.AsyncClient()
    print("--- Strangler Fig Proxy Service Started ---")
    print(f"Monolith URL: {MONOLITH_URL}")
    print(f"Movies Service URL: {MOVIES_SERVICE_URL}")
    print(f"Gradual migration: {GRADUAL_MIGRATION}")
    print(f"Migration traffic percentage: {MOVIES_MIGRATION_PERCENT}%")
    yield
    # При остановке приложения закрываем клиент
    await clients["httpx"].aclose()
    print("--- Strangler Fig Proxy Service Stopped ---")


# Создаем объект FastAPI с использованием контекстного менеджера lifespan
app = FastAPI(lifespan=lifespan)


# ************************ Реализация проксирования ******************************

@app.get("/health")
async def health_check():
    """Эндпоинт для проверки работоспособности прокси-сервиса."""
    return Response(content="Strangler Fig Proxy is healthy", status_code=200)


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
async def proxy_request(request: Request, path: str):
    """
    Основной эндпоинт, который перехватывает все запросы и проксирует их
    на соответствующий сервис (монолит или микросервис).
    """
    client: httpx.AsyncClient = clients["httpx"]
    
    target_url = MONOLITH_URL
    
    # Логика паттерна Strangler Fig
    # Если запрос касается фильмов, применяем логику постепенного переключения
    if path.startswith("api/movies") and GRADUAL_MIGRATION=="true":
        # Генерируем случайное число от 0 до 99
        if random.randint(0, 99) < MOVIES_MIGRATION_PERCENT:
            target_url = MOVIES_SERVICE_URL
            print(f"Routing '/{path}' to -> MOVIES-SERVICE")
        else:
            # В противном случае переключаем target_url на монолит
            print(f"Routing '/{path}' to -> MONOLITH")
    else:
        # Все остальные запросы идут на монолит
        print(f"Routing '/{path}' to -> MONOLITH")

    # Формируем полный URL для переключения сервиса
    run_url = f"{target_url}/{path}"
    
    # Копируем заголовки, исключая 'host', т.к. он будет заменен httpx
    headers = {key: value for key, value in request.headers.items() if key.lower() != 'host'}

    # Создаем запрос к сервису
    req = client.build_request(
        method=request.method,
        url=run_url,
        headers=headers,
        params=request.query_params,
        content=await request.body(),
        timeout=60.0 # Увеличиваем таймаут для надежности
    )
    
    try:
        # Отправляем запрос и получаем ответ
        run_resp = await client.send(req, stream=True)
        
        # Возвращаем ответ клиенту в виде потока, чтобы не загружать память
        return StreamingResponse(
            run_resp.aiter_raw(),
            status_code=run_resp.status_code,
            headers=run_resp.headers,
            media_type=run_resp.headers.get("content-type"),
        )
    except httpx.RequestError as e:
        # Обработка ошибок сети при обращении к сервисам
        error_message = f"Failed to connect to service: {e}"
        print(f"ERROR: {error_message}")
        return Response(content=error_message, status_code=503) # Service Unavailable


if __name__ == "__main__":
    import uvicorn
    # Запуск сервера для локальной отладки
    uvicorn.run(app, host="0.0.0.0", port=8000)
