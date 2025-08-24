import asyncio
import json
import os
from contextlib import asynccontextmanager
from typing import Optional, List
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# ****************** Конфигурация KAFKA ******************
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
MOVIE_TOPIC = "movie-events"
USER_TOPIC = "user-events"
PAYMENT_TOPIC = "payment-events"

kafka_resources = {}


# ****************** Модели данных (Pydantic) на основе спецификации API ******************
class MovieEvent(BaseModel):
    movie_id: int
    title: str
    action: str
    user_id: Optional[int] = None
    rating: Optional[float] = None
    genres: Optional[List[str]] = None
    description: Optional[str] = None

class UserEvent(BaseModel):
    user_id: int
    action: str
    timestamp: str
    username: Optional[str] = None
    email: Optional[str] = None

class PaymentEvent(BaseModel):
    payment_id: int
    user_id: int
    amount: float
    status: str
    timestamp: str
    method_type: Optional[str] = None


# ****************** Управление жизненным циклом приложения (Lifespan) ******************
async def consume():
    """Фоновая задача для чтения сообщений из Kafka."""
    consumer = kafka_resources["consumer"]
    print("[CONSUMER] Starting consumer task...")
    try:
        await consumer.start()
        async for msg in consumer:
            print(
                f"[CONSUMER] Received message from topic '{msg.topic}': {msg.value.decode('utf-8')}"
            )
    except asyncio.CancelledError:
        print("[CONSUMER] Consumer task cancelled.")
    finally:
        print("[CONSUMER] Stopping consumer...")
        await consumer.stop()
        print("[CONSUMER] Consumer stopped.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Запуск
    print("--- Events Service Starting Up ---")
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKERS)
    await producer.start()
    print("[PRODUCER] Kafka Producer started.")
    
    consumer = AIOKafkaConsumer(
        MOVIE_TOPIC, USER_TOPIC, PAYMENT_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        group_id="events-service-group" # Группа консьюмеров
    )
    
    kafka_resources["producer"] = producer
    kafka_resources["consumer"] = consumer
    
    # Запускаем консьюмера в фоновой задаче
    consumer_task = asyncio.create_task(consume())
    kafka_resources["consumer_task"] = consumer_task
    
    yield
    
    # Остановка 
    print("--- Events Service Shutting Down ---")
    kafka_resources["consumer_task"].cancel()
    await kafka_resources["consumer_task"]
    
    await kafka_resources["producer"].stop()
    print("[PRODUCER] Kafka Producer stopped.")
    

app = FastAPI(lifespan=lifespan)


# ****************** API Эндпоинты ******************

@app.get("/api/events/health")
async def health_check():
    return {"status": True}

@app.post("/api/events/movie", status_code=status.HTTP_201_CREATED)
async def create_movie_event(event: MovieEvent):
    producer = kafka_resources["producer"]
    event_json = event.model_dump_json().encode("utf-8")
    
    print(f"[PRODUCER] Sending to topic '{MOVIE_TOPIC}': {event_json.decode()}")
    record_metadata = await producer.send_and_wait(MOVIE_TOPIC, event_json)
    
    return {
        "status": "success",
        "partition": record_metadata.partition,
        "offset": record_metadata.offset,
        "event": event.model_dump()
    }

@app.post("/api/events/user", status_code=status.HTTP_201_CREATED)
async def create_user_event(event: UserEvent):
    producer = kafka_resources["producer"]
    event_json = event.model_dump_json().encode("utf-8")

    print(f"[PRODUCER] Sending to topic '{USER_TOPIC}': {event_json.decode()}")
    record_metadata = await producer.send_and_wait(USER_TOPIC, event_json)

    return {
        "status": "success",
        "partition": record_metadata.partition,
        "offset": record_metadata.offset,
        "event": event.model_dump()
    }

@app.post("/api/events/payment", status_code=status.HTTP_201_CREATED)
async def create_payment_event(event: PaymentEvent):
    producer = kafka_resources["producer"]
    event_json = event.model_dump_json().encode("utf-8")
    
    print(f"[PRODUCER] Sending to topic '{PAYMENT_TOPIC}': {event_json.decode()}")
    record_metadata = await producer.send_and_wait(PAYMENT_TOPIC, event_json)

    return {
        "status": "success",
        "partition": record_metadata.partition,
        "offset": record_metadata.offset,
        "event": event.model_dump()
    }
