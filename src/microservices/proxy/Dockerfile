FROM python:3.12-slim

# Устанавливаем переменную окружения, чтобы pip не кэшировал пакеты
ENV PIP_NO_CACHE_DIR=off 
# Отключаем создание .pyc файлов
ENV PYTHONDONTWRITEBYTECODE=1
# Не буферизуем вывод, чтобы логи сразу появлялись
ENV PYTHONUNBUFFERED=1

RUN mkdir /usr/src/app

COPY main.py /usr/src/app

COPY requirements.txt /usr/src/app

WORKDIR /usr/src/app

RUN pip install -r requirements.txt

EXPOSE 8000

# Используем uvicorn для запуска FastAPI приложения
# --host 0.0.0.0 делает сервис доступным извне контейнера
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]