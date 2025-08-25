# Задание 1

1. Спроектирована to be архитектуру КиноБездны, система разделена на отдельные домены и организовано интеграционное взаимодействие и единая точка вызова сервисов.
Результат представлен в виде контейнерной диаграммы в нотации С4.

[To-Be архитектура решения.](./schemas/1.png)

# Задание 2

### 1. Proxy
Команда КиноБездны уже выделила сервис метаданных о фильмах movies и вам необходимо реализовать бесшовный переход с применением паттерна Strangler Fig в части реализации прокси-сервиса (API Gateway), с помощью которого можно будет постепенно переключать траффик, используя фиче-флаг.

Сервис реализован на языке программирования Python с использованием библиотеки FastAPI. Файл сервиса (main.py), зависимости (requirements.txt) и Dockerfile размещены в каталоге ./src/microservices/proxy.

Конфигурация для запуска сервиса берется из файла docker-compose.

[Фрагмент лога, соответствующий запуску сервиса.](./schemas/2_1__1.png)

Протестируем постепенный переход, изменив переменную окружения MOVIES_MIGRATION_PERCENT=80% в файле docker-compose.yml.

Будем многократно отправлять запросы к API Gateway:
```bash
curl http://localhost:8000/api/movies
```

Приведенный далее фрагмент лога показывает, что при MOVIES_MIGRATION_PERCENT=80% микросервис вызывается значительно чаще чем монолит.

[Фрагмент лога.](./schemas/2_1__2.png)

Запустим postman тесты - они все зеленые (тесты для events не должны запускаться на этом этапе).

[Форма postman 1.](./schemas/2_1__p1.png)

[Форма postman 2.](./schemas/2_1__p2.png)

[Форма postman 3.](./schemas/2_1__p3.png)



### 2. Kafka
Проверим гипотезу насколько просто реализовать применение Kafka в данной архитектуре.

Для этого нужно сделать MVP сервис events, который будет при вызове API создавать и сам же читать сообщения в топике Kafka.

Сервис с consumer'ами и producer'ами реализован на языке программирования Python с использованием библиотеки FastAPI. Файл сервиса (main.py), зависимости (requirements.txt) и Dockerfile размещены в каталоге ./src/microservices/events.

Файл docker-compose модифицирован.

Запустим postman тесты для сервиса events.

[Форма postman (для сервиса events).](./schemas/2_2__p1.png)

Скриншот состояния топиков Kafka из UI http://localhost:8090 

[Скриншот.](./schemas/2_2__kafka_topics.png)



# Задание 3

Команда начала переезд в Kubernetes для лучшего масштабирования и повышения надежности. 
Вам, как архитектору осталось самое сложное:
 - реализовать CI/CD для сборки прокси сервиса
 - реализовать необходимые конфигурационные файлы для переключения трафика.

### CI/CD

В папке .github/worflows доработан деплой новых сервисов proxy и events в docker-build-push.yml, чтобы api-tests при сборке отрабатывали корректно при отправке коммита в ваш репозиторий.

["зеленая" сборка и "зеленые" тесты.](./schemas/3_ci_1.png)

[образы в github registry.](./schemas/3_ci_2.png)


### Proxy в Kubernetes

#### Шаг 1

Для деплоя в kubernetes необходимо залогиниться в docker registry Github'а.

1. Создан Personal Access Token (PAT) https://github.com/settings/tokens с правом read:packages
   
2. В src/kubernetes/*.yaml (event-service, monolith, movies-service и proxy-service) отредактирован путь до образов 

3. Добавлен секрет в src/kubernetes/dockerconfigsecret.yaml 
 

#### Шаг 2

Доработаны src/kubernetes/event-service.yaml и src/kubernetes/proxy-service.yaml (созданы Deployment и Service).

Доработан ingress.yaml, чтобы можно было с помощью тестов проверить создание событий

Выполнены шаги для поднятия кластера:

  1. Создан namespace:
  ```bash
  kubectl apply -f src/kubernetes/namespace.yaml
  ```
  2. Созданы секреты и переменные
  ```bash
  kubectl apply -f src/kubernetes/configmap.yaml
  kubectl apply -f src/kubernetes/secret.yaml
  kubectl apply -f src/kubernetes/dockerconfigsecret.yaml
  kubectl apply -f src/kubernetes/postgres-init-configmap.yaml
  ```

  3. Развернута база данных:
  ```bash
  kubectl apply -f src/kubernetes/postgres.yaml
  ```

  4. Развернута Kafka:
  ```bash
  kubectl apply -f src/kubernetes/kafka/kafka.yaml
  ```

[экранная форма с результатами развертывания.](./schemas/3_kub_1.png)

Проверим, что теперь запущено 3 пода:

[экранная форма с проверкой подов.](./schemas/3_kub_2.png)

  5. Развернут монолит:
  ```bash
  kubectl apply -f src/kubernetes/monolith.yaml
  ```
  6. Развернуты микросервисы:
  ```bash
  kubectl apply -f src/kubernetes/movies-service.yaml
  kubectl apply -f src/kubernetes/events-service.yaml
  ```
  7. Развернут прокси-сервис:
  ```bash
  kubectl apply -f src/kubernetes/proxy-service.yaml
  ```

[экранная форма с результатами развертывания.](./schemas/3_kub_3.png)

  После запуска и поднятия подов вывод команды 
  ```bash
  kubectl -n cinemaabyss get pod
  ```
  Будет наподобие такого
```bash
  NAME                              READY   STATUS    
  events-service-7587c6dfd5-6whzx   1/1     Running  
  kafka-0                           1/1     Running   
  monolith-8476598495-wmtmw         1/1     Running  
  movies-service-6d5697c584-4qfqs   1/1     Running  
  postgres-0                        1/1     Running  
  proxy-service-577d6c549b-6qfcv    1/1     Running  
  zookeeper-0                       1/1     Running 
```

Получен ожидаемый результат:

[экранная форма с проверкой подов.](./schemas/3_kub_3.png)


  8. Добавим ingress

  - добавьте аддон
  ```bash
  minikube addons enable ingress
  ```
  ```bash
  kubectl apply -f src/kubernetes/ingress.yaml
  ```
  9. Добавьте в /etc/hosts
  127.0.0.1 cinemaabyss.example.com

  10. Вызовите
  ```bash
  minikube tunnel
  ```
  11. Вызовите https://cinemaabyss.example.com/api/movies
  Вы должны увидеть вывод списка фильмов
  Можно поэкспериментировать со значением   MOVIES_MIGRATION_PERCENT в src/kubernetes/configmap.yaml и убедится, что вызовы movies уходят полностью в новый сервис

  12. Запустите тесты из папки tests/postman
  ```bash
   npm run test:kubernetes
  ```
  Часть тестов с health-чек упадет, но создание событий отработает.
  Откройте логи event-service и сделайте скриншот обработки событий

#### Шаг 3
Добавьте сюда скриншота вывода при вызове https://cinemaabyss.example.com/api/movies и  скриншот вывода event-service после вызова тестов.


# Задание 4
Для простоты дальнейшего обновления и развертывания вам как архитектуру необходимо так же реализовать helm-чарты для прокси-сервиса и проверить работу 

Для этого:
1. Перейдите в директорию helm и отредактируйте файл values.yaml

```yaml
# Proxy service configuration
proxyService:
  enabled: true
  image:
    repository: ghcr.io/db-exp/cinemaabysstest/proxy-service
    tag: latest
    pullPolicy: Always
  replicas: 1
  resources:
    limits:
      cpu: 300m
      memory: 256Mi
    requests:
      cpu: 100m
      memory: 128Mi
  service:
    port: 80
    targetPort: 8000
    type: ClusterIP
```

- Вместо ghcr.io/db-exp/cinemaabysstest/proxy-service напишите свой путь до образа для всех сервисов
- для imagePullSecret проставьте свое значение (скопируйте из конфигурации kubernetes)
  ```yaml
  imagePullSecrets:
      dockerconfigjson: ewoJImF1dGhzIjogewoJCSJnaGNyLmlvIjogewoJCQkiYXV0aCI6ICJaR0l0Wlhod09tZG9jRjl2UTJocVZIa3dhMWhKVDIxWmFVZHJOV2hRUW10aFVXbFZSbTVaTjJRMFNYUjRZMWM9IgoJCX0KCX0sCgkiY3JlZHNTdG9yZSI6ICJkZXNrdG9wIiwKCSJjdXJyZW50Q29udGV4dCI6ICJkZXNrdG9wLWxpbnV4IiwKCSJwbHVnaW5zIjogewoJCSIteC1jbGktaGludHMiOiB7CgkJCSJlbmFibGVkIjogInRydWUiCgkJfQoJfSwKCSJmZWF0dXJlcyI6IHsKCQkiaG9va3MiOiAidHJ1ZSIKCX0KfQ==
  ```

2. В папке ./templates/services заполните шаблоны для proxy-service.yaml и events-service.yaml (опирайтесь на свою kubernetes конфигурацию - смысл helm'а сделать шаблоны для быстрого обновления и установки)

```yaml
template:
    metadata:
      labels:
        app: proxy-service
    spec:
      containers:
       Тут ваша конфигурация
```

3. Проверьте установку
Сначала удалим установку руками

```bash
kubectl delete all --all -n cinemaabyss
kubectl delete  namespace cinemaabyss
```
Запустите 
```bash
helm install cinemaabyss .\src\kubernetes\helm --namespace cinemaabyss --create-namespace
```
Если в процессе будет ошибка
```code
[2025-04-08 21:43:38,780] ERROR Fatal error during KafkaServer startup. Prepare to shutdown (kafka.server.KafkaServer)
kafka.common.InconsistentClusterIdException: The Cluster ID OkOjGPrdRimp8nkFohYkCw doesn't match stored clusterId Some(sbkcoiSiQV2h_mQpwy05zQ) in meta.properties. The broker is trying to join the wrong cluster. Configured zookeeper.connect may be wrong.
```

Проверьте развертывание:
```bash
kubectl get pods -n cinemaabyss
minikube tunnel
```

Потом вызовите 
https://cinemaabyss.example.com/api/movies
и приложите скриншот развертывания helm и вывода https://cinemaabyss.example.com/api/movies

## Удаляем все

```bash
kubectl delete all --all -n cinemaabyss
kubectl delete namespace cinemaabyss
```
