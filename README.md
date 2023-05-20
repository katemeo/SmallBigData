# Big Data 

Этот репозиторий содержит **Dockerfile** от [apache-airflow](https://github.com/apache/incubator-airflow) для [Docker](https://www.docker.com/)'s [автоматической сборки](https://registry.hub.docker.com/u/puckel/docker-airflow/) опубликованной в общедоступном реестре [Docker Hub Registry](https://registry.hub.docker.com/).

## Информация

* Основано на Python (3.7-slim-buster) официальное изображение [python:3.7-slim-buster](https://hub.docker.com/_/python/) и использует официальный [Postgres](https://hub.docker.com/_/postgres/)
* Скачать [Docker](https://www.docker.com/)
* Скачать [Docker Compose](https://docs.docker.com/compose/install/)

## Предустановка

Для полноценного использования проекта вам необходимо скачать датасет и поместить его в папку datasets.
* Скачать [chess_games.csv](https://www.kaggle.com/datasets/arevel/chess-games)

## Использование

Для запуска проекта:

    docker-compose up -d

Пароль для входа

| Пользователь        | Пароль    |
|---------------------|-----------|
| `airflow`           | `airflow` |

Если вы хотите использовать специальный запрос, убедитесь, что вы настроили подключения:
Перейдите в Admin -> Connections и отредактируйте "postgres_default", установите эти значения (эквивалентно значениям в airflow.cfg/docker-compose.yaml) :

| Переменная          | Изначальное значение |
|---------------------|----------------------|
| `POSTGRES_HOST`     | `postgres`           | 
| `POSTGRES_PORT`     | `5432`               | 
| `POSTGRES_USER`     | `postgres`           |
| `POSTGRES_PASSWORD` | `postgres`           |
| `POSTGRES_DB`       | `airflow`            |

## Ссылка пользовательского интерфейса

- Airflow: [localhost:8080](http://localhost:8080/)


## Использование HIVE

Для создания таблиц под датасеты 

    docker exec -it hive-server hive -f /tmp/scripts/create.hql

Для заполнения данными этих таблиц

    docker exec -it datanode hdfs dfs -copyFromLocal /tmp/datasets/chess_games.csv /user/hive/warehouse/ods.db/chess_games

Для выгрузки датасетов из ods в arhive

    docker exec -it hive-server hive -e "INSERT INTO archive.chess_games SELECT * FROM ods.chess_games";
