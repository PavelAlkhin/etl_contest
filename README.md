<h2>ETL test</h2>

Для выполнения потребуются: 
- Docker  
- Python 3.6+  

<h3>Установка окружения:</h3>

```shell
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
docker-compose up -d
```

<h3>Задачи:</h3>

- Сохранить данные из двух таблиц сервера-источника в одну денормализованную таблицу сервера-назначения.  
- Написать тест, проверяющий эквивалентность данных в обоих серверах.  

<h3>Требования к коду:</h3>

- Код для тестов положить в пакет etl.  
- Загрузка данных батчами по интервалу дат с шагом в час.  
- Дозагрузка данных с момента последней даты в сервере назначения.  
