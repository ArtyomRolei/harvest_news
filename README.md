Сервис по сбору новостных RSS-лент с сайта https://lenta.ru/rss/news.

Сервис за интервал времени в 10 секунд запрашивает RSS, затем забирает из него
только последнюю новость, и если такой новости нет в базе данных сервиса, то 
сохраняет её в базу. 

Выводит в консоль всю базу данных (для демонстрации), но только в тех случаях, 
когда она была успешно пополнена свежей новостью.

Зависимости перечислены в requirements.txt.

Присутствует готовый рабочий Dockerfile, успешно протестированно на докере. # harvest_news
