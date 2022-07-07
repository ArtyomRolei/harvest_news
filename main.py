import requests
import xmltodict
import time
import pprint
import sqlite3


class NewsHarvester:
    """
    Класс NewsHarvester предназначен для сбора новостных RSS-лент.

    Основные действия:
    1. Забирает RSS с сайта https://lenta.ru/rss/news;
    2. Отрезает лишнее от файла, чтобы взять только одну последнюю новость;
    3. Парсит итоговый RSS в словарь;
    4. Следит за тем, чтобы не сохранять дублирующие новости (сверяет по ссылкам, так как они гарантировано уникальные);
    5. Сохраняет новости в базу данных.
    6. Печатает в консоль
    --------------------------------------

    Атрибуты:
    --------------------------------------
    two_last_news_link: list[str]
        - хранит ссылки на последние две новости, чтобы уменьшить лишние запросы к базе;

    timer_for_sleep: int
        - интервал между запросами к lenta.ru (в секундах);

    main_rss_link: str
        - адрес получения RSS;

    db_connection: sqlite3.Connection
    db_cursor: sqlite3.Cursor
        - экземпляры класса Connection и Cursor соответственно, для работы с БД;
    --------------------------------------

    Методы:
    --------------------------------------

    rss_cutter(rss_text: str, substr_to_find: str) -> str
        - Статический метод. Отрезает RSS rss_text до места, где встречается substr_to_find (в нашем случае это </item>),
            завершает отрезанный RSS правильным образом для сохранения XML-структуры документа, возвращает полученное
            значение в виде строки;

    item_is_fresh(news_link: str) -> bool
        - Проверяет, получали ли такую новость ранее. Если не получали, то сохраняет в two_last_news_link методом FIFO
            ссылку на эту новость и возвращает True. Иначе возвращает False;

    run()
        - Запускает вечный рабочий цикл. В цикле происходят действия, описанные в блоке "Основные действия";
    """
    def __init__(self, timer_for_sleep: int = 10):
        # Опытным путём было установлено, что lenta.ru может случайным образом возвращать 2 варианта разных RSS,
        # с последней новостью либо с предпоследней новостью в первом item'е. Поэтому будем хранить список с двумя
        # последними ссылками на новости для последующего сравнения с каждым ответом, чтобы было понимание, новая ли
        # это новость или нет. Решено использовать стандартный list вместо deque, так как тут всего два элемента:
        self.two_last_news_link: list[str] = ['', '']

        # Таймер задержки между запросами:
        self.timer_for_sleep: int = timer_for_sleep

        # Ссылка на публичный RSS, с которого будем собирать новости:
        self.main_rss_link: str = 'https://lenta.ru/rss/news'

        # Присоединяемся к БД (или создаём, если отсутствует) и создаём курсор:
        self.db_connection: sqlite3.Connection = sqlite3.connect(r'rss_news.db')
        self.db_cursor: sqlite3.Cursor = self.db_connection.cursor()
        self.db_cursor.execute("""CREATE TABLE IF NOT EXISTS news(
            id INTEGER PRIMARY KEY,
            author TEXT,
            category TEXT,
            description TEXT,
            img_link TEXT,
            link TEXT,
            pubDate TEXT,
            title TEXT);
        """)
        self.db_connection.commit()

        # Получаем из двух последних добавленных записей БД ссылки на новости для сверки их с новыми поступившими:
        # Здесь в одной строке делаем выборку в обратном порядке по ID, и сразу получаем имена колонок:
        db_description = self.db_cursor.execute("SELECT * FROM news ORDER BY id DESC;").description
        db_column_names = [i[0] for i in db_description]
        # print(db_column_names)
        # Берём последние две записи:
        results = self.db_cursor.fetchmany(2)
        # print(results)
        try:
            # Получаем индекс столбца link:
            column_link_index = db_column_names.index('link')
            try:
                # Забираем ссылки на две последние новости:
                self.two_last_news_link[0] = results[0][column_link_index]
                self.two_last_news_link[1] = results[1][column_link_index]
            except(IndexError):
                print("Не удалось получить данные последних двух записей из БД. (NewsHarvester.__init__)")
        except(ValueError):
            print("Не удалось найти в БД столбец с именем 'link'. (NewsHarvester.__init__)")

    # Так как lenta.ru выдаёт довольно длинный rss-текст, а нам нужна только последняя новость за каждый запрос,
    # имеет смысл отрезать весь текст до первого </item>, чтобы в итоге облегчить работу парсеру xmltodict.parse:
    @staticmethod
    def rss_cutter(rss_text: str, substr_to_find: str) -> str:
        """
        Статический метод.
            Отрезает RSS rss_text до места, где встречается substr_to_find (в нашем случае это </item>),
            завершает отрезанный RSS правильным образом для сохранения XML-структуры документа, возвращает полученное
            значение в виде строки;

        Параметры:
        ------------------------
            rss_text: str
                - Текс RSS, который необходимо обрезать.

            substr_to_find: str
                - Подстрока для поиска первого появления в тексте rss_text, до которой и будет отрезан текст.
                    В нашем случае это </item>.

        ------------------------
        Возвращает: строку, которая является уже обрезанным rss_text и правильно завершенным для сохранения
            xml-структуры докумета (для последующего парсинга).
        """
        try:
            substr_index = rss_text.index(substr_to_find)
        except(ValueError):
            err_text = f'ОШИБКА! Подстрока "{substr_to_find}" \
не найдена в передаваемом тексте rss_text в методе rss_cutter'
            print(err_text)
            return err_text
        else:
            return rss_text[:substr_index] + substr_to_find + '</channel></rss>'

    # Проверяем, точно ли новость является новой. Для этого сверяем ссылку с двумя последними ссылками.
    # Если новость новая, то вставляем в начало списка и возвращаем True, иначе False:
    def item_is_fresh(self, news_link: str) -> bool:
        """
        Проверяет, получали ли такую новость ранее. Если не получали, то сохраняет в two_last_news_link методом FIFO
            ссылку на эту новость и возвращает True. Иначе возвращает False;

        Параметры:
        ---------------------------
            news_link: str
                - Ссылка на полученную новость.
        ---------------------------
        Возвращает: True - в случае, если новость окажется свежей и ранее не полученой. Иначе - False.
        """
        if news_link in self.two_last_news_link:
            return False
        else:
            self.two_last_news_link.insert(0, news_link)
            self.two_last_news_link.pop()
            return True

    # Рабочий цикл:
    def run(self):
        """
        Запускает вечный рабочий цикл. Для удобства дублирую сюда список основных действий метода.
        Основные действия:
        1. Забирает RSS с сайта https://lenta.ru/rss/news;
        2. Отрезает лишнее от файла, чтобы взять только одну последнюю новость;
        3. Парсит итоговый RSS в словарь;
        4. Следит за тем, чтобы не сохранять дублирующие новости (сверяет по ссылкам, так как они гарантировано уникальные);
        5. Сохраняет новости в базу данных.
        """
        time.sleep(1)
        while(True):
            # Запрашиваем RSS:
            response = requests.get(self.main_rss_link)

            # Так как нам нужна только последняя новость, отрезаем лишнее от файла, чтобы облегчить xmltodict.parse:
            cut_rss_text: str = NewsHarvester.rss_cutter(response.text, '</item>')

            # Парсим RSS в словарь:
            rss_dict: dict = xmltodict.parse(cut_rss_text)

            # pprint.pprint(rss_dict) печатал получаемый словарь во время отладки

            # Получаем ссылку из последней новости:
            news_link: str = rss_dict['rss']['channel']['item']['link']

            # Проверим, получали ли такую новость ранее (проверяем по ссылке, так как они уникальные на lenta.ru):
            if self.item_is_fresh(news_link):

                # Если новость свежая, то собираем данные из неё и сохраняем в БД:
                author: str = rss_dict['rss']['channel']['item']['author']
                category: str = rss_dict['rss']['channel']['item']['category']
                description: str = rss_dict['rss']['channel']['item']['description']
                img_link: str = rss_dict['rss']['channel']['item']['enclosure']['@url']
                pub_date: str = rss_dict['rss']['channel']['item']['pubDate']
                title: str = rss_dict['rss']['channel']['item']['title']
                self.db_cursor.execute("""INSERT INTO news(author, category, description, img_link, 
                    link, pubDate, title) 
                    VALUES(?, ?, ?, ?, ?, ?, ?);
                """, (author, category, description, img_link, news_link, pub_date, title))
                self.db_connection.commit()

                # Печатаем записи БД в консоль (для демонстрации результата):
                self.db_cursor.execute("SELECT * FROM news;")
                all_results = self.db_cursor.fetchall()
                pprint.pprint(all_results)
                print('Получена свежая новость, БД обновлена!')
                # pprint.pprint(self.two_last_news_link)
            else:
                print('Нет свежих новостей.')

            time.sleep(self.timer_for_sleep)


if __name__ == '__main__':
    NewsHarvester().run()
