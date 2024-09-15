import psycopg2
from psycopg2 import Error
from tabulate import tabulate

# Параметри для підключення до бази даних
db_host = "192.168.0.106"
db_name = "Lab7"
db_user = "root"
db_password = "root"

try:
    # Підключення до бази даних PostgreSQL
    conn = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password
    )
    print("З'єднання з базою даних відкрите.")
    cursor = conn.cursor()

    # Функція для відображення таблиць і їх даних
    def display_tables():
        print("\n\nВиведення таблиць")

        # Отримання списку таблиць
        cursor.execute('''
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public';
        ''')
        tables = cursor.fetchall()

        # Виведення структури та даних для кожної таблиці
        for table in tables:
            table_name = table[0]
            cursor.execute(f'SELECT * FROM {table_name};')
            rows = cursor.fetchall()
            print(f"Таблиця: {table_name}")

            # Отримання заголовків колонок
            headers = [desc[0] for desc in cursor.description]

            # Виведення таблиці в форматі
            print(tabulate(rows, headers=headers, tablefmt='grid'))
            print()

    # Функція для виконання та виведення запитів
    def execute_query(query, query_description):
        print(f"\n\n{query_description}")
        cursor.execute(query)
        results = cursor.fetchall()
        headers = [desc[0] for desc in cursor.description]
        print(tabulate(results, headers=headers, tablefmt='grid'))

    # Виклик функції для відображення таблиць
    display_tables()

    # Запит 1: Вивести всі комедії і відсортувати за рейтингом
    query_1 = '''
        SELECT Movie_Name, Genre, Duration, Rating
        FROM Movies
        WHERE Genre = 'Comedy'
        ORDER BY Rating DESC;
    '''
    execute_query(query_1, "Запит 1: Вивести всі комедії і відсортувати за рейтингом")

    # Запит 2: Визначити останню дату показу для кожного фільму
    query_2 = '''
        SELECT m.Movie_Name, c.Cinema_Name, ms.Start_Date, ms.Screening_Days, 
               (ms.Start_Date + INTERVAL '1 day' * (ms.Screening_Days - 1)) AS End_Date
        FROM Movie_Screenings ms
        JOIN Movies m ON ms.Movie_ID = m.Movie_ID
        JOIN Cinemas c ON ms.Cinema_ID = c.Cinema_ID;
    '''
    execute_query(query_2, "Запит 2: Визначити останню дату показу для кожного фільму")

    # Запит 3: Визначити максимальний прибуток кожного кінотеатру з одного сеансу
    query_3 = '''
        SELECT c.Cinema_Name, c.Ticket_Price, c.Seat_Count, 
               (c.Ticket_Price * c.Seat_Count) AS Max_Profit
        FROM Cinemas c;
    '''
    execute_query(query_3, "Запит 3: Визначити максимальний прибуток кожного кінотеатру з одного сеансу")

    # Запит 4: Вивести всі фільми певного жанру (наприклад, комедії)
    query_4 = '''
        SELECT Movie_Name, Genre, Duration, Rating
        FROM Movies
        WHERE Genre = 'Comedy';
    '''
    execute_query(query_4, "Запит 4: Вивести всі фільми певного жанру (наприклад, комедії)")

    # Запит 5: Порахувати кількість фільмів у кожному жанрі
    query_5 = '''
        SELECT Genre, COUNT(*) AS Movie_Count
        FROM Movies
        GROUP BY Genre;
    '''
    execute_query(query_5, "Запит 5: Порахувати кількість фільмів у кожному жанрі")

    # Запит 6: Крос-таблиця для підрахунку кількості показів кожного жанру в кожному кінотеатрі
    query_6 = '''
        SELECT c.Cinema_Name,
               SUM(CASE WHEN m.Genre = 'Melodrama' THEN 1 ELSE 0 END) AS Melodramas_Count,
               SUM(CASE WHEN m.Genre = 'Comedy' THEN 1 ELSE 0 END) AS Comedies_Count,
               SUM(CASE WHEN m.Genre = 'Action' THEN 1 ELSE 0 END) AS Actions_Count
        FROM Movie_Screenings ms
        JOIN Movies m ON ms.Movie_ID = m.Movie_ID
        JOIN Cinemas c ON ms.Cinema_ID = c.Cinema_ID
        GROUP BY c.Cinema_Name;
    '''
    execute_query(query_6, "Запит 6: Крос-таблиця для підрахунку кількості показів кожного жанру в кожному кінотеатрі")

    # Закриття курсору та з'єднання
    cursor.close()
    conn.close()
    print("З'єднання з базою даних закрите.")

except Error as e:
    print(f"Помилка: {e}")
