import Parser, DB, re, psycopg2
from datetime import date


# Точка входа
def main() -> None:
    db_item = DB.db()
    k = 0
    coinsList = Parser.parser().get_coins_list()
    db_item.create_new_db("coin_database")

    try:
        db_item.cursor.execute("create table coins (id serial primary key, name char(30), url char(50), last_update date null)")
        for coin_item in coinsList:
            db_item.cursor.execute("insert into coins (name, url) values (%s, %s)", (coin_item[0], coin_item[1]))
        db_item.conn.commit()
    except psycopg2.ProgrammingError as e:
        print("Таблица coins уже существует, она будет обновлена!")
        db_item.conn.commit()
        for coin_item in coinsList:
            db_item.cursor.execute("select id from coins where name=%s", (coin_item[0],))
            if db_item.cursor.fetchone() == None:
                db_item.cursor.execute("insert into coins (name, url) values (%s, %s)", (coin_item[0], coin_item[1]))
        db_item.conn.commit()

    print("Выполнить первую часть задания ? (Первые 3 пункта) 1 / 0")
    if int(input()) == 1:
        print("Выберите валюту: (Для продолжения списка вводить пустую строку)")
        result = choice_coin(db_item)
        choice, name_of_coin, url_of_coin, last_update = result
        data_of_coin = Parser.parser().get_data_about_coin(url_of_coin)
        insert_data_about_coin(db_item,name_of_coin, data_of_coin)

    print("\nВторая часть задания!")
    fl = True
    while fl:
        print("\n--- Показать прибыль по выбраной валюте\t1")
        print("--- Для выбранной валюты посчитать количество дней повышения курса\t2")
        print("--- По выбраной валюте показать объем первом дне появления на сайте\t3")
        print("--- Отображение информации о валюте в определенный день\t4")
        print("--- Сравнение двух валют\t5")
        print("--- Exit\telse")
        try:
            choice = int(input())
        except Exception as e:
            print("Некоректный ввод!")
            exit(-1)
        if choice == 1:
            incom_info(db_item)
        elif choice == 2:
            count_up_and_down(db_item)
        elif choice == 3:
            volume_of_first_day(db_item)
        elif choice == 4:
            get_data_of_day(db_item)
        elif choice == 5:
            compare_pair(db_item)
        else:
            fl = False
    db_item.end_session()


# Добавляет данные о крипте в таблицу
def insert_data_about_coin(db_item: 'db', name: 'str', data: 'list') -> None:
    try:
        db_item.cursor.execute("""create table data_about_coins 
        (id serial primary key, coin_id integer references coins (id) on delete cascade on update cascade, day date, open real, high real, low real, close real, volume bigint, market_cap bigint)""")
        db_item.conn.commit()
        for day_info in data:
            db_item.cursor.execute("""insert into data_about_coins
            (coin_id, day, open, high, low, close, volume, market_cap)
            values
            ((select id from coins where name=%s), %s, %s, %s, %s, %s, %s, %s)
            """, (name, str(day_info[0]), day_info[1], day_info[2], day_info[3], day_info[4], day_info[5], day_info[6]))
        db_item.cursor.execute('update coins set last_update=%s where name=%s',(str(data[0][0]),name))
    except psycopg2.ProgrammingError as e:
        db_item.conn.commit()
        db_item.cursor.execute("select last_update from coins where name=%s", (name,))
        result_of_operation = db_item.cursor.fetchone()[0]
        if result_of_operation == None:
            for day_info in data:
                db_item.cursor.execute("""insert into data_about_coins
                (coin_id, day, open, high, low, close, volume, market_cap)
                values
                ((select id from coins where name=%s), %s, %s, %s, %s, %s, %s, %s)
                """, (name, str(day_info[0]), day_info[1], day_info[2], day_info[3], day_info[4], day_info[5], day_info[6]))
            db_item.cursor.execute('update coins set last_update=%s where name=%s', (str(data[0][0]), name))
        else:
            last_update = result_of_operation
            today = data[0][0]
            for i in range(0,(today - last_update).days):
                db_item.cursor.execute("""insert into data_about_coins
                               (coin_id, day, open, high, low, close, volume, market_cap)
                               values
                               ((select id from coins where name=%s), %s, %s, %s, %s, %s, %s, %s)
                               """, (name, str(data[i][0]), data[i][1], data[i][2], data[i][3], data[i][4], data[i][5], data[i][6]))
    db_item.conn.commit()


#считывает данные с бд для выборы критовалюты из списка, который был получен с сайта
def choice_coin(db_item: 'db') -> tuple:
    i = 0
    fl = True
    while fl == True:
        db_item.cursor.execute("select * from coins order by id offset %s limit %s", (i, 7))
        items = db_item.cursor.fetchall()
        if items != None:
            for item in items:
                print(str(item[0]) + '\t' + item[1] + '\t')
            id = input()
            if id != '':
                id = int(id)
                db_item.cursor.execute("select * from coins where id=%s", (id,))
                result = db_item.cursor.fetchone()
                name_of_coin = re.search(r"\w+", result[1]).group()
                url_of_coin = re.search(r"/.*/.*/",result[2]).group()
                last_update = result[3]
                break
            else:
                i += 7
        else:
            fl = False
    return (id, name_of_coin, url_of_coin, last_update)


#Информация о днях повышения и снижения курсы валюты
def count_up_and_down(db_item: 'db') -> None:
    print("Быберите валюту из списка (Пустая строка для продолжения списка):")
    choice = choice_coin(db_item)
    if choice[3] == None or choice[3] != date.today():
        data_of_coin = Parser.parser().get_data_about_coin(choice[2])
        insert_data_about_coin(db_item,choice[1],data_of_coin)
    db_item.cursor.execute("select count(*) from data_about_coins where close>open and coin_id=%s", (choice[0],))
    up = db_item.cursor.fetchone()[0];
    db_item.cursor.execute("select count(*) from data_about_coins where open>close and coin_id=%s", (choice[0],))
    down = db_item.cursor.fetchone()[0];
    print("\nПовышение (кол. дней) = {0}\nПонижение (кол. дней) = {1}".format(up,down))


#Получение информации о валюте в определенный день
def get_data_of_day(db_item: 'db') -> None:
    print("Быберите валюту из списка (Пустая строка для продолжения списка):")
    choice = choice_coin(db_item)
    if choice[3] == None or choice[3] != date.today():
        data_of_coin = Parser.parser().get_data_about_coin(choice[2])
        insert_data_about_coin(db_item, choice[1], data_of_coin)
    db_item.cursor.execute('select min(day), max(day) from data_about_coins where coin_id=%s', (choice[0],))
    result_of_operation = db_item.cursor.fetchone()
    min, max = result_of_operation[0], result_of_operation[1]
    day = input("Введите дату в интервале от " + str(result_of_operation[0]) + " до " + str(result_of_operation[1]) + " в формате ГГГГ-ММ-ДД: ")
    _day = date_from_str(day)
    if _day < min or _day > max:
        exit(-1)
    db_item.cursor.execute('select open, high, low, close, volume, market_cap from data_about_coins where day=%s and coin_id=%s', (day,choice[0]))
    result_of_operation = db_item.cursor.fetchone()
    print("\nopen = {0}\t high = {1}\t low = {2}\nclose = {3}\t volume = {4}\t market_cap = {5}".format(*result_of_operation))


#Получает данные о валюте в первый день
def volume_of_first_day(db_item : 'db') -> None:
    print("Быберите валюту из списка (Пустая строка для продолжения списка):")
    choice = choice_coin(db_item)
    if choice[3] == None or choice[3] != date.today():
        data_of_coin = Parser.parser().get_data_about_coin(choice[2])
        insert_data_about_coin(db_item, choice[1], data_of_coin)
    db_item.cursor.execute('select min(day) from data_about_coins where coin_id=%s', (choice[0],))
    first_day = db_item.cursor.fetchone()[0]
    db_item.cursor.execute('select volume from data_about_coins where coin_id=%s and day=%s', (choice[0], first_day))
    volume = db_item.cursor.fetchone()[0]
    if volume == 0:
        volume = '-'
    print("\nВалюта фиксируется с: {0}\nvolume = {1}".format(first_day, volume))


#Расчитывает прибыль и пр. для пользователя
def incom_info(db_item: 'db') -> None:
    print("Быберите валюту из списка (Пустая строка для продолжения списка):")
    choice = choice_coin(db_item)
    if choice[3] == None or choice[3] != date.today():
        data_of_coin = Parser.parser().get_data_about_coin(choice[2])
        insert_data_about_coin(db_item, choice[1], data_of_coin)
    summ = float(input("Введите сумму вложений (USD): "))
    db_item.cursor.execute("select min(day), max(day) from data_about_coins where coin_id=%s",(choice[0],))
    first_day, last_day = db_item.cursor.fetchone()
    start_day = date_from_str(input("Введите дату начала вложения в интервале от {0} до {1}, в формате ГГГГ-ММ-ДД: ".format(first_day, last_day)))
    if start_day < first_day or start_day > last_day:
        exit(-1)
    db_item.cursor.execute("select open from data_about_coins where coin_id=%s and day=%s", (choice[0],str(start_day)))
    open_first_day = db_item.cursor.fetchone()[0]
    coins = summ / open_first_day
    db_item.cursor.execute("select close from data_about_coins where coin_id=%s and day=%s", (choice[0], last_day))
    close_last_day = db_item.cursor.fetchone()[0]
    print("\nПрибыль/убыток на момент {0} составляет = {1}".format(last_day,close_last_day * coins - summ))
    db_item.cursor.execute("""select day, low from data_about_coins 
    where coin_id=%s and 
    day>=%s and 
    low=(select min(low) from data_about_coins where coin_id=%s and day>=%s)""", (choice[0], start_day,choice[0], start_day))
    day_of_low, low = db_item.cursor.fetchone()
    print("Максимальная просадка случилась {0} и составила = {1}".format(day_of_low, low * coins - summ))
    db_item.cursor.execute("""select day, high from data_about_coins
     where coin_id=%s 
     and day>=%s and 
     high=(select max(high) from data_about_coins where coin_id=%s and day>=%s)""",(choice[0], start_day, choice[0], start_day))
    day_of_high, high = db_item.cursor.fetchone()
    print("Максимальная прибыль случилась {0} и составила = {1}".format(day_of_high, high * coins - summ))


#Преобразует тип str в тип date при соответствии маски
def date_from_str(_date: 'str') -> date:
    search = re.search(r"\d\d\d\d-\d\d-\d\d",_date)
    if search == None:
        exit(-1)
    else:
        split_date = search.group().split('-')
        return date(int(split_date[0]),int(split_date[1]),int(split_date[2]))


#Сравнение 2х криптовалют в определенный промежуток времени
def compare_pair(db_item : 'db') -> None:
    print("Быберите первую валюту из списка (Пустая строка для продолжения списка):")
    choice1 = choice_coin(db_item)
    if choice1[3] == None or choice1[3] != date.today():
        data_of_coin = Parser.parser().get_data_about_coin(choice1[2])
        insert_data_about_coin(db_item, choice1[1], data_of_coin)
    print("Быберите вторую валюту из списка (Пустая строка для продолжения списка):")
    choice2 = choice_coin(db_item)
    if choice2[3] == None or choice2[3] != date.today():
        data_of_coin = Parser.parser().get_data_about_coin(choice2[2])
        insert_data_about_coin(db_item, choice2[1], data_of_coin)
    db_item.cursor.execute("select min(day), max(day) from data_about_coins where coin_id in (%s, %s)", (choice1[0], choice2[0]))
    left, right = db_item.cursor.fetchone()
    print("Выберите диапазон времени от {0} до {1} в формате ГГГГ-ММ-ДД (через пробел): ".format(left, right))
    dates = input().split(' ')
    left_interval = date_from_str(dates[0])
    right_interval = date_from_str(dates[1])
    if left_interval > right_interval:
        bufer = right_interval
        right_interval = left_interval
        left_interval = bufer
    if left_interval < left or left_interval > right or right_interval < left or right_interval > right:
        exit(-1)
    days = (right_interval - left_interval).days

    db_item.cursor.execute("""select
         max((select max(high) from data_about_coins where day >= _data.day and coin_id = %(ch)s) - low),
         max(high - (select min(low) from data_about_coins where day >= _data.day and coin_id = %(ch)s)),
         max(market_cap),
         (sum(%(ratio)s*(high - low)*(high - low)) + sum(%(ratio)s*(high-low)) + sum(%(ratio)s*(high-low)))
         from data_about_coins as _data 
         where day>=%(l)s and day<=%(r)s and coin_id=%(ch)s""", {'ch': choice1[0], 'l': left_interval, 'r':right_interval, 'ratio': 1/(days + 1)})
    max_incom1, max_loss1, max_market_cap1, disp1 = db_item.cursor.fetchone()

    print("{0}\nМаксимальная прибыль = {1}\tМаксимальная просадка = {2}\nНаибольший объем торгов = {3}\tДисперсия = {4}\n".format(choice1[1],max_incom1,max_loss1, max_market_cap1, disp1))

    db_item.cursor.execute("""select
             max((select max(high) from data_about_coins where day >= _data.day and coin_id = %(ch)s) - low),
             max(high - (select min(low) from data_about_coins where day >= _data.day and coin_id = %(ch)s)),
             max(market_cap),
             (sum(%(ratio)s*(high - low)*(high - low)) + sum(%(ratio)s*(high-low)) + sum(%(ratio)s*(high-low)))
             from data_about_coins as _data 
             where day>=%(l)s and day<=%(r)s and coin_id=%(ch)s""",{'ch': choice2[0], 'l': left_interval, 'r': right_interval, 'ratio': 1/(days + 1)})
    max_incom2, max_loss2, max_market_cap2, disp2 = db_item.cursor.fetchone()

    print(
        "{0}\nМаксимальная прибыль = {1}\tМаксимальная просадка = {2}\nНаибольший объем торгов = {3}\tДисперсия = {4}\n".format(
            choice2[1], max_incom2, max_loss2, max_market_cap2, disp2))


if __name__ == '__main__':
    main()