import psycopg2
from psycopg2 import sql


class db:

    __user = 'Dmitry'
    __host = 'localhost'
    __password = 'Lenix1234'
    __port = '5432'
    __dbname = 'postgres'
    __cursor = None
    __conn = None


    #создает подключение к БД
    def __init__(self, autocommit = False):
        self.__conn = psycopg2.connect(user = self.__user, password = self.__password, host = self.__host, port = self.__port, dbname = self.__dbname)
        self.__conn.autocommit = autocommit
        self.__cursor = self.__conn.cursor()


    #Заканчивает работу, очищая переменные
    def end_session(self):
        if not self.__cursor is None:
            self.__cursor.close()
        if not self.__conn is None:
            self.__conn.close()


    #Геттер
    @property
    def cursor(self):
        return self.__cursor

    #Геттер
    @property
    def conn(self):
        return self.__conn


    #Создает новую БД с определенным именем и подключается к ней
    def create_new_db(self, name: 'str' = 'default_name_db') -> None:
        fl = self.__conn.autocommit
        self.__conn.autocommit = True
        try:
            self.__cursor.execute("create database {} with template=template0".format(name))
        except psycopg2.ProgrammingError as e:
            print("База данных с таким именем существует. Производится подключеие...")
        self.__dbname = name
        self.__init__()
        self.__conn.autocommit = fl