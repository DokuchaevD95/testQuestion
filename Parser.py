import requests
import re
from DB import db
from datetime import date

class parser:
    __url = "https://coinmarketcap.com" #Страницы
    __months = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10,'Nov': 11, 'Dec': 12} #Словарь с месяцами


    #Парсит страницу сайта, доставая все валюты и ссылки на них, для формирования дальнейшего запроса
    def get_coins_list(self):
        page = requests.get(self.__url + "/all/views/all/").text
        result = list()
        table = re.search('<table.*</table>', page, re.DOTALL)
        if not table:
            raise Exception("Не удалось извлечь таблицу из HTML страницы")
        table = table.group()
        a = re.findall(r'<a class="currency-name-container".+</a>', table)
        if len(a) == 0:
            raise Exception('Не удалось корректно извлечь теги с названием и ссылкой на страницу крипты')
        print("На сайте содержутся данные о " + str(len(a)) + " валютах")
        for item in a:
            name = re.sub(r'<a.+?>', '', item)
            name = re.sub('</a>', '', name)
            cryptoUrl = re.sub(r'.+href="', '', item)
            cryptoUrl = re.sub(r'".+>', '', cryptoUrl)
            result.append([name, cryptoUrl])
        return result


    #Получает на вход определенный url валюты, идет поиск с какого момента доступна информация
    #Затем получает все данные и формирует из них лист
    def get_data_about_coin(self, url):
        data = list()
        url_of_coin = self.__url + url + 'historical-data/'

        page = requests.get(url_of_coin).text

        today = date.today();

        first_time = re.search(r'\'All Time\': \["\d\d-\d\d-\d\d\d\d"', page).group()
        first_time = re.sub(r'\'All Time\': \["', '', first_time)
        first_time = re.sub(r'"', '', first_time)
        first_time = first_time.split('-')
        first_time = first_time[2] + first_time[0] + first_time[1];

        this_time = str(today).split('-')
        this_time = this_time[0] + this_time[1] + this_time[2]

        url_of_coin += f'?start={first_time}&end={this_time}'
        page = requests.get(url_of_coin).text

        page = re.search(r'<tbody.+?</tbody>', page, re.DOTALL).group()
        page = re.findall(r'<tr.+?</tr>', page, re.DOTALL)
        for i in page:
            data_of_day = re.findall(r'<td.*?>.*?</td>', i, re.DOTALL)
            for j in range(0, len(data_of_day)):
                data_of_day[j] = re.sub(r'.*<td.*?>', '', data_of_day[j], re.DOTALL)
                data_of_day[j] = re.sub(r'</td>.*', '', data_of_day[j], re.DOTALL)
                data_of_day[j] = re.sub(r',', '', data_of_day[j], re.DOTALL)
                if j == 0:
                    day = int(re.search(r'\d{2}', data_of_day[j]).group())
                    month = self.__months[re.search(r'[A-Za-z]{3}', data_of_day[j]).group()]
                    year = int(re.search(r'\d{4}', data_of_day[j]).group())
                    data_of_day[j] = date(year, month, day)
                elif j < 5 and j > 0:
                    data_of_day[j] = float(data_of_day[j])
                else:
                    if data_of_day[j] == '-':
                            data_of_day[j] = 0
                    data_of_day[j] = int(data_of_day[j])
            data.append(data_of_day)
        return data