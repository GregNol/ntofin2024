from flask import Flask, jsonify, make_response
import pandas as pd
from sqlite3 import connect
from datetime import datetime

app = Flask(__name__)

con = connect('data.db', check_same_thread=False)
cur = con.cursor()  # Подключаем курсор
cur.execute('''
CREATE TABLE IF NOT EXISTS weather (
Date TEXT PRIMARY KEY,
Temp FLOAT NOT NULL,
date_only TEXT NOT NULL
)
''')  # Создаём таблицу
query = '''SELECT * FROM weather'''
weat = 'INSERT INTO weather (Date, Temp, date_only) values(?, ?, ?)'

if cur.execute('''SELECT COUNT(*) from weather ''').fetchall() == 0:  # Если таблица пустая, загружаем в неё значения
    url = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/daily-min-temperatures.csv"
    df = pd.read_csv(url)
    df['date_only'] = pd.to_datetime(df['Date'])
    for i in df.values.tolist():
        cur.execute(weat, (i[0], i[1], str(i[2])))  # загружаем данные
    con.commit()  # Сохраняем изменения


def get_weather_info(date):  # выгрузка температуры по дате
    quer = f"SELECT Temp FROM weather WHERE Date = '{date}'"
    result = cur.execute(quer).fetchall()
    if result:
        return result[0][0]
    return "No data"


@app.errorhandler(404)  # Если страница не найдена
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


@app.route('/weather/<string:date>', methods=['GET'])  # Температура по дате
def get_weather(date):
    try:
        return jsonify({'Temp': get_weather_info(date)})
    except:
        return jsonify({'error': 'error'})


@app.route('/all', methods=['GET'])
def get_all():
    try:
        return pd.read_sql('''SELECT * FROM weather''', con).to_json()  # Считывает всю таблицу в DataFrame, после
        # переводит его в Json
    except:
        return jsonify({'error': 'error'})


@app.route('/create/<string:date>/<float:temp>', methods=['GET'])  # Создание строки
def create(date, temp):
    try:
        cur.execute(weat, (date, temp, str(datetime.strptime(date, '%Y-%m-%d'))))  # Создание строки по дате и
        # температуре
        con.commit()
        return jsonify({'result': 'successful'})
    except:
        return jsonify({'result': 'error, date is busy'})


@app.route('/delete/<string:date>', methods=['POST'])  # удаление строки из бд по дате
def del_weather(date):
    try:
        delit = f'''DELETE FROM weather WHERE Date = ?'''  # формирование команды для sql
        cur.execute(delit, (date,))  # отправка запроса в бд на удаление
        con.commit()
        return jsonify({'result': 'successful'})
    except:
        return jsonify({'result': 'error'})


@app.route('/middle/<string:date1>/<string:date2>', methods=['GET'])  # медиана и среднее на промежутке
def middle(date1, date2):
    try:
        date1 = datetime.strptime(date1, '%Y-%m-%d')  # начальная граница даты
        date2 = datetime.strptime(date2, '%Y-%m-%d')  # конечная граница даты
        df = pd.read_sql(query, con)  # читаем всю таблицу sql
        df['date_only'] = pd.to_datetime(df['date_only'])  # переводим в datatime
        w = df.loc[(df['date_only'] >= date1) & (df['date_only'] <= date2)]  # берём отрезок из DataFrame, на котором
        # дата входит в промежуток, обозначенный в запросе
        mean = w['Temp'].mean()  # Среднее арифметическое
        median = w['Temp'].median()  # Медиана
        return jsonify({'weather': {'mean': mean, 'median': median}})
    except:
        return jsonify({'error': 'error'})


@app.route('/page/<int:page>', methods=['GET'])  # Вывод "страницы"
def page(page):
    try:
        w = pd.read_sql(query, con).iloc[(page - 1) * 20: page * 20].to_dict()  # Читаем всю бд в DataFrame, берём из
        # него отрезок(страницу), формируем словарь
        return jsonify({'weather': w})
    except:
        return jsonify({'error': 'error'})


if __name__ == '__main__':
    app.run(debug=True)
