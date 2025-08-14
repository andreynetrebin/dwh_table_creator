import logging
import os
import uuid
from flask import Flask, request, render_template, redirect, url_for, flash
import psycopg2
import pandas as pd
import configparser
from webhdfs_client.client import WebHDFSClient

# Создание папки для логов, если она не существует
if not os.path.exists('logs'):
    os.makedirs('logs')

# Настройка логирования
logging.basicConfig(
    filename='logs/app.log',  # Путь к файлу логов
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Чтение конфигурации
config = configparser.ConfigParser()
config.read('config.ini')

app = Flask(__name__)
app.secret_key = config['flask']['secret_key']  # Замените на ваш секретный ключ
ip_address = config['flask']['ip_address']

# Параметры подключения к базе данных
DB_PARAMS = {
    'dbname': config['database']['dbname'],
    'user': config['database']['user'],
    'password': config['database']['password'],
    'host': config['database']['host'],
    'port': config['database']['port']
}

# Имя схемы для внешней и внутренней таблиц
SCHEMA_EXTERNAL = config['database']['schema_external']
SCHEMA_INTERNAL = config['database']['schema_internal']


# Функция для инициализации клиента WebHDFS
def init_webhdfs_client():
    primary_url = config['hdfs']['primary_url']
    standby_url = config['hdfs']['standby_url']
    username = config['hdfs']['username']

    try:
        client = WebHDFSClient(base_url=primary_url, username=username)
        client.list_status('/')  # Пробный запрос для проверки доступности
        return client
    except Exception as e:
        logger.error(f"Primary HDFS недоступен: {e}. Пытаемся подключиться к standby.")
        try:
            client = WebHDFSClient(base_url=standby_url, username=username)
            client.list_status('/')  # Пробный запрос для проверки доступности
            return client
        except Exception as e:
            logger.error(f"Standby HDFS недоступен: {e}. Не удалось подключиться к HDFS.")
            return None


# Инициализация клиента WebHDFS
webhdfs_client = init_webhdfs_client()
if webhdfs_client is None:
    logger.critical("Не удалось подключиться к HDFS (ни primary, ни standby).")
    raise Exception("Не удалось подключиться к HDFS (ни primary, ни standby).")


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        logger.info(f"Ключи в request.files: {request.files.keys()}")  # Логирование ключей

        if 'csv_file' not in request.files:
            flash("Файл не был загружен.", "danger")
            logger.warning("Попытка загрузки без файла.")
            return redirect(url_for('index'))

        local_csv_path = request.files['csv_file']

        # Проверка, что файл не пустой
        if local_csv_path.filename == '':
            flash("Выберите файл для загрузки.", "danger")
            logger.warning("Попытка загрузки пустого файла.")
            return redirect(url_for('index'))

        # Создание директории temp, если она не существует
        temp_dir = 'temp'
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)

        # Сохранение файла локально в директорию temp для чтения заголовков
        local_file_path = os.path.join(temp_dir, local_csv_path.filename)
        local_csv_path.save(local_file_path)

        # Чтение CSV файла и удаление первой строки
        try:
            df = pd.read_csv(local_file_path)  # Читаем весь файл
            headers = df.columns.tolist()
            df.to_csv(local_file_path, index=False, header=False)  # Сохраняем обратно без первой строки
            logger.info(f"Заголовки успешно прочитаны: {headers}")
        except Exception as e:
            flash(f"Ошибка при чтении CSV файла: {str(e)}", "danger")
            logger.error(f"Ошибка при чтении CSV файла: {str(e)}")
            return redirect(url_for('index'))

        # Создание уникальной директории на HDFS с заданным префиксом
        hdfs_directory = f'/projects/vlgmic_{uuid.uuid4()}'  # Укажите путь к директории на HDFS
        webhdfs_client.mkdirs(hdfs_directory)  # Создание директории на HDFS

        # Загружаем файл на HDFS
        hdfs_file_path = f"{hdfs_directory}/{local_csv_path.filename}"

        try:
            data_node_url = webhdfs_client.upload_file(local_file_path, hdfs_file_path)
            if data_node_url:
                webhdfs_client.write_file(data_node_url, local_file_path)
                logger.info(f"Файл {local_csv_path.filename} успешно загружен на HDFS в {hdfs_file_path}.")
        except Exception as e:
            flash(f"Ошибка при загрузке файла на HDFS: {str(e)}", "danger")
            logger.error(f"Ошибка при загрузке файла на HDFS: {str(e)}")
            # Удаление директории
            webhdfs_client.delete_file(hdfs_directory, recursive=True)
            logger.info(f"Удаление директории: {hdfs_directory}")
            return redirect(url_for('index'))

        # Удаление локального файла после загрузки на HDFS
        os.remove(local_file_path)

        # Перенаправление на страницу сопоставления заголовков
        return render_template('columns_input.html', headers=headers, hdfs_directory=hdfs_directory,
                               hdfs_file_path=hdfs_file_path)

    # Если метод GET, просто отображаем страницу загрузки
    return render_template('index.html')


@app.route('/columns_input', methods=['POST'])
def columns_input():
    hdfs_file_path = request.form.get('hdfs_file_path')
    hdfs_directory = request.form.get('hdfs_directory')
    table_name = request.form.get('table_name')  # Получаем имя таблицы
    columns_info = request.form.getlist('columns_info')
    types_info = request.form.getlist('types_info')
    delimiter = request.form.get('delimiter')

    # Проверка, что все необходимые данные переданы
    if not hdfs_file_path or not table_name or not columns_info or not types_info or not delimiter:
        flash("Не все данные были переданы.", "danger")
        return redirect(url_for('index'))

    # Подготовка к созданию внешней таблицы
    columns_definition = ', '.join([f"{col} {typ}" for col, typ in zip(columns_info, types_info)])

    # Создание внешней таблицы
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        create_external_table(conn, SCHEMA_EXTERNAL, table_name, hdfs_file_path, columns_definition, delimiter)
        external_table_created = True
    except Exception as e:
        flash(f"Ошибка при создании внешней таблицы: {str(e)}", "danger")
        logger.error(f"Ошибка при создании внешней таблицы: {str(e)}")
        return redirect(url_for('index'))

    # Создание таблицы на основе внешней таблицы
    try:
        create_table_from_external(conn, SCHEMA_INTERNAL, f'{SCHEMA_EXTERNAL}.{table_name}', table_name)
        internal_table_created = True
    except Exception as e:
        flash(f"Ошибка при создании таблицы: {str(e)}", "danger")
        logger.error(f"Ошибка при создании таблицы: {str(e)}")
        return redirect(url_for('index'))

    # Удаление внешней таблицы
    try:
        drop_external_table(conn, f'{SCHEMA_EXTERNAL}.{table_name}')
        external_table_dropped = True
    except Exception as e:
        flash(f"Ошибка при удалении внешней таблицы: {str(e)}", "danger")
        logger.error(f"Ошибка при удалении внешней таблицы: {str(e)}")
        return redirect(url_for('index'))

    webhdfs_client.delete_file(hdfs_directory, recursive=True)
    logger.info(f"Удаление директории: {hdfs_directory}")

    # Перенаправление на страницу успеха с информацией о созданных таблицах
    return redirect(url_for('success',
                            external_table_name=table_name,
                            internal_table_name=table_name))


@app.route('/success')
def success():
    external_table_name = request.args.get('external_table_name')
    internal_table_name = request.args.get('internal_table_name')

    return render_template('success.html',
                           external_table_name=external_table_name,
                           internal_table_name=internal_table_name)


@app.route('/assign_roles')
def assign_roles():
    table_name = request.args.get('table_name')
    # Логика назначения ролей
    return render_template('assign_roles.html', table_name=table_name)


@app.route('/assign_roles_action', methods=['POST'])
def assign_roles_action():
    table_name = request.form.get('table_name')  # Получаем имя таблицы из скрытого поля
    username = request.form.get('username')
    selected_role = request.form.get('role')

    # Логика назначения ролей
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        with conn.cursor() as cursor:
            cursor.execute(f"GRANT {selected_role} ON {SCHEMA_INTERNAL}.{table_name} TO {username};")
            conn.commit()
            logger.info("Назначена роль {selected_role} пользователю {username} на таблицу {table_name}")
        flash("Роль успешно назначена.", "success")
    except Exception as e:
        flash(f"Ошибка при назначении роли: {str(e)}", "danger")
        logger.error(f"Ошибка при назначении роли: {str(e)}")

    return redirect(url_for('index'))  # Перенаправление на страницу успеха или другую страницу


def create_external_table(conn, schema_name: str, table_name: str, hdfs_path: str, columns_definition: str,
                          delimiter: str) -> None:
    with conn.cursor() as cursor:
        try:
            query = f"""
                CREATE EXTERNAL TABLE {schema_name}.{table_name} (
                    {columns_definition}
                )
                LOCATION ('pxf://{hdfs_path}?PROFILE=hdfs:csv&SERVER=adh')
                FORMAT 'CSV' (delimiter=E'{delimiter}');
            """
            logger.info(f"query - {query}")
            print(query)
            cursor.execute(query)
            conn.commit()
            logger.info(f"Внешняя таблица {schema_name}.{table_name} успешно создана.")
        except Exception as e:
            logger.error(f"Ошибка при создании внешней таблицы {schema_name}.{table_name}: {str(e)}")
            raise


def create_table_from_external(conn, schema_name: str, external_table_name: str, new_table_name: str) -> None:
    with conn.cursor() as cursor:
        query = f"""
            CREATE TABLE {schema_name}.{new_table_name} AS
            SELECT * FROM {external_table_name};
        """
        logger.info(f"query - {query}")
        cursor.execute(query)
        conn.commit()


def drop_external_table(conn, table_name: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(f"DROP EXTERNAL TABLE IF EXISTS {table_name};")
        conn.commit()


if __name__ == '__main__':
    app.run(debug=False, host=ip_address)
