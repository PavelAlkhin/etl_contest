import datetime
import logging
import pathlib

from pymysql import Error

from mypackage import queries as q
from mypackage import sql_service
from mypackage import params
import pymysql as pm
import pymysql.cursors as pc

DATE_FMT = '%Y.%m.%d %H:%M:%S'
here = pathlib.Path(__file__).resolve()
PATH = here.parents[1]


def timeStamped(fname, t_time, fmt='%Y-%m-%d-%H-%M-%S-{fname}'):
    return t_time.strftime(fmt).format(fname=fname)


def _config_logging():
    _time = datetime.datetime.now()
    ff_name = timeStamped('migration.log', t_time=_time)
    file_name_log = f'{PATH}\\{ff_name}'
    logging.basicConfig(
        filename=file_name_log,
        level=logging.DEBUG,
        format='[%(asctime)s] %(levelname).1s %(message)s',
        datefmt=DATE_FMT)


def get_date_plus_hour(dt_from):
    return dt_from + datetime.timedelta(hours=1)


def get_connection_src(mysql_cred):
    try:
        conn = pm.connect(**mysql_cred)
        cur = conn.cursor(pc.DictCursor)

    except Error as error:
        logging.error(f' Connection error. {error}')

    return conn, cur


def upload_data_to_dst_by_batches(dt_from, dt_till):
    count_batches = 0
    while last_date_src > dt_till:
        in_range = False
        for date_src in all_src_dates_data:
            if date_src['dt'] >= dt_from:
                if date_src['dt'] < dt_till:
                    in_range = True
                    break
        if not in_range:
            dt_from = dt_till + datetime.timedelta(seconds=1)
            dt_till = get_date_plus_hour(dt_from)
            continue

        """GET ALL DATA BY DATE FILTER"""
        data = sql_service.execute_get_rows(cur_src, q.SRC_QUERY_DATE_FILTER, (dt_from, dt_till))

        """Insert data to dst base"""
        query_dict = {'find_by_id': q.DST_QUERY_FIND_BY_ID,
                      'insert': q.DST_QUERY_INSERT,
                      'update': q.DST_QUERY_UPADTE}
        sql_service.execute_insert_rows(conn_dst, cur_dst, query_dict, data)
        count_batches += 1
        logging.info(f'{count_batches} batch: rows={data.__len__()}.')

        dt_from = dt_till
        dt_till = get_date_plus_hour(dt_from)

    return count_batches


def close_con_cur():
    cur_dst.close()
    cur_src.close()
    conn_src.close()
    conn_dst.close()


if __name__ == '__main__':

    _config_logging()

    logging.info(f' START migration.')

    """LOAD MYSQL PARAMS"""
    param = params.load_params()

    conn_src, cur_src = get_connection_src(param['mysql_src'])
    conn_dst, cur_dst = get_connection_src(param['mysql_dst'])

    """Get last date in dst base. Берем последнюю дату из назначения"""
    last_dst_date = sql_service.execute_get_rows(cur_dst, q.DST_FOUND_LAST_DATE, (), limit=1)

    """Get tuple with all dates in source. Вытаскиваем только даты из всей таблицы источника. 
    Так как могут быть большие пробелы в данных"""
    all_src_dates_data = sql_service.execute_get_rows(cur_src, q.SRC_QUERY_DATES_ASC, (), limit=0)

    """Last SRC date. Последняя дата в Источнике, для сравнения в цикле. 
    Прибавляем час, так как нам нужна последняя дата из источника."""
    last_src_date_data = sql_service.execute_get_rows(cur_src, q.SRC_QUERY_DATES_DESC, (), limit=1)
    if last_src_date_data.__len__() == 0:
        logging.error('No data in transactions table.')
        raise Exception('No data in transactions table')

    last_date_src = last_src_date_data[0]['dt']

    last_date_src = get_date_plus_hour(last_date_src)

    if last_dst_date.__len__() == 0:
        first_date_src = sql_service.execute_get_rows(cur_src, q.SRC_QUERY_DATES_ASC, (), limit=1)
        dt_from = first_date_src[0]['dt']
    else:
        dt_from = last_dst_date[0]['dt']

    dt_till = get_date_plus_hour(dt_from)

    """Пройдемся циклом по датам"""
    counts_batches = upload_data_to_dst_by_batches(dt_from, dt_till)

    data = sql_service.execute_get_rows(cur_dst, q.DST_QUERY_ALL, ())

    close_con_cur()

    logging.info(f' END migration.')

    for row in data:
        print(f'Total Rows in DST {row}')
