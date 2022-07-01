import pytest

from mypackage import app, sql_service
from mypackage import params
from mypackage import queries as q

"""Проверяем что есть конфигурационный файл и он читается"""


def test_load_params():
    param = params.load_params()
    assert param.__len__() == 2


"""Проверяем что подключемся к базе"""


def test_get_connection_src(databases_config):
    conn_src, cur_src = app.get_connection_src(databases_config['mysql_src'])
    conn_dst, cur_dst = app.get_connection_src(databases_config['mysql_dst'])

    assert conn_src is not None
    assert cur_src is not None
    assert conn_dst is not None
    assert cur_dst is not None


"""Проверяем, что запрос в источник возвратил записи"""


def test_sql_service_execute_rows(databases_config):
    conn_src, cur_src = app.get_connection_src(databases_config['mysql_src'])
    last_dst_date = sql_service.execute_get_rows(cur_src, q.SRC_QUERY_DATES_ASC, (), limit=1)
    assert last_dst_date.__len__() > 0


"""Проверяем вставку строки с id=40000"""


def test_sql_service_insert_rows(get_conn_curr_dst, get_query_dict, get_test_data_dict, mysql_delete_id_40000):
    total_data_befor = sql_service.execute_get_rows(get_conn_curr_dst[1], q.DST_COUNT_ROWS, ())
    assert total_data_befor.__len__() > 0

    total_befor = total_data_befor[0]['total']

    sql_service.execute_insert_rows(get_conn_curr_dst[0], get_conn_curr_dst[1], get_query_dict, get_test_data_dict)
    total_data_after = sql_service.execute_get_rows(get_conn_curr_dst[1], q.DST_COUNT_ROWS, ())

    assert total_data_after.__len__() > 0

    total_afer = total_data_after[0]['total']
    assert (total_afer - total_befor) == 1


"""Пытаемся вставить запись с id=1. 
Если в базу уже что-то лили, то должен быть эксепш, так как такой ID=1 уже дожен быть в базе назначения"""


def test_must_eraise_with_id(get_conn_curr_dst, get_query_dict, get_test_data_dict_for_excep):
    with pytest.raises(Exception):
        sql_service.execute_insert_rows(get_conn_curr_dst[1], get_conn_curr_dst[1], get_query_dict,
                                        get_test_data_dict_for_excep)
