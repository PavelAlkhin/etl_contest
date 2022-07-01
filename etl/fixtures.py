import datetime

import pymysql
import pytest

from mypackage import app
from mypackage.params import load_params
from mypackage import queries as q
import pymysql.cursors as pc



@pytest.fixture(scope="session")
def databases_config():
    return load_params()


@pytest.fixture()
def get_conn_curr_src(databases_config):
    return app.get_connection_src(databases_config['mysql_src'])


@pytest.fixture()
def get_conn_curr_dst(databases_config):
    return app.get_connection_src(databases_config['mysql_dst'])


@pytest.fixture()
def get_query_dict():
    return {'find_by_id': q.DST_QUERY_FIND_BY_ID,
            'insert': q.DST_QUERY_INSERT,
            'update': q.DST_QUERY_UPADTE}


@pytest.fixture()
def get_test_data_dict():
    return [{'id': 40000,
             'dt': datetime.datetime(2020, 1, 1, 0, 0, 0),
             'idoper': 1,
             'move': -1,
             'amount': 100,
             'name_oper': 'test operation'}]


@pytest.fixture()
def get_test_data_dict_for_excep():
    return [{'id': 1,
             'dt': datetime.datetime(2020, 1, 1, 0, 0, 0),
             'idoper': 1,
             'move': -1,
             'amount': 100,
             'name_oper': 'test operation'}]

@pytest.fixture()
def query_for_delete_id_40000():
    return """
    DELETE FROM transactions_denormalized WHERE id=40000
    """

@pytest.fixture()
def mysql_delete_id_40000(get_conn_curr_dst, query_for_delete_id_40000):
    get_conn_curr_dst[1].execute(query_for_delete_id_40000)
