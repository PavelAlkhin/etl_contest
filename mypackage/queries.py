"""Source queries"""
SRC_QUERY_DATE_FILTER = """
    SELECT 
        t.id as id, 
        t.dt as dt, 
        t.idoper as idoper, 
        t.move as move, 
        t.amount as amount, 
        ot.name as name_oper 
    FROM transactions t
        JOIN operation_types ot ON t.idoper = ot.id
    WHERE dt >= %s AND dt < %s
"""

SRC_QUERY_DATES_ASC = """
    SELECT
        dt 
    FROM transactions
    ORDER BY dt ASC"""

SRC_QUERY_DATES_DESC = """
    SELECT
        dt 
    FROM transactions
    ORDER BY dt DESC"""

"""Destionation queries"""
DST_FOUND_LAST_DATE = """
    SELECT 
        dt  
    FROM transactions_denormalized ORDER BY dt DESC
"""

DST_QUERY_INSERT = """
    INSERT INTO transactions_denormalized
        (id, dt, idoper, move, amount, name_oper) 
    VALUES (%s, %s, %s, %s, %s, %s)
"""

DST_QUERY_UPADTE = """
    UPDATE transactions_denormalized
    SET dt = %s, idoper = %s, move = %s, amount = %s, name_oper = %s 
    WHERE id = %s
"""

DST_QUERY_FIND_BY_ID = """
    SELECT id 
    FROM transactions_denormalized t 
    where id = %s
"""

DST_QUERY_ALL = """
    SELECT * 
    FROM transactions_denormalized
"""

DST_COUNT_ROWS = """
    SELECT 
        COUNT(*) AS total 
    FROM transactions_denormalized
"""
