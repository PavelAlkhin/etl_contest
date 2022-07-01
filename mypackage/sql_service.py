def execute_get_rows(cur, query, query_param, limit=0):
    data = []
    cur.execute(query, query_param)
    rows = cur.fetchall()
    count = 1
    for row in rows:
        data.append(row)
        if limit == count:
            break
        count += 1
    return data


def execute_insert_rows(conn, cur, query_dict, data):
    for row in data:
        id = row['id']
        if founded_by_id_in_dst(cur, query_dict['find_by_id'], id):
            raise Exception(f'Row with id={id} already exist')

        cur.execute(query_dict['insert'], (row['id'],
                                           row['dt'],
                                           row['idoper'],
                                           row['move'],
                                           row['amount'],
                                           row['name_oper'],))
        conn.commit()

        """Не стал удалять, просто комментим"""
        # else:
        #     cur.execute(query_dict['update'], (row['dt'],
        #                                   row['idoper'],
        #                                   row['move'],
        #                                   row['amount'],
        #                                   row['name_oper'],
        #                                   row['id']))

def founded_by_id_in_dst(cur, query, id):
    res = execute_get_rows(cur, query, (id,), limit=1)
    if res.__len__() == 0:
        return False
    else:
        return True
