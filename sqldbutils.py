import utils as imutil
import pyodbc

#SQL db setting
#dsn = 'sqlserverdatasource'
#user = 'slam\hwu'
#password = '123321Bb'
#database = 'SQLCRIS_User'
dsn = 'cambdatasource'
user = 'gateuser'
password = 'CPga0109U'
database = 'GateDB_Cris'


def get_db_connection():
    con_string = 'DSN=%s;UID=%s;PWD=%s;DATABASE=%s;' % (dsn, user, password, database)
    cnxn = pyodbc.connect(con_string)
    cursor = cnxn.cursor()
    return {'cnxn': cnxn, 'cursor': cursor}


def release_db_connection(cnn_obj):
    cnn_obj['cnxn'].close()
    #cnn_obj['cursor'].close()
    #cnn_obj['cnxn'].disconnect()


def query_data(query, container):
    conn_dic = get_db_connection()
    conn_dic['cursor'].execute(query)
    rows = conn_dic['cursor'].fetchall()
    columns = [column[0] for column in conn_dic['cursor'].description]
    for row in rows:
        container.append(dict(zip(columns, row)))
    release_db_connection(conn_dic)
