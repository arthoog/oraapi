# Based on: https://blogs.oracle.com/opal/how-to-use-python-flask-with-oracle-database

# veldtype = next(veldtype for veldtype in veldtypen if veldtype[0].lower() == k)
# form = next(form for form in forms if form['form'] == 'trd')

import cx_Oracle
from app.api import db_config

pool = None

def init_db():
    cx_Oracle.init_oracle_client(lib_dir=db_config.oracle_client)

def init_session(connection, requestedTag_ignored):
    cursor = connection.cursor()
    cursor.execute("""
        ALTER SESSION SET
          TIME_ZONE = 'CET'
          NLS_DATE_FORMAT = 'yyyy-mm-dd"T"hh24:mi:ss'
          NLS_TIMESTAMP_FORMAT = 'yyyy-mm-dd"T"hh24:mi:ss.ff'""") #ff3
    connection.autocommit = True #doet het niet?

def start_pool():

    # Generally a fixed-size pool is recommended, i.e. pool_min=pool_max.
    # Here the pool contains 4 connections, which is fine for 4 conncurrent
    # users.
    #
    # The "get mode" is chosen so that if all connections are already in use, any
    # subsequent acquire() will wait for one to become available.
    #
    # Note there is no explicit 'close cursor' or 'close connection'.  At the
    # end-of-scope when init_session() finishes, the cursor and connection will be
    # closed automatically.  In real apps with a bigger code base, you will want to
    # close each connection as early as possible so another web request can use it.

    pool_min = 4
    pool_max = 4
    pool_inc = 0
    pool_gmd = cx_Oracle.SPOOL_ATTRVAL_WAIT

    username = db_config.user
    password = db_config.pw
    connect_string = db_config.connect_string

    print("Connecting to", connect_string)

    global pool
    pool = cx_Oracle.SessionPool \
        ( user = username
        , password = password
        , dsn = connect_string
        , min = pool_min
        , max = pool_max
        , increment = pool_inc
        , threaded = True
        , getmode = pool_gmd
        , sessionCallback = init_session
        )

# =======================================================

forms = {}

def fetch_forms():
    global forms
    sql = "select * from r_forms order by form"
    with pool.acquire() as connection:
        cursor = connection.cursor()
        cursor.execute(sql)
        cursor.rowfactory = lambda *args: dict(zip([d[0].lower() for d in cursor.description], args))
        formsarray = cursor.fetchall()
        cursor.close()
        #connection.close()

    forms = {value['form']: value for (index,value) in enumerate(formsarray)}

    print(cursor.rowcount, "rows from", sql)
    print("fetch_forms", forms)
    return forms

def get_formrows(formname: str):
    connection = pool.acquire()
    cursor = connection.cursor()
    sql = f"select * from r_formrows where form = '{formname}' order by volgnr"
    cursor.execute(sql)
    cursor.rowfactory = lambda *args: dict(zip([d[0].lower()
                                           for d in cursor.description], args))
    #global formrows
    formrows = cursor.fetchall()
    cursor.close()
    connection.close()
    print(cursor.rowcount, "rows from", sql)
    print("get_formrows", formrows)
    return formrows

# ------------------------------
# get veldtypen voor sqlbuilding update en insert

def get_veldtypen(tabname: str):
    connection = pool.acquire()
    cursor = connection.cursor()
    sql = f"select * from {tabname} where 1=0"
    cursor.execute(sql)
    # (name, type, display_size, internal_size, precision, scale, null_ok)
    d = {value[0].lower(): (value[0], value[1].name[8:], value[1:]) for (index, value) in enumerate(cursor.description)}
    cursor.close()
    connection.close()
    return d

def exec_sql(sql: str):
    with pool.acquire() as connection:
        cursor = connection.cursor()
        # connection.autocommit = True
        cursor.execute(sql)
        connection.commit()
        cursor.close() #nodig??
    return

def exec_sql(sql: str, pars: dict):
    with pool.acquire() as connection:
        cursor = connection.cursor()
        # connection.autocommit = True
        cursor.execute(sql, pars)
        connection.commit()
        cursor.close() #nodig??
    return

# ----- form -------------------------
def get_all_rows(tab: str, order: str):
    # sql = f"select * from {tab} order by id" # ID !!!!!
    sql = f"select * from {tab}"
    if order: #staat nog geen order = '-ts,-xx' toe. splitten en joinen
        if (order[0]=='-'):
            sql += f" order by {order[1:]} desc"
        else:
            sql += f" order by {order}"

    with pool.acquire() as connection:
        cursor = connection.cursor()
        cursor.execute(sql)
        cursor.rowfactory = lambda *args: dict(zip([d[0].lower() for d in cursor.description], args))
        rows = cursor.fetchall()
        cursor.close()
        #connection.close()

    print(cursor.rowcount, "rows from", sql) #kan dit nog?
    print(tab, rows)
    return rows

def get_rows_page(tab: str, order: str, skip: int, limit: int):
    sql = f"select * from {tab}"
    if order: #staat nog geen order = '-ts,-xx' toe. splitten en joinen
        # orders = order.split(',')
        orders = [o.strip() for o in order.split(',')]
        orders = [f"{o[1:].lstrip()} desc" if o[0] =='-' else o for o in orders]
        # order = ', '.join(orders)
        sql += f" order by {', '.join(orders)}"

    with pool.acquire() as connection:
        cursor = connection.cursor(scrollable=(skip != 0))
        cursor.execute(sql)
        if skip != 0:
            #cursor.scrollable =True
            cursor.scroll(value=skip, mode='absolute')
        cursor.rowfactory = lambda *args: dict(zip([d[0].lower() for d in cursor.description], args))
        rows = cursor.fetchmany(numRows=limit)
        cursor.close()
        #connection.close()

    print(cursor.rowcount, "rows from", sql) #kan dit nog?
    print(tab, rows)
    return rows

def get_row(tab: str, id: int):
    connection = pool.acquire()
    cursor = connection.cursor()
    sql = f"select * from {tab} where id = '{id}'"
    cursor.execute(sql)
    cursor.rowfactory = lambda *args: dict(zip([d[0].lower() for d in cursor.description], args))
    row = cursor.fetchall()
    cursor.close()
    connection.close()
    print(cursor.rowcount, "row from", sql)
    print(tab, row)
    return row

def put_row(tab: str, id: int, body):
    veldtypen = get_veldtypen(tab)
    print('veldtypen', veldtypen)
    sql = ''
    for k, v in body.items():
        vt = veldtypen[k][1]
        print ('vt', k, vt)
        if vt == 'VARCHAR':
            sql += f", {k}='{v or ''}'"
        elif vt == 'NUMBER':
            sql += f", {k}={v or 'null'}"
        elif vt =='DATE':
            sql += f", {k}=to_date('{v or ''}', 'yyyy-mm-dd hh24:mi:ss')"

    sql = f"update {tab} set {sql[2:]} where id = {id}"
    print('sql', sql)
    exec_sql(sql)
    return get_row(tab, id)

def put_row2(tab: str, id: int, body):
    veldtypen = get_veldtypen(tab)
    print('veldtypen', veldtypen)
    sql = ''
    pars = {}
    for k, v in body.items():
        vt = veldtypen[k][1]
        print ('vt', k, vt)
        sql += f", {k}=:{k}"
        pars[k] = v
        # if vt == 'VARCHAR':
        #     sql += f", {k}='{v or ''}'"
        # elif vt == 'NUMBER':
        #     sql += f", {k}={v or 'null'}"
        # elif vt =='DATE':
        #     sql += f", {k}=to_date('{v or ''}', 'yyyy-mm-dd hh24:mi:ss')"

    sql = f"update {tab} set {sql[2:]} where id = :whereid"
    pars['whereid'] = id
    print('sql', sql)
    print('pars', pars)
    exec_sql(sql, pars)
    return get_row(tab, id)

#------invoice--------------------------------
def fetch_all_invoices():
    connection = pool.acquire()
    cursor = connection.cursor()
    sql = "select to_char(invoice_date, 'yyyy-mm-dd hh24:mi') as invoice_date, invoice_number, total from invoices where " \
          "total > :total_var order by total"
    total_var = 200
    cursor.execute(sql, [total_var])

    cursor.rowfactory = lambda *args: dict(zip([d[0].lower() for d in cursor.description], args))
    data = {}
    data["data"] = cursor.fetchall()
    data["sql"] = sql

    cursor.close()
    connection.close()
    return data

def put_invo(invo_nr, body):
    # print(body)
    # print(dict(body))

    # sql = "update invoices set invoice_date=:invoice_date, total=:total where invoice_number=:invo_nr"
    # sql = "update invoices set total=:total where invoice_number=:invoice_number"
    # cursor.execute(sql, [body["invoice_date"], body["total"], invo_nr])
    # cursor.execute(sql, body)

    sql = f"update invoices" \
          f"\nset invoice_number={body['invoice_number']}" \
          f"\n,   invoice_date='{body['invoice_date']}'" \
          f"\n,   total={body['total']}" \
          f"\nwhere invoice_number={invo_nr}"
    print('sql:', sql)
    with pool.acquire() as connection:
        cursor = connection.cursor()
        cursor.execute(sql)
        connection.commit()
        sql = "select to_char(invoice_date, 'yyyy-mm-dd hh24:mi:ss') as invoice_date, invoice_number, total from invoices where invoice_number=:invo_nr"
        # ook als invoice_number verandert urlpar gebruiken
        cursor.execute(sql, [invo_nr])
        cursor.rowfactory = lambda *args: dict(zip([d[0].lower() for d in cursor.description], args))
        data = cursor.fetchall()
        cursor.close()
        #connection.close()
    return data
