from typing import List
from fastapi import APIRouter, Request, FastAPI

import app.api.db as db
# from app.api.models import Invoice

mijn = APIRouter()
glob = None
# forms = []

@mijn.get('/')
def root():
    return 'API is running'

@mijn.get('/glob')
def test():
    global glob
    return f'dit is test met glob = {glob}'

#----- def ------------------------------
@mijn.get('/forms')
def get_forms():
    # global forms
    # return f'forms = {db.forms}'
    return db.forms

@mijn.get('/formrows/{formname}')
def formrows(formname: str):
    return db.get_formrows(formname)

# ----- form -------------------------
@mijn.get('/f/{formnaam}/')
def get_tabrows(formnaam: str, order: str = None, skip: int = 0, limit: int = 20):
    form = db.forms[formnaam] # via functie doen? db.get_form('trd')
    print('form', form)
    tab = form['tabel_naam']
    return db.get_rows_page(tab, order, skip, limit)

@mijn.get('/f/{formnaam}/{id}')
def get_tabrow(formnaam: str, id: int):
    form = db.forms[formnaam] # via functie doen? db.get_form('trd')
    print('form', form)
    tab = form['tabel_naam']
    return db.get_row(tab, id)

@mijn.put('/f/trd/{id}')
# def get_all_invoices(invo_nr: int, invo: str = Body(...)):
# def put_invoice(invo_nr: int, Body(...)):
async def put_trd(id: int, request: Request):
    body = await request.json()
    #zoek tabelnaam bij formnaam
    form = db.forms['trd'] # via functie doen? db.get_form('trd')
    print('form', form)
    tab = form['tabel_naam']
    data = db.put_row2(tab, id, body)
    return data

# ------invoice------------------------
# @mijn.get('/all', response_model=List[Invoice])
@mijn.get('/invo/all')
def get_all_invoices():
    # return db_manager.get_all_invoices()
    data = db.fetch_all_invoices()
    return data

@mijn.put('/invo/{invo_nr}')
# def get_all_invoices(invo_nr: int, invo: str = Body(...)):
# def put_invoice(invo_nr: int, Body(...)):
async def put_invoice(invo_nr, request: Request):
    body = await request.json()
    data = db.put_invo(invo_nr, body)
    # return body["total"]
    return data

#-------test----------------------------
@mijn.get('/test/all')
def get_all_invoices():
    # return db_manager.get_all_invoices()
    data = db.fetch_all_invoices()
    return data

@mijn.get('/test/{id}')
def test(id: int):
    global glob
    glob = id
    return f'dit is test met id = {id}'

