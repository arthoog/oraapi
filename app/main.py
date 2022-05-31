from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.mijn import mijn
import app.api.db as db

app = FastAPI(openapi_url='/api/v1/openapi.json',
              docs_url='/api/v1/docs')

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)

# forms = None


@app.on_event("startup")
def startup():
    db.init_db()
    db.start_pool()
    db.fetch_forms()
    # global forms
    # forms = db.fetch_forms()
    # print("startup", forms)


app.include_router(mijn, prefix='/api/v1')
