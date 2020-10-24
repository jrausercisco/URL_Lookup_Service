from typing import List, Optional

import databases
import sqlalchemy
from fastapi import FastAPI
from pydantic import BaseModel

# Provide bulk request table, pending resolve table, stats table, categories table
#
# bulk request table: stores bulk requests that can be used to get categories of  
# large amounts of urls and to trigger resolving categories of urls with unknwon
# categories
#
# pending resolve table: stores urls that have unknown category that need to have
# category resolved
#
# stats table: stores requests and submissions for urls from devices
#
# categories table: stores mapping of urls to categories



# SQLAlchemy specific code, as with any other app
DATABASE_URL = "sqlite:///./test.db"
# DATABASE_URL = "postgresql://user:password@postgresserver/db"

database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()

bulk_requests_table = sqlalchemy.Table(
    "bulk_requests",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("url_requests", sqlalchemy.String),
    sqlalchemy.Column("all_resolved", sqlalchemy.Boolean),
)

categories_table = sqlalchemy.Table(
    "categories",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("url", sqlalchemy.String),
    sqlalchemy.Column("category", sqlalchemy.String),
)


engine = sqlalchemy.create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)
metadata.create_all(engine)


class URLItem(BaseModel):
    url: str
    category: str

class BulkQuery(BaseModel):
    uuid: str
    query: List[URLItem]
    all_resolved: bool


app = FastAPI()


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


@app.get("/bulk_requests/")
async def read_bulk_requests():
    query = bulk_requests_table.select()
    return await database.fetch_all(query)


@app.post("/bulk_requests/")
async def create_bulk_request(bulk: List[str]):
    str1 = " "
    query = bulk_requests_table.insert().values(url_requests=str1.join(bulk), all_resolved=False)
    last_record_id = await database.execute(query)
    return {"id": last_record_id}

@app.get("/categories/")
async def read_categories():
    query = categories_table.select()
    return await database.fetch_all(query)


@app.post("/categories/")
async def create_category_submit(bulk: List[URLItem]):
    for item in bulk:
        query = categories_table.insert().values(url=item.url, category=item.category)
        last_record_id = await database.execute(query)
    return {"id": last_record_id}


