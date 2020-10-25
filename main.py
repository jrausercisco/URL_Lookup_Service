from typing import List, Optional

import databases
import sqlalchemy
from sqlalchemy import select
from sqlalchemy import delete
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



DATABASE_URL = "sqlite:///./test.db"

database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()


category_mapping_table = sqlalchemy.Table(
    "category_mapping",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("url", sqlalchemy.String),
    sqlalchemy.Column("category", sqlalchemy.String),
)

pending_resolved_table = sqlalchemy.Table(
    "pending_resolved",
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

@app.get("/url_category_mapping/")
async def read_category_mapping():
    query = category_mapping_table.select()
    return await database.fetch_all(query)

@app.get("/pending_resolved_url_category_mapping/")
async def read_pending_resolved():
    query = pending_resolved_table.select()
    return await database.fetch_all(query)

@app.get("/url_query/")
async def url_query(url_str: str):
    query = select([category_mapping_table]).where(category_mapping_table.c.url == url_str)
    result = await database.fetch_one(query)
    if result is None:
        pending_query = select([pending_resolved_table]).where(pending_resolved_table.c.url == url_str)
        pending_result = await database.fetch_one(pending_query)
        if pending_result is None:
            inserted = pending_resolved_table.insert().values(url=url_str)
            last_record_id = await database.execute(inserted)
    return result

@app.post("/url_category_mapping/")
async def create_category_mapping_submit(bulk: List[URLItem]):
    last_record_id = -1
    for item in bulk:
        does_exist = select([category_mapping_table]).where(category_mapping_table.c.url == item.url)
        result_exist = await database.fetch_one(does_exist)
        if result_exist is None:
            query = category_mapping_table.insert().values(url=item.url, category=item.category)
            last_record_id = await database.execute(query)
            is_pending = select([pending_resolved_table]).where(pending_resolved_table.c.url == item.url)
            result_for_pending = await database.fetch_one(is_pending)
            if result_for_pending is not None:
                to_delete = pending_resolved_table.delete().where(pending_resolved_table.c.url == item.url)
                result_for_delete = await database.fetch_one(to_delete)
    return {"id": last_record_id}


