FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7

COPY . /app
RUN pip3 install databases aiosqlite
