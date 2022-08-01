import random

from fastapi import FastAPI, Request

app = FastAPI()


def generate_response():
    resp = {}
    for i in range(100):
        resp[str(random.randint(1,100000))] = float(f"0.{i}")
    return resp


default_response = generate_response()


@app.post("/")
async def cmd(request: Request):
    print("ooo")
    return default_response
