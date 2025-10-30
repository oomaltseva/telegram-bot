from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Hello from FastAPI!"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)

# всередині main.py або startup routine
from db import init_db

@app.on_event("startup")
async def on_startup():
    await init_db()
    # інші стартап дії...