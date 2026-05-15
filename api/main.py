from fastapi import FastAPI

app = FastAPI(title="Job Queue Engine")


@app.get("/health")
def health():
    return {"status": "ok"}
