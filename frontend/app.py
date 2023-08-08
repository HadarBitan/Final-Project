
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn

from frontend.models import get_users


app = FastAPI(debug=True)
templates = Jinja2Templates(directory="templates")

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def root():
    return {"message": "Hello World"}


@app.get("/view", response_class=HTMLResponse)
def route_view(request: Request):
    users = get_users()

    return templates.TemplateResponse("view.html", {"request": request, "users": users})


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8080)
