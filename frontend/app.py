from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import uvicorn

from frontend.models import get_malicious_accounts, upload_csv_data_to_db, create_fake_malicious_accounts, count_blocked_users_last_week, count_blocked_users_last_month, count_blocked_users_per_month_2023

app = FastAPI(debug=True)
templates = Jinja2Templates(directory="templates")

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def root():
    return {"message": "Hello World"}


@app.get("/view", response_class=HTMLResponse)
def route_view(request: Request):
    malicious_accounts = get_malicious_accounts()

    return templates.TemplateResponse("view.html", {"request": request, "malicious_accounts": malicious_accounts})


@app.get('/upload')
def route_upload_csv():
    res = upload_csv_data_to_db()
    return {
        'message': 'The CSV uploaded.',
        'res': res
    }


@app.get('/create_fakes')
def create_fakes():
    create_fake_malicious_accounts(250)
    return {
        'message': 'Fake at most 250 malicious accounts',
    }


@app.get('/statistics', response_class=HTMLResponse)
def show_malicious_accounts_last_week(request: Request):
    statistics_last_week = count_blocked_users_last_week()

    # Pass the statistics data to the template
    return templates.TemplateResponse(
        "statistics.html",
        {"request": request, "statistics_last_week": statistics_last_week}
    )

@app.get("/blocked-users-data_last_week")
async def get_blocked_users_data() :
    last_week = count_blocked_users_last_week()
    print(last_week)
    return last_week

@app.get("/monthly_data")
async def get_blocked_users_data_last_month() :
    last_month = count_blocked_users_last_month()
    print(last_month)
    return last_month


@app.get("/yearly_data")
async def get_blocked_users_data_last_year() :
    last_year = count_blocked_users_per_month_2023()
    print(last_year)
    return last_year


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8080)
