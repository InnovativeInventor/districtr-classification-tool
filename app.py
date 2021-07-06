from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates
from dateutil import parser
import pandas as pd
import pytz
import secrets
import yaml
from dotenv import dotenv_values
import time
import mongoset

config = dotenv_values()
db = mongoset.connect(config["MONGODB"], config["collection"])
logs = db["logs"]

app = FastAPI()
templates = Jinja2Templates(directory="templates")

with open("conf.yaml") as f:
    keywords = yaml.load(f)["keywords"]


@app.get("/", response_class=PlainTextResponse)
async def read_root() -> str:
    return PlainTextResponse(
        'Go to /{location} (for all submissions) or /{location}/{start-date}/{end-date} to start. E.g. <a href="/michigan">/michigan</a> or <a href="/michigan/2021-5-1/2021-5-7">/michigan/2021-5-1/2021-5-7</a>'
    )


def nab_submissions(location: str, start=False, stop=False) -> pd.DataFrame:
    if location == "michigan":
        submissions = pd.read_csv(
            f"https://o1siz7rw0c.execute-api.us-east-2.amazonaws.com/beta/submissions/csv/{location}?length=10000"
        )
    else:
        submissions = pd.read_csv(
            f"https://k61e3cz2ni.execute-api.us-east-2.amazonaws.com/prod/submissions/csv/{location}?length=10000"
        )

    submissions["datetime"] = submissions["datetime"].apply(
        lambda x: parser.parse(x.split("(")[0])
    )

    if start and stop:
        submissions = submissions[
            submissions["datetime"].apply(lambda x: start < x and x < stop)
        ]
    return submissions


def nab_written_submission(location: str, start=False, stop=False) -> pd.DataFrame:
    submissions = nab_submissions(location, start, stop)
    return submissions[submissions["type"] == "written"].fillna("")


@app.get("/{location}", response_class=HTMLResponse)
async def classify(request: Request, location: str):
    location = location.lower().rstrip()
    written_submissions = nab_written_submission(location)

    if len(written_submissions):
        return templates.TemplateResponse(
            "location.html",
            {
                "request": request,
                "location": location,
                "written_submissions": written_submissions,
            },
        )
    else:
        raise HTTPException(
            status_code=404, detail=f"No written submissions found for {location}."
        )


@app.get("/{location}/{start}/{stop}", response_class=HTMLResponse)
async def classify_filter(request: Request, location: str, start: str, stop: str):
    location = location.lower().rstrip()
    start = parser.parse(start).replace(tzinfo=pytz.UTC)
    stop = parser.parse(stop).replace(tzinfo=pytz.UTC)

    written_submissions = nab_written_submission(location, start, stop)

    if len(written_submissions):
        return templates.TemplateResponse(
            "location.html",
            {
                "request": request,
                "location": location,
                "written_submissions": written_submissions,
            },
        )
    else:
        raise HTTPException(
            status_code=404,
            detail=f"No written submissions found for {location} between {start} and {stop}.",
        )


@app.get("/{location}/submit", response_class=HTMLResponse)
async def submit(request: Request, location: str):
    all_submissions = nab_submissions(location)

    classifications = dict(request.query_params)
    data = []
    written_totals = 0
    plan_totals = 0
    coi_totals = 0

    for week_start, submissions in all_submissions.groupby(
        pd.Grouper(key="datetime", freq="W-MON", label="left")
    ):
        written_df = submissions[submissions["type"] == "written"].fillna("")
        written = len(written_df)
        written_both = len(
            submissions[
                submissions["id"].apply(
                    lambda x: str(x) in classifications
                    and classifications[str(x)] == "both"
                )
            ]
        )
        written_theory = len(
            submissions[
                submissions["id"].apply(
                    lambda x: str(x) in classifications
                    and classifications[str(x)] == "theory"
                )
            ]
        ) + written_both
        written_coi = len(
            submissions[
                submissions["id"].apply(
                    lambda x: str(x) in classifications
                    and classifications[str(x)] == "coi"
                )
            ]
        ) + written_both
        written_comments = pd.to_numeric(written_df["numberOfComments"]).sum()

        districts_df = submissions[submissions["type"] == "written"].fillna("")

        plan_df = submissions[submissions["type"] == "plan"].fillna("")
        plan = len(plan_df)
        plan_comments = pd.to_numeric(plan_df["numberOfComments"]).sum()
        cd = len(submissions[submissions["districttype"] == "ush"])
        sd = len(submissions[submissions["districttype"] == "ush"])
        hd = len(submissions[submissions["districttype"] == "ush"])

        coi_df = submissions[submissions["type"] == "coi"].fillna("")
        coi = len(coi_df)
        coi_comments = pd.to_numeric(coi_df["numberOfComments"]).sum()

        written_totals += written
        plan_totals += plan
        coi_totals += coi

        data.append(
            {
                "week_start": week_start,
                "written": written,
                "written_theory": written_theory,
                "written_coi": written_coi,
                "written_comments": written_comments,
                "plan": plan,
                "plan_comments": plan_comments,
                "cd": cd,
                "sd": sd,
                "hd": hd,
                "coi": coi,
                "coi_comments": coi_comments,
            }
        )

    keywords = []
    for k, v in classifications.items():
        if k.endswith("-key") and v.strip():
            sub_id = k.split("-")[0]
            keyword = v.rstrip()
            keywords.append((sub_id, keyword))

    if not logs.insert({"uuid": secrets.token_hex(16), "data": data, "query_params": classifications, "location": location}):
        time.sleep(0.5)
        assert logs.insert({"uuid": secrets.token_hex(16), "data": data, "query_params": classifications, "location": location})

    return templates.TemplateResponse(
        "render.html",
        {
            "request": request,
            "location": location,
            "data": list(enumerate(data, start=1)),
            "keywords": keywords,
            "written_totals": written_totals,
            "plan_totals": plan_totals,
            "coi_totals": coi_totals,
        },
    )
