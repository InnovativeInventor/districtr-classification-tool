from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates
from dateutil import parser
import pandas as pd
import pytz
import yaml

app = FastAPI()
templates = Jinja2Templates(directory="templates")

with open("conf.yaml") as f:
    keywords = yaml.load(f)["keywords"]

@app.get("/", response_class=PlainTextResponse)
async def read_root() -> str:
    return PlainTextResponse("Go to /{location} (for all submissions) or /{location}/{start-date}/{end-date} to start. E.g. <a href=\"/michigan\">/michigan</a> or <a href=\"/michigan/2021-5-1/2021-5-7\">/michigan/2021-5-1/2021-5-7</a>")

def nab_written_submission(location: str) -> pd.DataFrame:
    if location == "michigan":
        submissions = pd.read_csv(f"https://o1siz7rw0c.execute-api.us-east-2.amazonaws.com/beta/submissions/csv/{location}")
    else:
        submissions = pd.read_csv(f"https://k61e3cz2ni.execute-api.us-east-2.amazonaws.com/prod/submissions/csv/{location}")

    submissions["datetime"] = submissions["datetime"].apply(lambda x: parser.parse(x.split("(")[0]))

    return submissions[submissions["type"] == "written"].fillna("")

@app.get("/{location}", response_class=HTMLResponse)
async def classify(request: Request, location: str):
    location = location.lower().rstrip()
    written_submissions = nab_written_submission(location)

    print(written_submissions.columns, written_submissions, written_submissions["type"])
    if len(written_submissions):
        return templates.TemplateResponse("location.html",
                                          {
                                              "request": request,
                                              "location": location,
                                              "written_submissions": written_submissions
                                          })
    else:
        raise HTTPException(status_code=404, detail=f"No written submissions found for {location}.")

@app.get("/{location}/{start}/{stop}", response_class=HTMLResponse)
async def classify_filter(request: Request, location: str, start: str, stop: str):
    location = location.lower().rstrip()
    start = parser.parse(start).replace(tzinfo=pytz.UTC)
    stop = parser.parse(stop).replace(tzinfo=pytz.UTC)

    written_submissions = nab_written_submission(location)
    written_submissions_filtered = written_submissions[written_submissions["datetime"].apply(lambda x: start < x and x < stop)]

    print(written_submissions.columns, written_submissions_filtered, written_submissions["type"])
    if len(written_submissions_filtered):
        return templates.TemplateResponse("location.html",
                                          {
                                              "request": request,
                                              "location": location,
                                              "written_submissions": written_submissions_filtered
                                          })
    else:
        raise HTTPException(status_code=404, detail=f"No written submissions found for {location} between {start} and {stop}.")

@app.get("/{location}/submit", response_class=HTMLResponse)
async def submit(request: Request, location: str):
    written_submissions = nab_written_submission(location)

    classifications = dict(request.query_params)

    theory = written_submissions[written_submissions["id"].apply(
        lambda x: str(x) in classifications and classifications[str(x)] == "theory"
    )]
    coi = written_submissions[written_submissions["id"].apply(
        lambda x: str(x) in classifications and classifications[str(x)] == "coi"
    )]

    print(classifications, location, len(theory), len(coi), len(written_submissions))
