import json
import os
from fastapi import FastAPI, Response
from jobspy import scrape_jobs, Site
import redis.asyncio as redis
import hashlib

app = FastAPI()
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0,
    password=os.getenv("REDIS_PASSWORD", "password"),
    decode_responses=True
)

def make_cache_key(params: dict) -> str:
  key = json.dumps(params, sort_keys=True)
  return "jobs:" + hashlib.sha256(key.encode()).hexdigest()

@app.get("/jobs/")
async def get_jobs(
  site_name: str | list[str] | Site | list[Site] | None = None,
  search_term: str | None = None,
  google_search_term: str | None = None,
  location: str | None = None,
  distance: int | None = 50,
  is_remote: bool = False,
  job_type: str | None = None,
  easy_apply: bool | None = None,
  results_wanted: int = 15,
  country_indeed: str = "usa",
  proxies: list[str] | str | None = None,
  ca_cert: str | None = None,
  description_format: str = "markdown",
  linkedin_fetch_description: bool | None = False,
  linkedin_company_ids: list[int] | None = None,
  offset: int | None = 0,
  hours_old: int = None,
  enforce_annual_salary: bool = False,
  verbose: int = 0
): 
  params = {
    "site_name": site_name,
    "search_term": search_term,
    "google_search_term": google_search_term,
    "location": location,
    "distance": distance,
    "is_remote": is_remote,
    "job_type": job_type,
    "easy_apply": easy_apply,
    "results_wanted": results_wanted,
    "country_indeed": country_indeed,
    "proxies": proxies,
    "ca_cert": ca_cert,
    "description_format": description_format,
    "linkedin_fetch_description": linkedin_fetch_description,
    "linkedin_company_ids": linkedin_company_ids,
    "offset": offset,
    "hours_old": hours_old,
    "enforce_annual_salary": enforce_annual_salary,
    "verbose": verbose
  }
  cache_key = make_cache_key(params)
  cached_jobs = await redis_client.get(cache_key)
  if cached_jobs:
    return Response(
      cached_jobs,
      media_type="application/json",
    )
  jobs_df = scrape_jobs(**params)
  jobs_res = jobs_df.to_json(orient="records")
  await redis_client.set(cache_key, jobs_res, ex=3600)
  return Response(
    jobs_res,
    media_type="application/json",
  )