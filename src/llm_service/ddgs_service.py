# Visit http://localhost:8000/docs for documents.

import base64
import copy
import json
import time
from argparse import ArgumentParser
from typing import Dict, List, Literal, Optional, Union
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse
from gradio_client import Client
from duckduckgo_search import DDGS, AsyncDDGS
from fastapi import FastAPI
from fastapi import Body
import uvicorn
import json
from loguru import logger 
import aiohttp
import json

app = FastAPI()
ddgs = AsyncDDGS()

@app.post("/search")
async def get_search_result(data=Body(None)):
    query = data.get('query', '')
    n_result = data.get('n_result', '5')
    n_result = int(n_result)
    results = await ddgs.text(keywords=query,
            max_results=n_result,
            safesearch="off",
            region="cn-zh")
    return results


@app.get("/")
async def check_healthy():
    return "healthy"

async def optimize_prompt(request=Body(None)):
    pass


if __name__ == '__main__':
    uvicorn.run("ddgs_service:app", host="0.0.0.0", port=8082, workers=1)
    