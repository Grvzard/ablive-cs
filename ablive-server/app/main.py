import asyncio

from fastapi import FastAPI, status
from fastapi.responses import JSONResponse

from . import api_v2
from app.active_checker import active_checker
from app.core.config import settings

app = FastAPI(
    root_path=settings.API_PREFIX,
)

bg_tasks = set()


@app.on_event('startup')
async def on_startup():
    task = asyncio.create_task(active_checker())
    bg_tasks.add(task)


@app.exception_handler(Exception)
async def default_exception_handler(request, exec):
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "ok": False,
        },
    )


app.include_router(api_v2.router)
