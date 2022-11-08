
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from .crud import Worker
from .security import verify_api_key

router = APIRouter()


@router.get("/ping")
async def ping():
    return 'pong'


@router.post("/reg", dependencies=[Depends(verify_api_key)])
async def reg_worker(detail: str = ''):
    if worker_id := await Worker.add(detail):
        return {'ok': True, 'worker_id': str(worker_id)}
    else:
        return {'ok': False}


@router.post("/{worker_id}")
async def worker_heartbeat(worker_id: str):
    return {
        'ok': await Worker.active(worker_id),
        'interval': 10,
    }


@router.get("/{worker_id}")
async def get_worker_rooms(worker_id: str):
    if not await Worker.active(worker_id):
        return {'ok': False}
    if await Worker.is_checked(worker_id):
        return {
            'ok': True,
            'interval': 20,
        }
    else:
        worker = await Worker.retrieve(worker_id)
        return JSONResponse(
            content={
                'ok': True,
                'interval': 40,
                'rooms': worker['rooms'],
            }
        )
