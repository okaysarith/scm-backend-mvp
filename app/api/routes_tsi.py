# TSI endpoints

from fastapi import APIRouter

router = APIRouter()

@router.get('/tsi')
def get_tsi():
    return {"tsi": "info"}
