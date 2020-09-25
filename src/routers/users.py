from fastapi import APIRouter
from ..models.users import UserIn

router = APIRouter()

@router.post('/login')
async def user_login(user: UserIn)