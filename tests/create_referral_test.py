import os
import sys
import asyncio

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, ROOT)

from database import DB_NAME, init_db, create_referral

async def run():
    try:
        if os.path.exists(DB_NAME):
            os.remove(DB_NAME)
    except:
        pass
    await init_db()
    r1 = await create_referral(1,2)
    r2 = await create_referral(1,2)
    print('first created:', r1)
    print('second created:', r2)

asyncio.run(run())
