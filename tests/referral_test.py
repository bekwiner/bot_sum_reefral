import os
import sys
import asyncio

# Ensure imports resolve when running from tests/ dir
ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, ROOT)

# Ensure we run in repo folder
from database import DB_NAME

# Remove existing DB for clean test
if os.path.exists(DB_NAME):
    os.remove(DB_NAME)

from database import (
    init_db, add_user, get_user, create_referral, mark_referral_verified,
    count_verified_referrals, add_almaz, get_ref_by
)
from main import get_referral_reward

async def run_test():
    await init_db()

    inviter = 1
    invited = 2

    await add_user(inviter, 'inviter', None)
    await add_user(invited, 'invited', inviter)

    print('ref_by for invited:', await get_ref_by(invited))

    await create_referral(inviter, invited)

    print('verified refs before:', await count_verified_referrals(inviter))

    did = await mark_referral_verified(invited)
    print('mark_referral_verified first returned:', did)

    if did:
        reward = await get_referral_reward()
        await add_almaz(inviter, reward)
        user = await get_user(inviter)
        print('inviter almaz after reward:', user[3])

    did2 = await mark_referral_verified(invited)
    print('mark_referral_verified second returned:', did2)

    user = await get_user(inviter)
    print('inviter almaz final:', user[3])

asyncio.run(run_test())
