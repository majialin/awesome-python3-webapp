#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import orm

from models import User

async def test(loop):
    await orm.create_pool(loop=loop, user='www-data', password='www-data', db='awesome')
    u = User(name='Test', email='test@example.com', passwd='12345678', image='about:blank')
    await u.save()

loop = asyncio.get_event_loop()
loop.run_until_complete(test(loop))