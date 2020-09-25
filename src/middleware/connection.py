from buildpg import asyncpg
import sqlparse
from ..db.driver import Driver


class ConnectionMiddleware:

    def __init__(self, app, sql_file_paths, statement_names, min=5, max=20, schematic=False, connection_cls=None):

        self.app = app
        self.pool = None
        self.min = min
        self.max = max
        self.connection_cls = connection_cls
        self.statements = {}
        for sql_file in sql_file_paths:
            Driver.create_methods(sql_file)

    async def __call__(self, scope, recieve, send):

        if not self.pool:
            if self.connection_cls:
                self.pool = await asyncpg.create_pool_b(dsn='postgres://postgres:FraG2973@localhost:5432/playground', min_size=self.min, max_size=self.max, connection_class=self.connection_cls)
            else:
                self.pool = await asyncpg.create_pool_b(dsn='postgres://postgres:FraG2973@localhost:5432/playground', min_size=self.min, max_size=self.max)

        async with self.pool.acquire() as con:
            print('connection acquired')
            scope['driver'] = Driver(con)
            await self.app(scope, recieve, send)

        print('connection released')
