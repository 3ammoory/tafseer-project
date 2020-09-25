NORMAL_METHOD = '''
async def {func_name}(self, model=None, **kwargs):
    stmt = """{stmt}"""
    if model:
        results = await self.{operation}(stmt, **model.dict())
        return results
    else:
        results = await self.{operation}(stmt, **kwargs)
        return results
'''

CLASS_RESPONSE_METHOD = '''
async def {func_name}(self, model=None, **kwargs):
    stmt = """{stmt}"""
    if model:
        results = await self.{operation}(stmt, **model.dict())
        return {cls}.construct_f(**results)
    else:
        results = await self.{operation}(stmt, **kwargs)
        return {cls}.construct_f(**results)
'''

CLASS_LIST_RESPONSE_METHOD = '''
async def {func_name}(self, model=None, **kwargs):
    stmt = """{stmt}"""
    if model:
        results = await self.{operation}(stmt, **model.dict())
        return [{cls}.construct_f(**result) for result in results]
    else:
        results = await self.{operation}(stmt, **kwargs)
        return [{cls}.construct_f(**result) for result in results]
'''

IMPORT_CLASS = '''
{cls_name} = locate({cls_path})
'''
