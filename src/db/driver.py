import os
import asyncpg
import re
import sqlparse
from operator import itemgetter
from fastapi.exceptions import HTTPException
from .methods import NORMAL_METHOD, CLASS_RESPONSE_METHOD, IMPORT_CLASS, CLASS_LIST_RESPONSE_METHOD
from src.settings import BASE_DIR
from pydoc import locate


class Driver:

    def __init__(self, con: asyncpg.Connection):
        self.con: asyncpg.Connection = con

    @staticmethod
    def parse_sql(sql):
        db_operations = {'^': 'fetchrow', '!': 'execute',
                         '*': 'fetch', ':': 'fetchval'}
        statement_list = sqlparse.parse(sql)
        last_name = None
        stmts = {}
        for stmt in statement_list:
            name = None
            stmt_type = None
            response_class = None
            comments = [token for token in stmt if type(
                token) is sqlparse.sql.Comment]
            if comments:
                for comment in comments:
                    for single_comment in str(comment).splitlines():
                        name_match = re.match(
                            r'^\s*(\-\-\s*(name):\s*)', single_comment)
                        response_class_match = re.match(
                            r'^\s*(\-\-\s*(class):\s*)', single_comment)
                        if name_match:
                            name_no_padding = single_comment.rstrip(' _')
                            stmt_type = re.findall(
                                r'(\*|\^|!|:)$', name_no_padding)[0]
                            name_parsed_ending = re.sub(
                                r'(\s|\-|_|\^|\*|!|:)*$', '', name_no_padding[name_match.end():])
                            name = re.sub(r'\s|\-', '_', name_parsed_ending)
                        elif response_class_match:
                            response_class = re.sub(
                                r'(\s|\-|_|\^|\*|!|:)*$', '', single_comment[response_class_match.end():])

            formatted_stmt = sqlparse.format(str(stmt), strip_comments=True)
            if name and response_class:
                stmts[name] = {
                    'stmt': formatted_stmt, 'type': db_operations[stmt_type], 'cls': response_class}
                last_name = name
                response_class = None

            elif name:
                stmts[name] = {'stmt': formatted_stmt,
                               'type': db_operations[stmt_type]}
                last_name = name
            else:
                if not last_name:
                    raise TypeError('First SQL statement must have a name')
                stmts[last_name]['stmt'] = stmts[last_name]['stmt'] + \
                    formatted_stmt
        return stmts

    @classmethod
    def create_methods(cls, sql_file):
        import sys
        module_scope = sys.modules[__name__]
        file = open(sql_file).read()
        cls_methods = {}
        response_classes = {}
        cls_statements = cls.parse_sql(file)
        print(cls_statements)
        for method, options in cls_statements.items():
            if hasattr(options, 'cls'):
                cls_components = options['cls'].split('.')
                cls_name = cls_components[-1]
                cls_path = '.'.join(
                    ['src.models', '.'.join(cls_components)])
                exec(IMPORT_CLASS.format(cls_name=cls_name,
                                         cls_path=cls_path), None, response_classes)
                if options['type'] == 'fetch':
                    exec(CLASS_LIST_RESPONSE_METHOD.format(
                        func_name=method, stmt=options['stmt'], operation=options['type'], cls=cls_name), None, cls_methods)
                elif options['type'] == 'fetchrow':
                    exec(CLASS_RESPONSE_METHOD.format(func_name=method,
                                                      stmt=options['stmt'], operation=options['type'], cls=cls_name), None, cls_methods)
                else:
                    exec(NORMAL_METHOD.format(func_name=method,
                                              stmt=options['stmt'], operation=options['type']), None, cls_methods)
            else:
                exec(NORMAL_METHOD.format(
                    func_name=method, stmt=options['stmt'], operation=options['type']), None, cls_methods)

        for name, class_name in response_classes.items():
            setattr(module_scope, name, class_name)
        for name, method in cls_methods.items():
            setattr(cls, name, method)

    async def execute(self, statement, **parameters):
        return await self.con.execute(statement, **parameters)

    async def fetch(self, statement, **parameters):
        result = await self.con.fetch_b(statement, **parameters)
        if not result:
            raise HTTPException(status_code=404)
        return result

    async def fetchrow(self, statement, **parameters):
        result = await self.con.fetchrow_b(statement, **parameters)
        if not result:
            raise HTTPException(status_code=404)
        return result

    async def fetchval(self, statement, **parameters):
        result = await self.con.fetchval_b(statement, **parameters)
        if not result:
            raise HTTPException(status_code=404)
        return result
