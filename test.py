import sqlparse

def expr(expression: str):
    parsed = sqlparse.parse(expression)[0]

query = "select 2+foo"


from collections import deque
from sqlparse.sql import TokenList

class SQLTokenVisitor:
    def visit(self, token):
        """Visit a token."""
        method = 'visit_' + type(token).__name__
        if not token.is_whitespace:
            visitor = getattr(self, method, self.generic_visit)
            return visitor(token)

    def generic_visit(self, token):
        """Called if no explicit visitor function exists for a node."""
        if not isinstance(token, TokenList):
            return
        for tok in token:
            self.visit(tok)

def walk_tokens(token):
    queue = deque([token])
    while queue:
        token = queue.popleft()
        if isinstance(token, TokenList):
            queue.extend(token)
        yield token
class CaseVisitor(SQLTokenVisitor):
    """Build a list of SQL Case nodes

      The .cases list is a list of (condition, value) tuples per CASE statement

    """
    def __init__(self):
        self.cases = []

    def visit_Case(self, token):
        self.cases.append(token.get_cases(skip_ws=True))

class FunctionVisitor(SQLTokenVisitor):
    def __init__(self):
        self.functions = []

    def visit_Function(self, token):
        self.functions.append(token)

statement, = sqlparse.parse(query)
visitor = CaseVisitor()
visitor.visit(statement)
cases = visitor.cases

visitor = FunctionVisitor()
visitor.visit(statement)
funcs = visitor.functions
print(funcs)

def get_type(token):
    if type(token) == sqlparse.sql.Case:
        return "Case"
    elif type(token) == sqlparse.sql.Function:
        return "Function"
    elif type(token) == sqlparse.sql.Operation:
        return "Operation"
