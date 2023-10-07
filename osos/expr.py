from sqlglot import parse_one, Expression
from sqlglot.expressions import Identifier, Literal, Column, Add, Sum, Select
from collections.abc import Iterable

from osos.column import Node, AbstractCol, AbstractLit, Func, BinaryOp, UnaryOp


def print_tree(tree: Expression):
    for elem in tree.walk():
        node = elem[0]
        print(node.args)
        print("--" * node.depth, node)


def make_tree(tree: Expression) -> Node:
    for elem in tree.walk():
        node = elem[0]
        if isinstance(node, (Identifier, Column)):
            out = AbstractCol(node.name)
        elif isinstance(node, Literal):
            out = AbstractLit(node.name)


def main():
    stmts = [
        "SELECT SUM(x+3)",
        "SELECT CASE WHEN SUM(avg(x)+2) > 3 THEN 1 ELSE 0 END",
    ]
    # stmts = ["select fo,bar from baz"]
    for stmt in stmts[:1]:
        s = parse_one(stmt)
        print_tree(s)
        # print(select_stmt.parse_string(stmt))
        # print(repr(parse_one(stmt)))

        # print(len(parse_one(stmt)))
        ...
        # print(select_stmt.parse_string(stmt)[1:][0][0])
        # print(stmt, "\n\t", print_tree(walk_tree(select_stmt.parse_string(stmt))))


if __name__ == "__main__":
    main()
