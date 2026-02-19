#!/usr/bin/env python3
from __future__ import annotations

import argparse
import pathlib
import sys

import pymysql
from pymysql.constants import CLIENT


def statements_from_sql(path: pathlib.Path):
    with path.open("r", encoding="utf-8", errors="replace") as f:
        buf = []
        in_single = False
        in_double = False
        in_backtick = False
        in_line_comment = False
        in_block_comment = False
        escape = False
        line_start = True
        prev = ""

        while True:
            ch = f.read(1)
            if not ch:
                break

            if in_line_comment:
                if ch == "\n":
                    in_line_comment = False
                    line_start = True
                prev = ch
                continue

            if in_block_comment:
                if prev == "*" and ch == "/":
                    in_block_comment = False
                prev = ch
                continue

            if not (in_single or in_double or in_backtick):
                if line_start and ch in (" ", "\t", "\r"):
                    prev = ch
                    continue
                if line_start and ch == "#":
                    in_line_comment = True
                    prev = ch
                    continue
                if line_start and ch == "-":
                    nxt = f.read(2)
                    if nxt == "- ":
                        in_line_comment = True
                        prev = ""
                        continue
                    buf.append(ch)
                    buf.extend(list(nxt))
                    line_start = False
                    prev = nxt[-1] if nxt else ch
                    continue
                if ch == "/":
                    nxt = f.read(1)
                    if nxt == "*":
                        third = f.read(1)
                        if third == "!":
                            comment_payload = []
                            p = ""
                            while True:
                                c2 = f.read(1)
                                if not c2:
                                    break
                                if p == "*" and c2 == "/":
                                    if comment_payload:
                                        comment_payload.pop()
                                    break
                                comment_payload.append(c2)
                                p = c2
                            payload = "".join(comment_payload)
                            i = 0
                            while i < len(payload) and payload[i].isdigit():
                                i += 1
                            payload = payload[i:].lstrip()
                            if payload:
                                buf.append(payload)
                            prev = ""
                            line_start = False
                            continue
                        in_block_comment = True
                        prev = third
                        continue
                    buf.append(ch)
                    if nxt:
                        buf.append(nxt)
                        prev = nxt
                    else:
                        prev = ch
                    line_start = False
                    continue

            if ch == "'" and not (in_double or in_backtick) and not escape:
                in_single = not in_single
            elif ch == '"' and not (in_single or in_backtick) and not escape:
                in_double = not in_double
            elif ch == "`" and not (in_single or in_double):
                in_backtick = not in_backtick

            if ch == ";" and not (in_single or in_double or in_backtick):
                stmt = "".join(buf).strip()
                if stmt:
                    yield stmt
                buf = []
                line_start = True
                prev = ch
                escape = False
                continue

            buf.append(ch)
            line_start = ch == "\n"

            if ch == "\\" and (in_single or in_double):
                escape = not escape
            else:
                escape = False

            prev = ch

        tail = "".join(buf).strip()
        if tail:
            yield tail


def import_dump(dump_file: pathlib.Path, target_db: str, host: str, port: int, user: str, password: str) -> int:
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        autocommit=True,
        client_flag=CLIENT.MULTI_STATEMENTS,
        charset="utf8mb4",
    )

    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{target_db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
            )
            cur.execute(f"USE `{target_db}`")

        count = 0
        with conn.cursor() as cur:
            for stmt in statements_from_sql(dump_file):
                cur.execute(stmt)
                count += 1
                if count % 100 == 0:
                    print(f"Executed {count} statements...")
        return count
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Import large .sql dump into target MySQL database.")
    parser.add_argument("dump_file", type=pathlib.Path)
    parser.add_argument("--target-db", default="raw_thai_funds")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3307)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    args = parser.parse_args()

    if not args.dump_file.exists():
        print(f"Dump file not found: {args.dump_file}", file=sys.stderr)
        return 1

    n = import_dump(args.dump_file, args.target_db, args.host, args.port, args.user, args.password)
    print(f"Import done. statements={n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
