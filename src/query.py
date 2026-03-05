"""
Tiny helper to run ad-hoc SQL against the DuckDB warehouse.
"""
from __future__ import annotations

import argparse
from .warehouse import connect


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", type=str, default="data/warehouse.duckdb")
    p.add_argument("--sql", type=str, required=True)
    args = p.parse_args()

    con = connect(args.db)
    try:
        res = con.execute(args.sql).fetchdf()
        print(res.to_string(index=False))
    finally:
        con.close()


if __name__ == "__main__":
    main()
