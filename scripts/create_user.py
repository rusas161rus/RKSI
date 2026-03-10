import argparse
import sys
from pathlib import Path

from werkzeug.security import generate_password_hash

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db import get_main_conn


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--full-name", default="")
    parser.add_argument("--group-id", type=int)
    parser.add_argument("--admin", action="store_true")
    args = parser.parse_args()

    with get_main_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO site_users(username, password_hash, full_name, preferred_group_id)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                (
                    args.username,
                    generate_password_hash(args.password),
                    args.full_name or None,
                    args.group_id,
                ),
            )
            user_id = cur.fetchone()[0]

            if args.admin:
                cur.execute("INSERT INTO site_admins(user_id) VALUES (%s)", (user_id,))

    print(f"Created user id={user_id}, admin={args.admin}")


if __name__ == "__main__":
    main()
