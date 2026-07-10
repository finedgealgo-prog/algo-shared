"""
add_admin_user.py
-----------------
One-shot script to insert the admin user into MongoDB user_details.

Run from the shared/ directory (or any directory with features/ on the path):
    python3 add_admin_user.py

The admin user's mobile "0123456789" is stored as-is — it intentionally
bypasses normalize_mobile so it can't be registered via the public API.
authenticate_user now does a raw lookup first so login still works.
"""

import os
import sys
from datetime import datetime, timezone

import bcrypt
from pymongo import MongoClient

MONGO_URI   = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME     = os.getenv("MONGO_DB",  "stock_data")
COLLECTION  = "user_details"

ADMIN = {
    "mobile":       "0123456789",
    "country_code": "+91",
    "name":         "Admin",
    "email":        "finedgealgo@gmail.com",
    "password":     "finedgealgo",
    "is_admin":     True,
    "is_active":    True,
}


def main():
    client = MongoClient(MONGO_URI)
    db     = client[DB_NAME]
    col    = db[COLLECTION]

    existing = col.find_one({"email": ADMIN["email"]})
    if existing:
        print(f"Admin user already exists (id={existing['_id']}). Nothing inserted.")
        sys.exit(0)

    password_hash = bcrypt.hashpw(
        ADMIN["password"].encode("utf-8"), bcrypt.gensalt()
    ).decode("utf-8")

    doc = {
        "mobile":       ADMIN["mobile"],
        "country_code": ADMIN["country_code"],
        "name":         ADMIN["name"],
        "email":        ADMIN["email"],
        "password":     password_hash,
        "referral_code": None,
        "is_admin":     ADMIN["is_admin"],
        "is_active":    ADMIN["is_active"],
        "created_at":   datetime.now(timezone.utc).isoformat(),
    }

    result = col.insert_one(doc)
    print(f"Admin user inserted successfully (id={result.inserted_id})")
    print(f"  Mobile : {ADMIN['mobile']}")
    print(f"  Email  : {ADMIN['email']}")
    print(f"  Password: {ADMIN['password']}")


if __name__ == "__main__":
    main()
