from contact_schema import run_contact_schema_migrations
from core_schema import run_core_schema_migrations
from db import connect, init_schema, seed_discount_codes_from_csv
from integration_schema import run_integration_schema_migrations

if __name__ == "__main__":
    conn = connect()
    init_schema(conn)
    run_core_schema_migrations(conn)
    run_integration_schema_migrations(conn)
    run_contact_schema_migrations(conn)
    seed_discount_codes_from_csv(conn)
    conn.commit()
    conn.close()
    print("OK: schema created")
