from db import connect, init_schema, seed_discount_codes_from_csv

if __name__ == "__main__":
    conn = connect()
    init_schema(conn)
    seed_discount_codes_from_csv(conn)
    conn.commit()
    conn.close()
    print("OK: schema created")
