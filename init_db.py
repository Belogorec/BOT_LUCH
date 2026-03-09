from db import connect, init_schema

if __name__ == "__main__":
    conn = connect()
    init_schema(conn)
    conn.commit()
    conn.close()
    print("OK: schema created")
