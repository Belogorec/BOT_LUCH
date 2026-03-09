import random
import string
import csv

from db import connect

CODES_COUNT = 200
DISCOUNT_PERCENT = 15

# УКАЖИ USERNAME БОТА
BOT_USERNAME = "luchbarbot"


def generate_code():
    chars = string.ascii_uppercase + string.digits
    part = ''.join(random.choice(chars) for _ in range(5))
    return f"8M-{part}"


def main():
    conn = connect()

    codes = set()

    while len(codes) < CODES_COUNT:
        codes.add(generate_code())

    codes = list(codes)

    for code in codes:
        conn.execute(
            """
            INSERT INTO discount_codes (code, discount_percent)
            VALUES (?, ?)
            """,
            (code, DISCOUNT_PERCENT),
        )

    conn.commit()
    conn.close()

    with open("discount_qr_codes.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["code", "qr_link"])

        for code in codes:
            link = f"https://t.me/{BOT_USERNAME}?start=promo_{code}"
            writer.writerow([code, link])

    print(f"Generated {len(codes)} codes")
    print("CSV file: discount_qr_codes.csv")


if __name__ == "__main__":
    main()