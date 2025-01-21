import sys
import sqlite3
from pathlib import Path
from datetime import datetime


class Contacts:
    def __init__(self, db_path):
        self.db_path = db_path
        if not db_path.exists():
            print("Migrating db")
            connection = sqlite3.connect(db_path)
            cursor = connection.cursor()
            cursor.execute(
                """
                CREATE TABLE contacts(
                  id INTEGER PRIMARY KEY,
                  name TEXT NOT NULL,
                  email TEXT NOT NULL
                )
              """
            )
            connection.commit()
        self.connection = sqlite3.connect(db_path)
        self.connection.row_factory = sqlite3.Row

    def insert_contacts(self, contacts):
        print("Inserting contacts ...")
        # Done
        cursor = self.connection.cursor()
        start = datetime.now()
        cursor.executemany(
            """
            INSERT INTO contacts(name, email)
            VALUES(?, ?)
            """,
            contacts,
        )
        self.connection.commit()
        end = datetime.now()
        elapsed = end - start
        print("insert took", elapsed.microseconds / 1000, "ms")


    def get_name_for_email(self, email):
        print("Looking for email", email)
        cursor = self.connection.cursor()
        start = datetime.now()
        cursor.execute(
            """
            SELECT * FROM contacts
            WHERE email = ?
            """,
            (email,),
        )
        row = cursor.fetchone()
        end = datetime.now()

        elapsed = end - start
        print("query took", elapsed.microseconds / 1000, "ms")
        if row:
            name = row["name"]
            print(f"Found name: '{name}'")
            return name
        else:
            print("Not found")


# Done
def yield_contacts(num_contacts):
     yield from ((f"name-{i+1}", f"email-{i+1}@domain.tld") for i in range(num_contacts))


def main():
    num_contacts = int(sys.argv[1])
    db_path = Path("./contacts.sqlite3")
    contacts = Contacts(db_path)
    contacts.insert_contacts(yield_contacts(num_contacts))
    charlie = contacts.get_name_for_email(f"email-{num_contacts}@domain.tld")


if __name__ == "__main__":
    choice = 0
    if choice == 0:
        main()
    elif choice == 1:
        # Truncate the db : keep the schema without rows
        db_path = Path("./contacts.sqlite3")
        connection = sqlite3.connect(db_path)
        cursor = connection.cursor()
        cursor.execute("DELETE FROM contacts")
        connection.commit()
        connection.close()

