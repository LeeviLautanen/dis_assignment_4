import psycopg2
from psycopg2 import sql
from pymongo import MongoClient


class StoreDBClient:
    def __init__(self):
        self.flip_flop = True  # For alternating inserts between DBs

        # PostgreSQL configuration
        self.pg_connection = None
        self.pg_config = {
            "host": "localhost",
            "user": "postgres",
            "password": "mchelper",
            "port": "5432",
            "database": "store_db",
        }
        self.pg_tables = []

        # MongoDB configuration
        self.mongo_client = None
        self.mongo_db = None
        self.mongo_config = {
            "host": "localhost",
            "port": 27017,
            "database": "data_intensive_systems",
        }
        self.mongo_collections = []

        self.mongo_table_cols = {
            "customers": [
                "customer_id",
                "first_name",
                "last_name",
                "email",
                "created_date",
            ],
            "products": [
                "product_id",
                "product_name",
                "description",
                "price",
                "stock_quantity",
                "category_id",
            ],
            "orders": [
                "order_id",
                "customer_id",
                "order_date",
                "total_amount",
                "status",
            ],
            "product_reviews": [
                "review_id",
                "product_id",
                "customer_id",
                "rating",
                "review_text",
                "review_date",
            ],
            "user_preferences": ["preference_id", "customer_id", "theme", "language"],
        }

        self.primary_keys = {
            "customers": "customer_id",
            "categories": "category_id",
            "products": "product_id",
            "orders": "order_id",
            "order_items": "order_item_id",
            "product_reviews": "review_id",
            "user_preferences": "preference_id",
        }

    def connect_to_databases(self):
        try:
            if self.pg_connection:
                self.pg_connection.close()
            if self.mongo_client:
                self.mongo_client.close()

            # Connect to PostgreSQL
            self.pg_connection = psycopg2.connect(**self.pg_config)
            print(f"Connected to PostgreSQL {self.pg_config['database']}.")

            # Connect to MongoDB
            self.mongo_client = MongoClient(
                self.mongo_config["host"], self.mongo_config["port"]
            )
            self.mongo_db = self.mongo_client[self.mongo_config["database"]]
            print(f"Connected to MongoDB {self.mongo_config['database']}.")

            return True
        except Exception as e:
            print(f"Error connecting to databases: {e}")
            return False

    def update_local_table_lists(self):
        # Get PostgreSQL tables
        try:
            with self.pg_connection.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """)
                self.pg_tables = [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error fetching PostgreSQL tables: {e}")

        # Get MongoDB collections
        try:
            self.mongo_collections = list(self.mongo_db.list_collection_names())
        except Exception as e:
            print(f"Error fetching MongoDB collections: {e}")

        # Combine, sort and deduplicate
        all_entities = sorted(set(self.pg_tables + self.mongo_collections))
        return all_entities

    def select_table(self):
        if self.pg_tables == [] and self.mongo_collections == []:
            tables = self.update_local_table_lists()
        else:
            tables = sorted(set(self.pg_tables + self.mongo_collections))

        if not tables:
            print("No tables found in either database.")
            return None

        print("\nAvailable tables:")
        for i, table in enumerate(tables, 1):
            print(f"{i}. {table}")

        choice = int(input("\nSelect a table: "))
        if 1 <= choice <= len(tables):
            return tables[choice - 1]
        else:
            print("Invalid selection.")
            return None

    def get_rows(self, table_name):
        rows = []
        if table_name in self.pg_tables:
            cursor = self.pg_connection.cursor()
            cursor.execute(
                sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
            )
            rows += cursor.fetchall()

        if table_name in self.mongo_collections:
            rows += list(self.mongo_db[table_name].find())

        return rows

    def insert_data(self, table_name):
        if table_name in self.pg_tables:
            db_cols = self.get_postgresql_cols(table_name)
            db_types = ["postgresql"]
        elif table_name in self.mongo_collections:
            db_cols = self.mongo_table_cols.get(table_name, {})
            db_types = ["mongodb"]
        else:
            print(f"Table {table_name} not found in either database")
            return

        if table_name in self.pg_tables and table_name in self.mongo_collections:
            if self.flip_flop:
                db_types = ["mongodb"]
            else:
                db_types = ["postgresql"]

            self.flip_flop = not self.flip_flop

        # Collect data to be inserted
        new_data = {}
        print(f"\nInserting into {table_name}:")
        for col in db_cols:
            if col in self.primary_keys.get(table_name, ""):
                continue
            new_data[col] = input(f"{col}: ").strip()

        if not new_data:
            print("No data provided. Insert cancelled.")
            return

        if "postgresql" in db_types:
            try:
                with self.pg_connection.cursor() as cursor:
                    columns = list(new_data.keys())
                    values = list(new_data.values())

                    placeholders = ", ".join(["%s"] * len(columns))
                    query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                        sql.Identifier(table_name),
                        sql.SQL(", ").join(map(sql.Identifier, columns)),
                        sql.SQL(placeholders),
                    )

                    cursor.execute(query, values)
                    self.pg_connection.commit()
                    return True

            except Exception as e:
                print(f"Insert failed: {e}")
                self.pg_connection.rollback()
                return False

        if "mongodb" in db_types:
            try:
                self.mongo_db[table_name].insert_one(new_data)
                return True
            except Exception as e:
                print(f"MongoDB insert failed: {e}")
                return False

        print("\nData inserted successfully")

    def delete_data(self, row, table):
        if not row or not table:
            print("No row or table provided.")
            return False

        if isinstance(row, (list, tuple)):  # PostgreSQL row
            pk_col = self.primary_keys.get(table)
            pk_val = row[0]  # Assume first column is primary key

            try:
                with self.pg_connection.cursor() as cursor:
                    query = sql.SQL("DELETE FROM {} WHERE {} = %s").format(
                        sql.Identifier(table),
                        sql.Identifier(pk_col),
                    )
                    cursor.execute(query, (pk_val,))
                    self.pg_connection.commit()
                    return True
            except Exception as e:
                print(f"PostgreSQL delete failed: {e}")
                self.pg_connection.rollback()

        elif isinstance(row, dict):  # MongoDB document
            pk_col = self.primary_keys.get(table)
            pk_val = row.get("_id")

            try:
                self.mongo_db[table].delete_one({"_id": pk_val})
                return True
            except Exception as e:
                print(f"MongoDB delete failed: {e}")

        return False

    def update_data(self, row, table):
        if not row or not table:
            print("No row or table provided.")
            return False

        pk_col = self.primary_keys.get(table)

        if isinstance(row, (list, tuple)):  # PostgreSQL row
            pk_val = row[0]  # Assume first column is primary key
            db_cols = self.get_postgresql_cols(table)[1:]  # Dont include primary key

            # Ask user for new values
            new_values = []
            for col in db_cols:
                value = input(f"New value for {col}: ").strip()
                new_values.append(value)

            try:
                with self.pg_connection.cursor() as cursor:
                    set_clause = sql.SQL(", ").join(
                        sql.SQL("{} = %s").format(sql.Identifier(col))
                        for col in db_cols
                    )
                    query = sql.SQL("UPDATE {} SET {} WHERE {} = %s").format(
                        sql.Identifier(table),
                        set_clause,
                        sql.Identifier(pk_col),
                    )
                    cursor.execute(query, new_values + [pk_val])
                    self.pg_connection.commit()
                    return True
            except Exception as e:
                print(f"PostgreSQL update failed: {e}")
                self.pg_connection.rollback()

        elif isinstance(row, dict):  # MongoDB document
            pk_val = row.get("_id")
            db_cols = list(row.keys())
            db_cols.remove("_id")

            # Ask user for new values
            update_data = {}
            for col in db_cols:
                value = input(f"New value for {col}: ").strip()
                update_data[col] = value

            try:
                self.mongo_db[table].update_one({"_id": pk_val}, {"$set": update_data})
                return True
            except Exception as e:
                print(f"MongoDB update failed: {e}")

        return False

    def get_postgresql_cols(self, table_name):
        try:
            with self.pg_connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns 
                    WHERE table_name = %s
                """,
                    (table_name,),
                )

                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error getting PostgreSQL structure for table '{table_name}': {e}")

    def print_rows(self, rows):
        formatted_rows = []
        for row in rows:
            if isinstance(row, dict):
                row_copy = row.copy()  # Copy to avoid modifying original
                row_copy.pop("_id", None)  # Remove MongoDB internal ID
                formatted_rows.append(", ".join(f"{v}" for _, v in row_copy.items()))
            elif isinstance(row, (list, tuple)):
                formatted_rows.append(
                    ", ".join(str(item) for item in row[1:])
                )  # Skip primary key

        for i, row in enumerate(formatted_rows, 1):
            print(f"{i}. {row}")

    def main(self):
        self.connect_to_databases()
        self.update_local_table_lists()

        while True:
            print("\nDatabase Client")
            print("1. View data from table")
            print("2. Insert data into table")
            print("3. Update data in table")
            print("4. Delete data from table")
            print("5. Exit")

            choice = input("\nSelect option: ").strip()

            if choice == "1":
                selected_table = self.select_table()
                if selected_table:
                    rows = self.get_rows(selected_table)
                    print(f"\nData from {selected_table} ({len(rows)} rows):")
                    self.print_rows(rows)

            elif choice == "2":
                selected_table = self.select_table()
                if selected_table:
                    self.insert_data(selected_table)

            elif choice == "3":
                selected_table = self.select_table()
                if selected_table:
                    rows = self.get_rows(selected_table)
                    print(f"\nData from {selected_table} ({len(rows)} rows):")
                    self.print_rows(rows)
                    row_num = int(input("\nSelect row to update: "))

                    if 0 < row_num <= len(rows):
                        if self.update_data(rows[row_num - 1], selected_table):
                            print("Row updated successfully.")
                    else:
                        print("Invalid row number.")

            elif choice == "4":
                selected_table = self.select_table()
                if selected_table:
                    rows = self.get_rows(selected_table)
                    print(f"\nData from {selected_table} ({len(rows)} rows):")
                    self.print_rows(rows)
                    row_num = int(input("\nSelect row to delete: "))

                    if 0 < row_num <= len(rows):
                        if self.delete_data(rows[row_num - 1], selected_table):
                            print("Row deleted successfully.")
                    else:
                        print("Invalid row number.")

            elif choice == "5":
                print("Exiting...")
                break

            else:
                print("Invalid option. Please try again.")


if __name__ == "__main__":
    client = StoreDBClient()
    client.main()
