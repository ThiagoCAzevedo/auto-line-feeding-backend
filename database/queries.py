import polars as pl
from database.connector import MySQL_Connector


class UpsertInfos(MySQL_Connector):
    def __init__(self):
        MySQL_Connector.__init__(self)

    def upsert_df(self, table, df, batch_size):

        if isinstance(df, pl.LazyFrame):
            df = df.collect()

        total_rows = len(df)
        for i in range(0, total_rows, batch_size):
            batch = df.slice(i, batch_size)
            self._upsert_batch(table, batch)

    def _upsert_batch(self, table, df):
        if not table.replace("_", "").isalnum():
            return

        values = df.rows()

        columns = ", ".join(df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        update_clause = ", ".join([f"{col}=VALUES({col})" for col in df.columns])

        sql = f"""
            INSERT INTO {table} ({columns})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause};
        """

        cursor = self.connection.cursor()
        try:
            cursor.executemany(sql, values)
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise
        finally:
            cursor.close()


class SelectInfos(MySQL_Connector):
    def __init__(self):
        MySQL_Connector.__init__(self)

    def select_bd_infos(self, query):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            cols = cursor.column_names
            return pl.DataFrame(rows, schema=cols).lazy()
        finally:
            cursor.close()