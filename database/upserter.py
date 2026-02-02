import polars as pl
from database.connector import MySQL_Connector


class UpsertInfos(MySQL_Connector):
    def __init__(self):
        MySQL_Connector.__init__(self)

    def upsert_df(self, table, df, batch_size):
        df = df.collect()
        total_rows = len(df)
        for i in range(0, total_rows, batch_size):
            batch = df.slice(i, batch_size)
            self._upsert_batch(table, batch)

    def _upsert_batch(self, table, df):
        cursor = self.connection.cursor()

        columns = ", ".join(df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        update_clause = ", ".join([f"{col}=VALUES({col})" for col in df.columns])

        sql = f"""
            INSERT INTO {table} ({columns})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause};
        """

        values = df.to_numpy().tolist()
        cursor.executemany(sql, values)
        self.connection.commit()