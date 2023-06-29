import pandas as pd
import duckdb
import sql_scripts as sc


def run_sql(connection, sql_query=""):
    if connection:
        return connection.sql(sql_query)


def get_connection(db_name="test.db"):
    return duckdb.connect(db_name)


def add_prefix_to_dict(dict, prefix):
    result_dict = {}

    for k in dict.keys():
        result_dict[prefix + k] = dict[k]
    return result_dict


def read_raw_data(file_path):
    data_df = pd.read_json(file_path, lines=True)
    data_df = data_df.join(
        data_df["track_metadata"].apply(
            lambda x: pd.Series(add_prefix_to_dict(x, "track_metadata_"))
        )
    ).drop(["track_metadata"], axis=1)
    data_df = data_df.join(
        data_df["track_metadata_additional_info"].apply(
            lambda x: pd.Series(
                add_prefix_to_dict(x, "track_metadata_additional_info_")
            )
        )
    ).drop(["track_metadata_additional_info"], axis=1)
    data_df["listened_at"] = pd.to_datetime(data_df["listened_at"], unit="s")
    data_df["listened_at_dt"] = data_df["listened_at"].apply(lambda x: x.date())
    print(data_df.head(1))
    return data_df


def ingest_raw_data(file_path, raw_table):
    df1 = read_raw_data(file_path)
    with get_connection() as conn:
        conn.execute("SET GLOBAL pandas_analyze_sample=100000")
        conn.sql(sc.CREATE_RAW_TABLE.format(RAW_TABLE=raw_table))
        conn.sql(sc.INSERT_INTO_RAW.format(RAW_TABLE=raw_table))
        conn.close()
    return


def run_tasks():
    with get_connection() as conn:
        conn.execute("SET GLOBAL pandas_analyze_sample=100000")
        print("Runnin Task TASK_2_A_1")
        conn.sql(sc.TASK_2_A_1).show()
        print("Runnin Task TASK_2_A_2")
        conn.sql(sc.TASK_2_A_2).show()
        print("Runnin Task TASK_2_A_3")
        conn.sql(sc.TASK_2_A_3).show()
        print("Runnin Task TASK_2_B")
        conn.sql(sc.TASK_2_B).show()
        print("Runnin Task TASK_2_C")
        conn.sql(sc.TASK_2_C).show()
        conn.close()
    return


# Press the green button in the gutter to run the script.
if __name__ == "__main__":
    print("Starting data ingestion")
    ingest_raw_data("dataset/dataset.txt", "RAW_LISTENS")
    print("Data Ingestion completed")
    run_tasks()
    print("Exiting")

