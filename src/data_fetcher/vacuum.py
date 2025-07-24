import duckdb


def db_vacuum():
    con = duckdb.connect(r"data/data.duckdb")
    con.execute("VACUUM")
    con.execute("ANALYZE")
    con.close()
