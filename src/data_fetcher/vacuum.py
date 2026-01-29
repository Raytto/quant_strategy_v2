from qs.sqlite_utils import connect_sqlite


def db_vacuum():
    con = connect_sqlite("data/data.sqlite")
    con.execute("VACUUM")
    con.execute("ANALYZE")
    con.close()
