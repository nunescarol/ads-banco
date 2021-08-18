import dbUtils as dbu

dbu.createDatabase()
dbu.createTable()
conn = dbu.connect()
cur = conn.cursor()

time_string = dbu.insert(conn, cur)
time_string += dbu.select(conn, cur)
cur = conn.cursor()
time_string += dbu.update(conn, cur)
# cur.execute("insert into a")

cur.close()
conn.close()
# f = open("tempos-banco-overhead.csv", "w")
# f.write(time_string)
print(time_string)
# f.close()

