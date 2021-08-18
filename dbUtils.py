#dbUtils.py
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import time

R = 10
BI = 500
BS = 500
BU = 500

def createDatabase():
    senha = os.environ['SENHA_POSTGRES']
    conn = psycopg2.connect("dbname=postgres user=postgres password=@@carol16A")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("SELECT datname FROM pg_database;")
    list_database = cur.fetchall()
    if ("ads",) in list_database:
        cur.execute("DROP DATABASE ads;")
    cur.execute("CREATE DATABASE ads;")
    cur.close()
    conn.close()

def createTable():
    conn = psycopg2.connect("dbname=ads user=postgres password=@@carol16A")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("CREATE TABLE A(                        "
                " A0 INTEGER PRIMARY KEY,                      "
                " A1 INTEGER NULL,                      "
                " A2 NUMERIC NULL,                      "
                " A3 VARCHAR(26) NULL,                  "
                " A4 VARCHAR(26) NULL,                  "
                " A5 TIME DEFAULT CURRENT_TIME NULL,    "
                " A6 DATE DEFAULT CURRENT_DATE NULL );  ")
    cur.execute("VACUUM;")
    cur.close()
    conn.close()

def connect():
    conn = psycopg2.connect("dbname=ads user=postgres password=@@carol16A")
    return conn

def insert(conn, cur):
    c = 1 
    dt = 0
    s = "INSERT (s)\n"

    for r in range(R):
        i = time.perf_counter()
        
        for b in range(BI):
            cur.execute("insert into A(A0,A1,A2,A3,A4,A5,A6) values("+str(c)+",1,1.2,'abcdefghijklmnopqrstuvxz','abcdefghijklmnopqrstuvxz','15:25:22', '2021-05-11');") #COMENTE ESSA LINHA PARA MEDIR OVERHEAD
            c+=1
        f=time.perf_counter()
        dt=format((f-i)/BU, ".18f")
        s += str(r+1)+";"+str(dt)+"\n"
    return s

def select(conn, cur):
    dt = 0
    c = 1
    s = "SELECT (s)\n"
    for r in range(R):
        i = time.perf_counter() 
        
        for b in range(BS):
            cur.execute("select * from A where A0="+str(c)) #COMENTE ESSA LINHA PARA MEDIR OVERHEAD
            c+=1
        f=time.perf_counter() 
        dt=format((f-i)/BU, ".18f")
        s += str(r+1)+";"+str(dt)+"\n"
    return s

def update(conn, cur):
    dt = 0
    s = "UPDATE (s)\n"
    c = 1
    for r in range(R):
        i = time.perf_counter()
        for b in range(BU):
            cur.execute("update A set A1=1, A2=2.22, A3= 'ABCDEFGHIJKLMNOPQRSTUVXZ', A4 = 'ABCDEFGHIJKLMNOPQRSTUVXZ', A5= '16:25:22', A6='2031-05-11' where A0=" + str(c)) #COMENTE ESSA LINHA PARA MEDIR OVERHEAD
            c+=1
        f=time.perf_counter() 
        dt=format((f-i)/BU, ".18f")
        s += str(r+1)+";"+str(dt)+"\n"
    
    return s