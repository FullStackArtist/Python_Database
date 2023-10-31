import pyodbc 
conn_str = (
    r'DRIVER={SQL Server};'
    r'SERVER=DESKTOP-9SUONJV;'
    r'DATABASE=master;'
    r'Trusted_Connection=yes;'
)

cnxn = pyodbc.connect(conn_str)
cursor=cnxn.execute('SELECT * FROM Customers')

for row in cursor:
    print('row = %r' % (row,))
