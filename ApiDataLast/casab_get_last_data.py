from datetime import date
import requests
from datetime import date
import uuid
import mysql.connector as conn

today = str(date.today())
def colecting_data():
    url = 'https://data.exia.com.co/demo/data/last'
    try:
        res = requests.get(url).json()
    except Exception as e:
        print(404, e)
    else:
        print(200)
    
    data_id = str(uuid.uuid1(clock_seq=3))
    co2factor = res["co2factor"]
    createddata = res["created_at"].split(" ")
    created_at = "{}".format(today)
    created_at_time = createddata[3]
    day_energy = res["day_energy"]
    id = res["id"]
    pac_sum = res["pac_sum"]
    pac_sum_counter = res["pac_sum_counter"]
    pac_sum_temp = res["pac_sum_temp"]
    power_counter = res["power_counter"]
    power_real = res["power_real"]
    power_real_temp = res["power_real_temp"]
    reference = res["reference"]
    total_energy = res["total_energy"]
    updatedata = res["update_at"].split(" ")
    update_at_date = "{}".format(today)
    update_at_time = updatedata[3]

    return (
        data_id,co2factor,created_at,created_at_time,day_energy,
        id,pac_sum,pac_sum_counter,pac_sum_temp,
        power_counter,power_real,power_real_temp,
        reference,total_energy,update_at_date,update_at_time
        )

#populating the database
def insert_data(curr):
    insert_into_lastdata = ("""INSERT INTO lastdata (data_id,co2factor,created_at_date,created_at_time,day_energy,id,pac_sum,pac_sum_counter,pac_sum_temp,power_counter,power_real,power_real_temp,reference,total_energy,update_at_date,update_at_time)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);""")
    row_to_insert = colecting_data()
    curr.execute(insert_into_lastdata,row_to_insert)

    

try:
    db = conn.connect(
        user="casab001", 
        password="CA765@19", 
        host="casab.mysql.database.azure.com", 
        port=3306, 
        database="casabapidata"
    )  
    print("\n"*2)
    print("conexión exitosa", db)
    
except Exception as e:
    print("Ocurrió un error al conectar a SQL Server: ", e)

cursor = db.cursor()
stop = 0
while True:
    try:
        insert_data(cursor)
        db.commit()
    except Exception as e :
        print("error: ", e)
        break
    

