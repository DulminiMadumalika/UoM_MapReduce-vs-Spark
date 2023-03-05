import subprocess

hive_script = """
CREATE TABLE delay_flights (Id int, Year int,Month int,DayofMonth int,DayOfWeek int,DepTime int,
CRSDepTime int,ArrTime int,CRSArrTime int,UniqueCarrier string,FlightNum int,TailNum string,
ActualElapsedTime int,CRSElapsedTime int,AirTime int,ArrDelay int,DepDelay int,Origin string,
Dest string,Distance int,TaxiIn int,TaxiOut int,Cancelled int,CancellationCode string,
Diverted int,CarrierDelay int,WeatherDelay int,NASDelay int,SecurityDelay int,LateAircraftDelay int)
row format delimited fields terminated by ','  stored as textfile
location 's3://wpdbucket/dataset/' tblproperties ('skip.header.line.count'='1');
"""
# Define the EMR command to execute query for table creation
emr_command = f'hive -e "{hive_script}"'

# Use subprocess to execute the EMR command - for table creation
proc = subprocess.Popen(emr_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
output, error = proc.communicate()

# Print the output and error messages for table creation
print("Output:", output.decode('utf-8'))
print("Error:", error.decode('utf-8'))


# Define the Hive query to be executed
query_carrier = """SELECT Year, avg((CarrierDelay /ArrDelay)*100) from delay_flights GROUP BY Year;"""
query_weather = """SELECT Year, avg((WeatherDelay /ArrDelay)*100) from delay_flights GROUP BY Year;"""
query_NAS = """SELECT Year, avg((NASDelay /ArrDelay)*100) from delay_flights GROUP BY Year;"""
query_security = """SELECT Year, avg((SecurityDelay /ArrDelay)*100) from delay_flights GROUP BY Year;"""
query_late = """SELECT Year, avg((LateAirCraftDelay /ArrDelay)*100) from delay_flights GROUP BY Year;"""


# Define the EMR command to execute the Hive query
emr_command_carrier = f'hive -e "{query_carrier}"'
emr_command_weather = f'hive -e "{query_weather}"'
emr_command_NAS = f'hive -e "{query_NAS}"'
emr_command_Security = f'hive -e "{query_security}"'
emr_command_late = f'hive -e "{query_late}"'

# Use subprocess to execute the EMR command

for i in range(5):
    # Use subprocess to execute the EMR command - for carrier delay query
    print("=== Start Query Carrier ===", i+1)
    proc_carrier = subprocess.Popen(emr_command_carrier, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output_carrier, error_carrier = proc_carrier.communicate()

    # Print the output and error messages
    print(output_carrier.decode('utf-8'))
    print("Error:", error_carrier.decode('utf-8'))

for i in range(5):
    # Use subprocess to execute the EMR command - for weather delay query
    print("=== Start Query Weather===", i + 1)
    proc_weather = subprocess.Popen(emr_command_weather, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output_weather, error_weather = proc_weather.communicate()

    # Print the output and error messages
    print(output_weather.decode('utf-8'))
    print("Error Weather:", error_weather.decode('utf-8'))


for i in range(5):
    # Use subprocess to execute the EMR command - for NAS delay query
    print("=== Start Query NAS===", i + 1)
    proc_NSA = subprocess.Popen(emr_command_NAS, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output_NAS, error_NAS = proc_NSA.communicate()

    # Print the output and error messages
    print(output_NAS.decode('utf-8'))
    print("Error NAS:", error_NAS.decode('utf-8'))

for i in range(5):
    # Use subprocess to execute the EMR command - for security delay query
    print("=== Start Query Security ===", i + 1)
    proc_Sec = subprocess.Popen(emr_command_Security, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output_Sec, error_Sec = proc_Sec.communicate()

    # Print the output and error messages
    print(output_Sec.decode('utf-8'))
    print("Error security:", error_Sec.decode('utf-8'))

for i in range(5):
    # Use subprocess to execute the EMR command - for late delay query
    print("=== Start Query Late ===", i + 1)
    proc_late = subprocess.Popen(emr_command_late, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output_late, error_late = proc_late.communicate()

    # Print the output and error messages
    print(output_late.decode('utf-8'))
    print("Error Late:", error_late.decode('utf-8'))

