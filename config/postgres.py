import psycopg2
import json

#establishing the connection
conn = psycopg2.connect(
   database="postgres", user='postgres', password='password', host='127.0.0.1', port= '5432'
)
#Creating a cursor object using the cursor() method
cursor = conn.cursor()

#Executing an MYSQL function using the execute() method
cursor.execute("select version()")

# Fetch a single row using fetchone() method.
data = cursor.fetchone()
print("Connection established to: ",data)

cursor.execute("SELECT \"nodeConfig\", \"taskName\", \"pCpu\" FROM \"initialRuntimes\" WHERE \"wfName\" = \'nfcore/methylseq:1.6.1\'")
result = cursor.fetchall()
print(result)

# Opening JSON file
f = open('/root/git/WorkflowSim-1.0/config/runtimes_pp.json')
  
# returns JSON object as 
# a dictionary
data = json.load(f)

# Iterating through the json
# list

data_uti = []
for i in data:
    if i['wfName'] == 'nfcore/methylseq:1.6.1':
        sql_cmd = "SELECT \"pCpu\" FROM \"initialRuntimes\" WHERE \"wfName\" = \'nfcore/methylseq:1.6.1\' and \"taskName\" = \'" + i['taskName'] + "\' and \"nodeConfig\" = \'" + str(i['nodeConfig']) + "\'"
        cursor.execute(sql_cmd)
        pCpu = cursor.fetchall();
        #print(sql_cmd)
        value = max(pCpu)[0]
        key = "pCpu"
        if "pCpu" not in i.keys():
            i["pCpu"] = ""
        i["pCpu"] = value
        #print(pCpu)
        print(i)             
        data_uti.append(i)

#print(data)
with open('/root/git/WorkflowSim-1.0/config/runtimes_uti_pp.json', 'w') as f:
    json.dump(data_uti, f)

# Closing file
f.close()


#Closing the connection
conn.close()