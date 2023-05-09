from flask import Flask, request, abort
from flask import Flask, render_template, request, session, redirect, url_for,send_file,make_response
import pandas as pd
import boto3
from datetime import datetime
import os
#neo4j 
from neo4j import GraphDatabase
#from neo4j import __version__ as neo4j_version
#print(neo4j_version)

class Neo4jConnection:
    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))

        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        if self.__driver is not None:
          self.__driver.close()

    def query(self, query, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = self.__driver.session(database=db) if db is not None else self.__driver.session()
            response = list(session.run(query))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
              session.close()
        return response

    def check_permission(self,user,file):
                # check if owner
        query_string = "MATCH (u:User {username:'"+"{user}".format(user=user)+"'})-[:OWNS]->(f:File {name:'"+"{file}".format(file=file)+"'}) RETURN u;"
        res = self.query(query_string)
                # owner
        if len(res):
            return "read,write"
        else:
                        # check if any access granted
            query_string = "MATCH (u:User {username:'"+"{user}".format(user=user)+"'})-[access:HAS_ACCESS]->(f:File {name:'"+"{file}".format(file=file)+"'}) RETURN access.permission as permission;"
            res = self.query(query_string)
                        # if permission
            if len(res)>0:
                return res[0].data()['permission']
            else:
                return "No Permission"

    def add_permission(self,user,file,permission):
        query_string = "MATCH (u:User {username:'"+"{user}".format(user=user)+"'}), (f:File {name:'"+"{file}".format(file=file)+"'})\n"
        query_string += "MERGE (u)-[:HAS_ACCESS {permission:'"+"{permission}".format(permission=permission)+"'}]->(f);"
        res = self.query(query_string)

    def remove_access(self,user,file):
        query_string = "MATCH (u:User {username:'"+"{user}".format(user=user)+"'})-[a:HAS_ACCESS]->(f:File {name:'"+"{file}".format(file=file)+"'}) DELETE a;"
        res = self.query(query_string)

    def insert_file(self, user, role, file, path):

        query_string = "MATCH (u:User {username:'"+"{user}".format(user=user)+"'})\n"
        query_string += "MERGE (f:File {name:'"+"{file}".format(file=file)+"'})\n"
        query_string += "ON CREATE SET f.path = '"+"{path}".format(path=path)+"'\n"
        query_string += "MERGE (u)-[:OWNS]->(f);"

        res = self.query(query_string)

        query_string = "MATCH (u:User)-[:HAS_ROLE]->(r:Role {type:'"+"{role}".format(role=role)+"'}) RETURN u;"

        user_res = self.query(query_string)
        userlist = list()
        for user in user_res:
            userlist.append(user.data()['u']['username'])
        print(userlist)

        for uname in userlist:
            self.add_permission(uname,file,"read,write")


conn = Neo4jConnection(uri="bolt://localhost:7687", user="neo4j", pwd="Srinidhi123")

# Inserting file
#res = conn.insert_file('userA.aseetartisits@bits.com','assetartists','fileA.png',"F://path/")

# Checking permission
#res = conn.check_permission('userC.developer@bits.com','fileA.cpp')
#print(res)

# Add permission
#res = conn.add_permission('userA.developer@bits.com','fileA.png','read')

# Remove Access
#res = conn.remove_access('userA.developer@bits.com','fileA.png')

#connect to RDS MySQL server
import mysql.connector
mydb = mysql.connector.connect(
  host="datalakedb1.c0aqiocd7qwz.ap-south-1.rds.amazonaws.com",
  user="admin",
  password="SrinidhiBITS123",
  database="USERSINFO"
)
mycursor = mydb.cursor()

app = Flask(__name__)





app.secret_key = 'mysecretkey'



# Login route
@app.route('/', methods=['GET', 'POST'])

def login():
    if request.method == 'POST':
        # Check if the submitted username and password are in the authorized users list
        username = request.form['username']
        password = request.form['password']
        query="SELECT PASSWORD FROM USERS WHERE USERNAME=\'"+username+"\';"
        mycursor.execute(query)
        myresult = mycursor.fetchall()
        print(myresult)
        if password==myresult[0][0]:
        #    # Set the user as logged in
              session['logged_in'] = True
              session['username'] = username
              return redirect(url_for('home'))
        else:
            return render_template('login.html', error='Invalid username or password')
    else:
        return render_template('login.html')

# Home page route

@app.route('/home')
def home():
    if 'logged_in' in session and session['logged_in']:
        mycursor.execute("SELECT * FROM METADATA")
        rows = mycursor.fetchall()
        return render_template('home.html',rows=rows)
    else:
        return redirect(url_for('login'))

@app.route('/writefunc/<row>')
def writefunc(row):
    s3 = boto3.client('s3')
    S3_BUCKET_NAME="datalakeadbms"
    sql="SELECT TEAM FROM METADATA WHERE DATASETNAME=\'"+row+"\';"
    mycursor.execute(sql)
    rows=mycursor.fetchall()
    alist=row.split(".")
    filename=rows[0][0]+"/"+row
    print(filename)
    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=filename)
    file_content = obj['Body'].read()
    # Set the content type and disposition headers for the file download
    #headers = {
    #    'Content-Type': obj['ContentType'],
    #    'Content-Disposition': f'attachment; filename="{filename}"'
    #}

    # Return the file as an attachment
    #return send_file(obj['Body'], headers=headers)
    response = make_response(file_content)
    response.headers['Content-Disposition'] = f'attachment; filename={filename}'
    return response
    #return f"You selected{row}!"

@app.route('/readfunc/<row>')
def readfunc(row):
    s3 = boto3.client('s3')
    S3_BUCKET_NAME="datalakeadbms"
    sql="SELECT TEAM FROM METADATA WHERE DATASETNAME=\'"+row+"\';"
    mycursor.execute(sql)
    rows=mycursor.fetchall()
    alist=row.split(".")
    filename=rows[0][0]+"/"+row
    print(filename)
    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=filename)
    alist=row.split(".")
    if alist[1]=='csv':
     file_content = obj['Body'].read().decode('ISO-8859-1')

    else:
     file_content = obj['Body'].read().decode('utf-8')
    return render_template('readfunc.html',filename=filename, content=file_content)
    return f"You selected {row}!"

@app.route('/mydatasets')
def mydatasets():
    if 'logged_in' in session and session['logged_in']:
        mycursor.execute("SELECT DATASETNAME FROM METADATA;")
        rows=mycursor.fetchall()
        readlist=[]
        writelist=[]
        for ele in rows:
            print(ele[0])
            res=conn.check_permission(session['username'],ele[0])
            if res=='read,write':
                writelist.append(ele[0])
            if res=='read':
                readlist.append(ele[0])
        print(writelist)
        return render_template('mydatasets.html',wl=writelist,rl=readlist)
    else:
        return redirect(url_for('login'))

def ingestStructuredDB1(filename,team):
 # Read CSV file into pandas dataframe
 fullfilename='/home/ubuntu/flaskwebapp/temp/'+filename
 s3 = boto3.client('s3')
 # Set the S3 bucket and folder name
 bucket_name = 'datalakeadbms'
 folder_name = team+'/'

 # Set the local file path of file1
 file_path ='/home/ubuntu/flaskwebapp/temp/'+filename

 # Set the S3 object key name (file name in S3)
 object_key = folder_name + filename

 # Upload file1 to S3 bucket test1 and folder team1
 s3.upload_file(file_path, bucket_name, object_key)
 df = pd.read_csv(fullfilename,encoding="ISO-8859-1")
 # Get table name from file name
 filelist=filename.split(".")
 table_name = filelist[0]
 # Get column names and types from dataframe
 columns = list(df.columns)
 types = df.dtypes
 # Define MySQL connection parameters
 config = {
    'user': 'admin',
    'password': 'SrinidhiBITS123',
    'host': 'datalakedb1.c0aqiocd7qwz.ap-south-1.rds.amazonaws.com',
    'database': 'DATALAKEDB1'
    }
 # Connect to MySQL
 conn = mysql.connector.connect(**config)
 cursor = conn.cursor()
 # Create MySQL table based on CSV file schema
 create_table_query = 'CREATE TABLE IF NOT EXISTS {} ('.format(table_name)
 for i in range(len(columns)):
    if 'object' in str(types[i]):
        create_table_query += '{} VARCHAR(255), '.format(columns[i])
    elif 'float' in str(types[i]):
        create_table_query += '{} FLOAT, '.format(columns[i])
    elif 'int' in str(types[i]):
        create_table_query += '{} INT, '.format(columns[i])
 create_table_query = create_table_query[:-2] + ');'
 cursor.execute(create_table_query)
 # Insert records into MySQL table
 insert_query = 'INSERT INTO {} ('.format(table_name)
 for col in columns:
    insert_query += '{}, '.format(col)
 insert_query = insert_query[:-2] + ') VALUES ('
 for i in range(len(columns)):
    insert_query += '%s, '
 insert_query = insert_query[:-2] + ');'
 for row in df.itertuples(index=False):
    cursor.execute(insert_query, tuple(row))
 conn.commit()
 # Close MySQL connection
 cursor.close()
 conn.close()
 # Get the current date and time
 now = datetime.now()
 # Format the current date and time as a string
 formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
 sql = "INSERT INTO METADATA (DATASETNAME,OWNER,TEAM,TYPE,LOCATION,CREATE_TIMESTAMP) VALUES (%s, %s,%s,%s,%s,%s)"
 val = (filename,session['username'],team,"structured","DATABASE1",formatted_date)
 mycursor.execute(sql, val)
 mydb.commit()
 
def ingestStructuredDB2(filename,team):
 # Read CSV file into pandas dataframe
 fullfilename='/home/ubuntu/flaskwebapp/temp/'+filename
 s3 = boto3.client('s3')
 # Set the S3 bucket and folder name
 bucket_name = 'datalakeadbms'
 folder_name = team+'/'

 # Set the local file path of file1
 file_path ='/home/ubuntu/flaskwebapp/temp/'+filename

 # Set the S3 object key name (file name in S3)
 object_key = folder_name + filename

 # Upload file1 to S3 bucket test1 and folder team1
 s3.upload_file(file_path, bucket_name, object_key)
 df = pd.read_csv(fullfilename,encoding="ISO-8859-1")
 # Get table name from file name
 filelist=filename.split(".")
 table_name = filelist[0]
 # Get column names and types from dataframe
 columns = list(df.columns)
 types = df.dtypes
 # Define MySQL connection parameters
 config = {
    'user': 'admin',
    'password': 'SrinidhiBITS123',
    'host': 'datalakedb2.cluster-c0aqiocd7qwz.ap-south-1.rds.amazonaws.com',
    'database': 'DATALAKEDB2'
    }
 # Connect to MySQL
 conn = mysql.connector.connect(**config)
 cursor = conn.cursor()
 # Create MySQL table based on CSV file schema
 create_table_query = 'CREATE TABLE IF NOT EXISTS {} ('.format(table_name)
 for i in range(len(columns)):
    if 'object' in str(types[i]):
        create_table_query += '{} VARCHAR(255), '.format(columns[i])
    elif 'float' in str(types[i]):
        create_table_query += '{} FLOAT, '.format(columns[i])
    elif 'int' in str(types[i]):
        create_table_query += '{} INT, '.format(columns[i])
 create_table_query = create_table_query[:-2] + ');'
 cursor.execute(create_table_query)
 # Insert records into MySQL table
 insert_query = 'INSERT INTO {} ('.format(table_name)
 for col in columns:
    insert_query += '{}, '.format(col)
 insert_query = insert_query[:-2] + ') VALUES ('
 for i in range(len(columns)):
    insert_query += '%s, '
 insert_query = insert_query[:-2] + ');'
 for row in df.itertuples(index=False):
    cursor.execute(insert_query, tuple(row))
 conn.commit()
 # Close MySQL connection
 cursor.close()
 conn.close()
 # Get the current date and time
 now = datetime.now()
 # Format the current date and time as a string
 formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
 sql = "INSERT INTO METADATA (DATASETNAME,OWNER,TEAM,TYPE,LOCATION,CREATE_TIMESTAMP) VALUES (%s, %s,%s,%s,%s,%s)"
 val =(filename,session['username'],team,"structured","DATABASE2",formatted_date)
 mycursor.execute(sql, val)
 mydb.commit()
 
def ingestUnstructured(filename,team):
 s3 = boto3.client('s3')
 # Set the S3 bucket and folder name
 bucket_name = 'datalakeadbms'
 folder_name = team+'/'

 # Set the local file path of file1
 file_path ='/home/ubuntu/flaskwebapp/temp/'+filename

 # Set the S3 object key name (file name in S3)
 object_key = folder_name + filename

 # Upload file1 to S3 bucket test1 and folder team1
 s3.upload_file(file_path, bucket_name, object_key)
 # Get the current date and time
 now = datetime.now()
 # Format the current date and time as a string
 formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
 sql = "INSERT INTO METADATA (DATASETNAME,OWNER,TEAM,TYPE,LOCATION,CREATE_TIMESTAMP) VALUES (%s, %s,%s,%s,%s,%s)"
 val = (filename,session['username'],team,"unstructured","S3",formatted_date)
 mycursor.execute(sql, val)
 mydb.commit()

@app.route('/dataingestion', methods=['GET','POST'])
def dataingestion():
    print("hello ingestion")
    if request.method == 'POST':
     file = request.files['file']
     filename = request.form['filename']
     file.save('temp/' + filename)
     filelist=filename.split(".")
     user=session['username']
     query="SELECT TEAM FROM USERS WHERE USERNAME=\'"+user+"\';"
     mycursor.execute(query)
     myresult = mycursor.fetchall()
     if filelist[1]=="csv":
        if myresult[0][0]=='management' or myresult[0][0]=='stats':
         ingestStructuredDB2(filename,myresult[0][0])
        else:
         ingestStructuredDB1(filename,myresult[0][0])
     else:
         ingestUnstructured(filename,myresult[0][0])
     filepn='temp/'+filename
     res=conn.insert_file(session['username'],myresult[0][0],filename,'path')
     os.remove(filepn)
     return 'File "' + filename + '" saved to local server!'
    else:
     return render_template("dataingestion.html")

# Index Page
@app.route('/datasharing')
def index():
    # Fetching data from MySQL Table
    #mycursor = mydb.cursor()
    query="SELECT DISTINCT DATASETNAME FROM METADATA WHERE OWNER=\'"+session['username']+"\';"
    mycursor.execute(query)
    result1 = mycursor.fetchall()
    mycursor.execute("SELECT DISTINCT USERNAME FROM USERS")
    result2 = mycursor.fetchall()
    return render_template('datasharing.html', result1=result1, result2=result2)

# Handling Form Submission
@app.route('/submit', methods=['POST'])
def submit():
    option1 = request.form.get('option1')
    option2 = request.form.get('option2')
    option3= request.form.get('typeofaccess')
    print('Selected Option 1:', option1)
    print('Selected Option 2:', option2)
    print(option3)
    res=conn.add_permission(option1,option2,option3)
    print(res)
    return 'Provided access'

@app.route('/logout',methods=['GET','POST'])
def logout():
    session.pop('logged_in', None)
    session.pop('username', None)
    return render_template('login.html')

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port=5000)
