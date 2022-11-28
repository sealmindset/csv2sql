import pandas as pd
import psycopg2
import re

def getColumnDtypes(dataTypes):
    dataList = []
    for x in dataTypes:
        if(x == 'int64'):
            dataList.append('int')
        elif (x == 'float64'):
            dataList.append('float')
        elif (x == 'bool'):
            dataList.append('boolean')
        elif (x == 'object'):
            dataList.append('longtext')
        else:
            dataList.append('blob')
    return dataList

def insertRows(df, dest_table):

    insert = """
    INSERT INTO `{dest_table}` (
        """.format(dest_table=dest_table)

    columns_string = str(list(df.columns))[1:-1]
    columns_string = re.sub(r' ', '\n        ', columns_string)
    columns_string = re.sub(r'\'', '', columns_string)

    values_string = ''

    for row in df.itertuples(index=False,name=None):
        values_string += re.sub(r'nan', 'null', str(row))
        values_string += ',\n'

    return insert + columns_string + ')\n     VALUES\n' + values_string[:-2] + ';'

data = pd.read_csv(r'nessus.csv', header=0)
df = pd.DataFrame(data)


# Column Headers
# Remove special characters and convert to lower case
df.columns = df.columns.str.replace('[\W+]', '', regex=True)
df.columns = df.columns.str.lower()
# Convert to list
columnName = list(df.columns.values)

# Identify columns data types
columnDataType = getColumnDtypes(df.dtypes)

## Create TABLE statement
#createTableStatement = 'CREATE TABLE IF NOT EXISTS nessusraw (' + '\n' + 'ID BIGINT unsigned PRIMARY KEY AUTO_INCREMENT,' 
createTableStatement = 'CREATE TABLE IF NOT EXISTS nessusraw ('  

for i in range(len(columnDataType)):
    createTableStatement = createTableStatement + '\n' + columnName[i] + ' ' + columnDataType[i] + ','

## left in as an example: remove last comma and cap it
createTableStatement = createTableStatement[:-1] + ');'
# Add primary key 
#createTableStatement = createTableStatement + '\n' + 'PRIMARY KEY(ID));'

# Create Database & Table
createDB = 'CREATE DATABASE IF NOT EXISTS sagedb;\n\n'
useDB = 'USE sagedb;\n\n'

file1 = open('createNsTbl.sql','w')
file1.write(createDB)
file1.write(useDB)
file1.write(createTableStatement)
file1.close()

# Insert Nessus scans
file2 = open('insertRows.sql','w')
file2.write(insertRows(df, 'nessusraw')) 
file2.close
