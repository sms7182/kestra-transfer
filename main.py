import os
import bson
import psycopg2
import base64
from pathlib import Path
import json
from bson.binary import Binary
from psycopg2.extras import Json

PG_CONN = {
         
      }

BACKUP_PATH = "./backup"

def infer_pg_type(value):
          if isinstance(value, bool):
              return "BOOLEAN"
          elif isinstance(value, (int)):
              return "BIGINT"
          elif isinstance(value, float):
              return "NUMERIC"
          elif isinstance(value, bool):
              return "BOOLEAN"
          elif isinstance(value, str):
              return "TEXT"
          elif isinstance(value, dict) or isinstance(value, list):
              return "JSONB"
          else:
              return "TEXT"

def create_table(conn, collection_name, sample_doc):
          cursor = conn.cursor()
          fields = [(k, infer_pg_type(v)) for k, v in sample_doc.items()]
          columns = ", ".join([f"{key} {pg_type}" for key, pg_type in fields])
          createdclmn=",".join([f"{key}" for key,pg_type in fields])
          create_query = f"CREATE TABLE IF NOT EXISTS {collection_name} (id SERIAL PRIMARY KEY, {columns});"
          cursor.execute(create_query)
          conn.commit()
          cursor.close()
          return createdclmn

def create_table_for_child(conn,table_name,columns):
    cursor=conn.cursor()
    
    # columns = ", ".join(fields)
    # columns=columns+","+ref_name
    
    create_query = f"CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY, {columns});"
    cursor.execute(create_query)
    conn.commit()
    cursor.close()
    



def recur_data(lst,tb_name,ref_id,ref_name,conn):
    # if ind<len(keysIn):
    #                     field=fields.pop(ind)
    #                     # create_table_for_child(conn,field,)
    #                     rs=recur_data(value,conn,collection_name,id)

    processed_vls=[]
    ind=0
    id=""
    for vl in lst:
        if isinstance(vl,Binary):
            id=base64.b64encode(vl).decode('utf-8')
            
            processed_vls.append(id)
        elif isinstance(vl,dict):
            # fields=vl.keys()
            
           
            fields = [(k, infer_pg_type(v)) for k, v in vl.items()]
            replacements = {
            'Fetch': 'Fetchk',
            'Order': 'Orderk'
            }

            columns = ", ".join([f"{replacements.get(key, key)} {pg_type}" for key, pg_type in fields])

            # columns = ", ".join([f"{key} {pg_type}" for key, pg_type in fields])
            columns=columns+f",{ref_name}_id TEXT"
            create_table_for_child(conn,tb_name,columns)
            vl[f"{ref_name}_id"]=ref_id
            # cmn=", ".join([f"{key} " for key, pg_type in fields])
            cmn=", ".join([f"{replacements.get(key, key)}" for key, pg_type in fields])
            # created=",".join([f"{key}" for key, pg_type in fields])
            created=", ".join([f"{replacements.get(key, key)}" for key, pg_type in fields])
            created=created+f",{ref_name}_id"
            createdcolumns=created.split(",")
            cmn=cmn+f",{ref_name}_id"
            insert_child(conn,tb_name,cmn,vl,ref_id,createdcolumns)
            # processed_vls.append(Json(rs))
        elif isinstance(vl,list):
            rs=recur_data(vl,"",id)

        else:
            processed_vls.append(vl)
    return processed_vls


def insert_child(conn,tb,columns,vls,parent_id,clmns):
    createdcolumns=list(clmns)
    cursor=conn.cursor()
    i_v=[]
    currentColumns=[]
    for k,vl in vls.items():
        currentColumns.append(k)
        if isinstance(vl, Binary):
            id=base64.b64encode(vl).decode('utf-8')
            i_v.append(id)
                    
        elif isinstance(vl, (dict,list)):
            pass
        else:
            i_v.append(vl)
    ind=0
    for clm in currentColumns:
        if clm=="Fetch":
            clm="Fetchk"
        if clm=="Order":
            clm="Orderk"
        columntype=infer_pg_type(i_v[ind])
        alterQ=f"ALTER TABLE {tb} add COLUMN IF NOT EXISTS  {clm} {columntype};"
        cursor.execute(alterQ)
        ind=ind+1

    # i_v.append(parent_id)
    placeholders = ", ".join(["%s"] * len(i_v))
    insert_query = f"INSERT INTO {tb} ({columns}) VALUES ({placeholders});"
    cursor.execute(insert_query, i_v)
    conn.commit()
    cursor.close()
    
def insert_data(conn, collection_name, documents,clmns):
          columns=list(clmns)
          cursor = conn.cursor()
          for doc in documents:
              keysIn=doc.keys()  
              fields=[]
              for key in keysIn:
                fields.append(key)
              keys = ", ".join(keysIn)
              processed_values = []
              id=""
              ind=0
              for value in doc.values():
                if isinstance(value, Binary):
                    id=base64.b64encode(value).decode('utf-8')
                    processed_values.append(id)
                elif isinstance(value, (dict,list)):
                    columntype=infer_pg_type(value)
                    alterQ=f"ALTER TABLE {collection_name} add COLUMN IF NOT EXISTS {fields[ind]} {columntype};"
                    cursor.execute(alterQ)
                    rs=recur_data(value,fields[ind],id,collection_name,conn)
                    processed_values.append(Json(rs))
                
                else:

                    # if  fields[ind] not in columns:
                    columntype=infer_pg_type(value)
                    alterQ=f"ALTER TABLE {collection_name} add COLUMN IF NOT EXISTS {fields[ind]} {columntype};"
                    cursor.execute(alterQ)
                    # columns.append(fields[ind])
                        
                    processed_values.append(value)
                ind+=1
        
              placeholders = ", ".join(["%s"] * len(doc))
              insert_query = f"INSERT INTO {collection_name} ({keys}) VALUES ({placeholders});"
              vls=list(doc.values())
              cursor.execute(insert_query, processed_values)
          conn.commit()
          cursor.close()

conn = psycopg2.connect(**PG_CONN)
backup_dir = Path(BACKUP_PATH)
for bson_file in backup_dir.glob("*.bson"):
    collection_name = bson_file.stem
    with open(bson_file, "rb") as f:
        data = bson.decode_all(f.read())
        if not data:
                continue
        sample_doc = data[0]
        print(collection_name)
        columnstr=create_table(conn, collection_name, sample_doc)
        columns=columnstr.split(",")
        insert_data(conn, collection_name, data,columns)
conn.close()
print("succesfull")
