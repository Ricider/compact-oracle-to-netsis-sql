def sql2oracle(postUrl,tableName,uniqueField,code_c):
    cursor.execute("SELECT * FROM "+tableName+" WHERE "+uniqueField+" IS NULL")
    row=cursor.fetchone()
    
    if row:
        columns = [column[0] for column in cursor.description]
        columnNames = {columns[i]:i for i in range(len(columns))}
        payload={columns[i]: row[i] for i in range(len(columns)) if columns[i]!=uniqueField and columns[i]!="PartyId"}
        
        sqlreturner=requests.get(url=postUrl+"?q="+code_c+"='"+row[columnNames[code_c]]+"'"  , auth=HTTPBasicAuth(httpAuthUsr, httpAuthPwd))
        
        method="patch"
        if ((sqlreturner.json()["count"])>0):
            patchid=(sqlreturner.json()["items"][0][uniqueField])
            if uniqueField=="PartyId": patchid=(sqlreturner.json()["items"][0]["PartyNumber"])
        else: method="post"
            
        if method=="post":
            r=requests.post(url=postUrl, json=payload  , auth=HTTPBasicAuth(httpAuthUsr, httpAuthPwd))
            cursor.execute("SELECT * FROM "+tableName+" WHERE "+code_c+"='" + str(r.json()[code_c])+"'")
            rowCnt=cursor.rowcount
            if r.status_code==201:
                if not rowCnt!=-1:
                    print(str(payload) + " sunucuya yeni giris olarak eklendi")
                    cursor.execute("UPDATE "+tableName+" SET "+uniqueField+"='"+str(r.json()[uniqueField])+"' WHERE "+code_c+"='"+row[columnNames[code_c]]+"'")
                    conn.commit()
                else: print (str(payload)+" "+str(r.status_code)+" hata kodu sebebiyle gonderilemedi")
            
        elif method=="patch":
            if True:
                r=requests.patch(url=postUrl+"/"+str(patchid), json=payload , auth=HTTPBasicAuth(httpAuthUsr, httpAuthPwd))
                print (payload)
                print (postUrl+"/"+str(patchid))
                cursor.execute("SELECT * FROM "+tableName+" WHERE "+code_c+"='" + str(r.json()[code_c])+"'")
                rowCnt=cursor.rowcount
                if r.status_code==200:
                    if not rowCnt!=-1:
                        print(str(payload) + " sunucuda modifiye edildi")
                        cursor.execute("UPDATE "+tableName+" SET "+uniqueField+"='"+str(r.json()[uniqueField])+"'WHERE "+code_c+"='"+row[columnNames[code_c]]+"'")
                        conn.commit()
                    else: print (str(payload)+" "+str(r.status_code)+" hata kodu sebebiyle gonderilemedi")

def dequeueSql(tableName,uniqueField,code_c):
    cursor.execute("SELECT TOP 1 * FROM "+tableName+" WHERE "+uniqueField+" IS NULL")

    row=cursor.fetchone()
    columns = [column[0] for column in cursor.description]
    columnNames = {columns[i]:i for i in range(len(columns))}
    
    cursor.execute("UPDATE "+tableName+" SET "+uniqueField+"= '-1' WHERE "+code_c+" = '"+row[columnNames[code_c]]+"'")
    conn.commit()
    
def sendData(postUrl,tableName,uniqueField,code_c):
    try:
        sql2oracle(postUrl,tableName,uniqueField,code_c)
    except Exception as e:
        print("error: ", e)
        dequeueSql(tableName,uniqueField,code_c)
