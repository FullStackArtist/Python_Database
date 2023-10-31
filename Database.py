import os
import csv
import pandas as pd
import pyfiglet
from rich import print
import shutil
from datetime import date,datetime
import subprocess as sp

def drop(sql):
    sql=sql.split(" ")
    try:
        flag=validate(sql[-1])
        if flag==1:
            table_backup(sql[-1],sql[-1]+"bkp"+str(date.today()))
            os.remove("bin/TABLE_INFORMATION/"+sql[-1]+".csv")
            print("Table drop")
            return 1
        else:
            return " TABLE NOT FOUND "
    except Exception as e:
        print(e)
        return " TABLE NOT FOUND "
        

def table_backup(table,bkp):
    shutil.copy("bin/TABLE_INFORMATION/"+table+".csv","rbin/TABLE_BKP")
    os.rename("rbin/TABLE_BKP/"+table+".csv","rbin/TABLE_BKP/"+bkp+".csv")
    return ' '
    

def truncate(sql):
    sql=sql.split(" ")
    try:
        flag=validate(sql[-1])
        if flag==1:
            now = datetime.now()
            bkp=sql[-1]+"TRUNCATE_bkp"+str(now.strftime("%d-%m-%Y-%H-%M-%S"))
            table_backup(sql[-1],bkp)
            df=pd.read_csv("bin/TABLE_INFORMATION/"+sql[-1]+".csv",index_col=False,sep = ',')
            df=df.columns
            #os.remove("bin/TABLE_INFORMATION/"+sql[-1]+".csv")
            with open("bin/TABLE_INFORMATION/"+sql[-1]+".csv","+w") as fp:
                print("here")
                wp=csv.writer(fp)
                wp.writerow(df)
            print(" TRUNCATED TABLE ")
            return 1
        else:
            return " TABLE NOT FOUND "
            
    except Exception as e:
        print(e)
        return " TABLE NOT FOUND "

def display():
    print("-"*150)
    print("                           _            ")   
    print(" \  / /\  |_) |\/|  /\    | \ |_)   /|   / \  /| ")
    print("  \/ /--\ | \ |  | /--\__ |_/ |_)    | o \_/ o | ")
    print(" "*150)
    print("-"*150)
    print(" "*150)
    print("WELCOME TO VARMA_DB DATABASE WITH VERSION 1.0.1")
    print("FOLLOWING STATEMENT CAN BE PERFORMED ON THE VARMA_DB CREATE,INSERT,DELETE,SELECT,DROP,DELETE,TRUNCATE")
    print(" "*150)
command=[]
class Entry_Point():
    def __init__(self):
        if os.path.exists("bin/SCHEMA_INFORMATION") and os.path.exists("rbin/TABLE_BKP"):
            pass
        else:
            os.mkdir("bin")
            os.mkdir("rbin")
            os.mkdir("bin/SCHEMA_INFORMATION")
            os.mkdir("bin/TABLE_INFORMATION")
            os.mkdir("rbin/TABLE_BKP")
        with open("bin/SCHEMA_INFORMATION/SCHEMA_INFORMATION.txt","+a") as fp:
            if os.stat("bin/SCHEMA_INFORMATION/SCHEMA_INFORMATION.txt").st_size==0:
                fp.write("%s\n" % "SCHEMA_INFORMATION")
        with open("bin/SCHEMA_INFORMATION/TABLE_INFORMATION.txt","+a") as fp:
            if os.stat("bin/SCHEMA_INFORMATION/TABLE_INFORMATION.txt").st_size==0:
                fp.write("%s\n" % "TABLE_INFORMATION")
                

def validate_columns(syntax):
    for i in syntax:
        if i=="" or i==" ":
            return 0
    else:
        return 1


def schema_information(table,syntax):
    with open("bin/SCHEMA_INFORMATION/SCHEMA_INFORMATION.txt","a") as fp:
        fp.write("%s\n" % table)
        fp.write("%s\n" % str(syntax))
    with open("bin/SCHEMA_INFORMATION/TABLE_INFORMATION.txt","a") as fp:
        fp.write("%s\n" % table)
    return

def backup(sql):
    with open("bin/HISTORY.txt","a+") as fp:
        for i in sql:
            fp.write("%s\n" % i)
        return
def validate(table):
    with open("bin/SCHEMA_INFORMATION/TABLE_INFORMATION.txt","r") as fp:
        for i in fp:
            if str(i.rstrip())==str(table):
                return 1
        return 0
        
def desc(sql):
    sql=sql.split(" ")
    if len(sql)==3:
        flag=validate(sql[-1])
        if flag==0:
            df=pd.read_csv("bin/TABLE_INFORMATION/"+sql[-1]+".csv",index_col=False,sep = ',')
            print("TABLE_NAME "+sql[-1])
            print("COLUMNS_NAME "+str(list(df.head(0))))
            return 1

        else:
            return "TABLE DOES NOT EXIST"
    else:
        return "INVALID STATEMENT"

def create(sql):
    command.append(sql)
    sql=sql.split(" ")
    try:
        if sql[0]=="CREATE" and sql[1]=="TABLE":
            syntax=str(sql[2])
            i=syntax.rfind("(")
            if i==-1 and syntax[-1]!=")":
                return "INVALID SELECT STATEMENT"
            flash=validate(syntax[0:i])
            if flash==1:
                return "TABLE ALREADY EXITS"
            table=syntax[0:i]
            syntax=syntax[i+1:-1]
            #if str(syntax[0])=="(" and str(syntax[-1])==")":
                #syntax=syntax[1:len(syntax)-1].split(",")
            syntax=syntax.split(",")
            status=validate_columns(syntax)
            if status==0:
                return "INVALID SELECT STATEMENT"
            with open("bin/TABLE_INFORMATION/"+table+".csv","+a") as fp:
                wp=csv.writer(fp)
                if len(syntax)==0:
                    return "INVALID SELECT STATEMENT"
                else:
                    wp.writerow(syntax)
            schema_information(table,syntax)
            return 1
            #else:
            #    return 0
        else:
            return "INVALID CREATE STATEMENT"
    except Exception as e:
        print(e)
        return "INVALID CREATE STATEMENT"

def where(wr):
    where=""
    l=[]
    for i in wr:
        where=""
        if i.upper()=="OR" or i.upper()=="AND":
            l.append(i.lower())
        
        for j in range(0,len(i)):
            if i[j]=="=":
                where=where+"="+i[j:]+""
                l.append(where)
                break
            elif i[j]=="!":
                where=where+i[j:]+""
                break
            else:
                where=where.upper()+i[j]
    where=" ".join(l)
    return where

def dwhere(wr):
    where=""
    l=[]
    for i in wr:
        where=""
        if i.upper()=="OR" or i.upper()=="AND":
            l.append(i.lower())
        
        for j in range(0,len(i)):
            if i[j]=="=":
                where=where+"!"+i[j:]+""
                l.append(where)
                break
            elif i[j]=="!":
                print(i[j])
                where=where+"="+i[j+1:]+""
                l.append(where)
                print(where)
                break
            else:
                where=where.upper()+i[j]
    where=" ".join(l)
    return where


def delete(sql,bsql):
    sql=sql.replace('"',"'")
    bsql=bsql.replace('"',"'")
    sql=sql.split(" ")
    bsql=bsql.split(" ")
    try:
        if sql[1].upper()!="FROM" and len(sql)<5:
            return " INVALID INSERT STATEMENT  "
        flash=validate(sql[2].upper())
        if flash==1:
            df=pd.read_csv("bin/TABLE_INFORMATION/"+sql[2].upper()+".csv",index_col=False,sep = ',')
            wr=bsql[sql.index("WHERE")+1:]
            w=dwhere(wr)
            print('from delete '+w)
            print(df)
            print(type(w))
            df=df.query(w)
            print(df)
            print("-"*100)
            #print(ddf)
            print("-"*100)

            #wr=bsql[w.index("==")+1:]
            #print([w.index("==")+1:])
            #print(df.drop(wr,inplace = True))
            return 1
        else:
            return " TABLE DOES NOT EXIST "+str(sql[2])
    except Exception as e:
        print(e)
        return " INVALID INSERT STATEMENT  "

def select(sql,bsql):
    sql=sql.replace('"',"'")
    bsql=bsql.replace('"',"'")
    sql=sql.split(" ")
    bsql=bsql.split(" ")
    flag=""
    cols=[]
    wr=""
    
    try:
        if len(sql)>=4:
            table=sql[sql.index("FROM")+1]
            flash=validate(table)
            if flash==1:
                df=pd.read_csv("bin/TABLE_INFORMATION/"+table+".csv",index_col=False,sep = ',')
                if sql[1]=="*":
                    if "WHERE" in sql:
                        wr=bsql[sql.index("WHERE")+1:]
                        w=where(wr)
                        df=df.query(w)
                        print(df)
                        return 1
                    else:
                        print(df)
                else:
                    cols=sql[1].split(",")
                    status=validate_columns(cols)
                    print("status")
                    if "WHERE" in sql:
                        wr=bsql[sql.index("WHERE")+1:]
                        wr=where(wr)
                    if status==1 and len(wr)>0:
                        df=df.query(wr)
                        df=df.get(cols)
                        print(df)
                        return 1
                    else:
                        print("here")
                        print(df.get(cols))
                        return 1
            else:
                return "TABLE DOES NOT EXIST"
            return 1
        else:
            return "INVALID SELECT STATEMENT "
            
    except Exception as e:
        print(e)
        return "INVALID SELECT STATEMENT"
def open_table(sql):
    sql=sql.split(" ")
    flash=validate(sql[-1])
    if len(sql)!=3:
        return " INVALID TABLE NAME "
    try:
        if flash==1:
            programName = "notepad.exe"
            file="bin/TABLE_INFORMATION/"+sql[-1]+".csv"
            sp.Popen([programName, file])
            return 1
        else:
            return " TABLE DOES NOT EXIST "+sql[-1]
    except Exception as e:
        #print(e)
        return "INVALID TABLE NAME"+sql[-1]

    
def cols_validate(table,icols,ival):
    icols=icols.split(",")
    ival=ival.split(",")
    i=len(icols)
    j=len(ival)
    insert=[]
    print(i,j)
    #print(icols,ival)
    df=pd.read_csv("bin/TABLE_INFORMATION/"+table+".csv",index_col=False,sep = ',')
    for col in df.columns:
        flag=0
        for t in range(0,len(icols)):
            #print(icols[t].upper(),str(col))
            if icols[t].upper().strip()==str(col):
                #print(icols[t].upper(),str(col))
                i=i-1
                j=j-1
                flag=1
                insert.append(str(ival[t]))
                break
        if flag!=1:
            insert.append("NULL")
            
    print(insert)
    if i==0 and j==0:
        return insert
    else:
         return "INVALID"
                


 
def insert(sql):
    sql=sql.split(" ")
    icols=" "
    ival=" "
    try:
        if len(sql)>=5:
            if sql[0].upper()=="INSERT" and sql[1].upper()=="INTO":
                flash=validate(sql[2].upper())
                if flash!=1:
                    return " TABLE DOES NOT EXIST "+str(sql[2])
                for i in range(3,len(sql)):
                    if sql[i].upper()!="VALUES":
                        icols=icols+str(sql[i])
                    else:
                        ival=" ".join(sql[i+1:])
                        break
                if ival==" ":
                    return " INVALID TABLE NAME "
                icols=icols.replace("(","")
                icols=icols.replace(")","")
                ival=ival.replace("(","")
                ival=ival.replace(")","")
                ival=ival.replace("'","")
                flash=validate_columns(icols.split(","))
                if flash==1:
                    flash=validate_columns(ival.split(","))
                else:
                    return " INVALID INSERT STATEMENT  "
                if flash==1:
                    insert=cols_validate(sql[2].upper(),icols,ival)
                    if insert!="INVALID":
                        with open("bin/TABLE_INFORMATION/"+sql[2]+".csv","+a") as fp:
                            wp=csv.writer(fp)
                            wp.writerow(insert)                        
                            return 1
                    else:
                        return " INVALID INSERT STATEMENT  "


        else:
            return " INVALID INSERT STATEMENT  "
    except Exception as e:
        print(e)
        return " INVALID INSERT STATEMENT  "
def help(sql):
    count='->'
    flag=0
    if len(sql)>1:
        with open('FORMAT_SQL.txt', 'r') as fp:
            lines=fp.readlines()
            print("-"*150)
            for line in lines:
                if line.find(sql[1])!=-1:
                    flag=1
                    print(" {} {}".format(count,line.strip()))
            

    else:
        file1 = open('FORMAT_SQL.txt', 'r')
        lines = file1.readlines()
        for line in lines:
            print(" {} {}".format(count,line.strip()))
    if flag==0:
        return 0
    else:
        return 1


def commit():
    pass
def rollback():
    pass
def update():
    pass

def clear():
    #print("******************** COMMAND EXECUTING ********************")
    os.system('CLS')
    command.append("CLS")
def history():
    command.append("HISTORY")
    if len(command)==0:
        print("******************** NO HISTORY ********************")
        return
    print("-"*150)
    print(" | No: | COMMAND EXECUTED ")
    for i in range(0,len(command)):
        print("-"*150)
        print(" |  "+str(i)+"  | "+str(command[i]))
    return


def main():
    global command
    print("-"*150)
    sql=input("SQL-> ")
    while True:
        if sql=="EXIT":
            return
        elif sql.upper() =="CLS" or sql.upper()=="CLEAR":
            clear()
            backup(sql.upper())
            print("-"*150)
            sql=input("SQL-> ")
            
        elif sql.upper()=="HISTORY":
            history()
            backup(command)
            print("-"*150)
            sql=input("SQL-> ")
        elif sql[0:6].upper()=="CREATE":
            command.append(sql.upper())
            flag=create(sql.upper())
            if flag==1:
                print("-"*150)
                print(" TABLE CREATE ")
                sql=input("SQL->")
            elif flag!=0:
                print("-"*150)
                print(flag)
                sql=input("SQL->")
        elif sql[0:6].upper()=="SELECT":
            bsql=sql
            command.append(sql)
            flag=select(sql.upper(),bsql)
            if flag==1:
                print("-"*150)
                sql=input("SQL->")
            else:
                print("-"*150)
                print("******************** INVALID COMMAND ********************")
                print(flag)
                print(sql)
                sql=input("SQL->")
        elif sql[0:4].upper()=="DESC":
            flag=desc(sql)
            command.append(sql.upper())
            if flag==1:
                print("-"*150)
                sql=input("SQL->")
            else:
                print("******************** INVALID COMMAND ********************")
                print(flag)
                print("-"*150)
                sql=input("SQL-> ")
        elif sql[0:4].upper()=="DROP":
            flag=drop(sql.upper())
            command.append(sql.upper())
            if flag==1: 
                print("-"*150)
                sql=input("SQL->")
            else:
                print("******************** INVALID COMMAND ********************")
                print(flag)
                print("-"*150)
                sql=input("SQL-> ")
        elif sql[0:8].upper()=="TRUNCATE":
            flag=truncate(sql.upper())
            command.append(sql.upper())
            if flag==1: 
                print("-"*150)
                sql=input("SQL->")
            else:
                print("******************** INVALID COMMAND ********************")
                print(flag)
                print("-"*150)
                sql=input("SQL-> ")
        elif sql[0:4].upper()=="OPEN":
            flag=open_table(sql.upper())
            if flag==1:
                print("-"*150)
                sql=input("SQL->")
            else:
                print("******************** INVALID COMMAND ********************")
                command.append(sql.upper())
                backup(sql.upper())
                print(flag)
                print("-"*150)
                sql=input("SQL-> ")
        elif sql[0:6].upper()=="INSERT":
            flag=insert(sql)
            if flag==1:
                print("-"*150)
                sql=input("SQL->")
            else:
                print("******************** INVALID COMMAND ********************")
                command.append(sql.upper())
                backup(sql.upper())
                print(flag)
                print("-"*150)
                sql=input("SQL-> ")                
        elif sql[0:4].upper()=="HELP":
            flag=help(sql.upper().split(" "))
            if flag==0:
                print(" SEARCH NOT FOUND PLEASE CHECK SPELLING ")
            print("-"*150)
            sql=input("SQL->")
        elif sql[0:6].upper()=="DELETE":
            flag=delete(sql.upper(),sql)
            if flag==1:
                print("-"*150)
                sql=input("SQL->")
            else:
                print("******************** INVALID COMMAND ********************")
                command.append(sql.upper())
                backup(sql.upper())
                print(flag)
                print("-"*150)
                sql=input("SQL-> ")            
        else:
            print("******************** INVALID COMMAND ********************")
            command.append(sql.upper())
            backup(sql.upper())
            print("-"*150)
            sql=input("SQL-> ")

if __name__=="__main__":
    Entry_Point()
    display()
    main()
    print("******************** Closed ********************")
