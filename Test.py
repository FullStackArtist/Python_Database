'''import pandas as pd
import os
import csv
df=pd.read_csv("bin/TABLE_INFORMATION/DEMO.csv",index_col=False,sep = ',')
df=df.columns
with open("bin/TABLE_INFORMATION/File1.csv","+w") as fp:
    wp=csv.writer(fp)
    wp.writerow(df)'''
l='column1,,column2'
l=l.split(",")
for i in l:
    if i==" " or i == "":
        print("empty")
    else:
        print(i)
