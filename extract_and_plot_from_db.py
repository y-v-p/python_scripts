import argparse
import MySQLdb
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as dates
from matplotlib.ticker import AutoMinorLocator
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta

def extractDFfromDB(hostname, username, password, database, table, columns, colnames, seqnos):
    conn = MySQLdb.connect(host=hostname, user=username, passwd=password, db=database)
    cursor = conn.cursor()

    cols = ""
    for item in columns:
        cols = cols + item + ", "
    cols = cols[:-2]

    tmpstr = ""
    for item in seqnos:
        tmpstr = tmpstr + "ROW_NUMBER = " + str(item) + " OR "
    tmpstr = tmpstr[:-3]
    dbcommand = "select " + cols + " from " + table + " where " + tmpstr
    # print dbcommand

    # extracting from the database
    cursor.execute(dbcommand);
    rows = cursor.fetchall()

    # convert extracted rows into pandas dataframe
    df = pd.DataFrame( [[ij for ij in i] for i in rows] )
    df.rename(columns=colnames, inplace=True);

    return df
#end def extractDFfromDB

def extractRowsFromTimeDB(hostname, username, password, database, table, begintime, endtime):
    conn = MySQLdb.connect(host=hostname, user=username, passwd=password, db=database)
    cursor = conn.cursor()

    dbcommand = "select ROW_NUMBER, TIMESTART, TIMEEND from " + table + " where TIMESTART <= \"" + endtime + "\" and TIMEEND >= \"" + begintime + "\""
    # print dbcommand

    # extracting from the database
    cursor.execute(dbcommand);
    rows = cursor.fetchall()

    # converting into panda dataframe
    df = pd.DataFrame( [[ij for ij in i] for i in rows] )
    df.rename(columns={0: 'ROW_NUMBER', 1: 'timestart', 2: 'timeend'}, inplace=True);
    return df
# end def extractRowsFromTimeDB


parser = argparse.ArgumentParser(description="")
parser.add_argument("beginT", help="begin time in YEAR-MM-DD HH:MM:SS format")
parser.add_argument("endT", help="end time in YEAR-MM-DD HH:MM:SS format")
parser.add_argument("col_1", help = "col_1 name as a condition to plot")
parser.add_argument("col_2", help = "col_2 name as a condition to plot")
parser.add_argument("col_3", help = "col_3 name as a condition to plot")
args = parser.parse_args()

begintime = args.beginT
endtime = args.endT
col_1 = int(args.col_1)
col_2 = int(args.col_2)
col_3 = int(args.col_3)

host = "your.database.host"
user = "db_user_name"
passwd = "db_user_password"
db = "db_name"

# getting information row number and start/end dates for table rows that
# belong to the specified (begintime - endtime) period
table = "TIMESTAMPS_TABLE"
timesDF = extractRowsFromTimeDB(host, user, passwd, db, table, begintime, endtime)
timesDF = timesDF.sort_values('timestart', ascending=True);
begintime_ = datetime.strptime(begintime, "%Y-%m-%d %H:%M:%S")

# creating a pandas dataframe with the extracted values
table_cols = ['ROW_NUMBER', 'COL1', 'COL2', 'COL3', 'VALUE', 'ERROR']
df_colnames = {0: 'row_number', 1: 'col_1', 2: 'col_2', 3: 'col_3', 4: 'value', 5: 'error'}
table = "VALUES_TABLE"
valuesDF = extractDFfromDB(host, user, passwd, db, table, table_cols, df_colnames, timesDF['row_num'])

# plotting value vs time for a particular condition specified in the input
x = []
y = []
x_err = []
y_err = []
for index, row in timesDF.iterrows():
    t1 = row['timestart']
    t2 = row['timeend']
    x.append(t1 + (t2-t1)/2)
    x_err.append((t2-t1)/2)
    tmp = valuesDF.loc[valuesDF['row_number'] == row['row_number']]
    tmp = tmp.loc[tmp['col_1'] == col_1]
    tmp = tmp.loc[tmp['col_2'] == col_2]
    tmp = tmp.loc[tmp['col_3'] == col_3]
    y.append(float(tmp['value'].to_string(index=False)))
    y_err.append(float(tmp['error'].to_string(index=False)))

plt.errorbar(x, y, xerr=x_err, yerr=y_err, fmt='o')
plt.gcf().subplots_adjust(bottom=0.2)
# plt.gca().tight_layout()
ax = plt.subplot(111)
ax.xaxis.set_minor_locator(dates.WeekdayLocator(byweekday=(5),interval=1))
ax.xaxis.set_minor_formatter(dates.DateFormatter('%d\n%a'))
ax.xaxis.set_major_locator(dates.MonthLocator())
ax.xaxis.set_major_formatter(dates.DateFormatter('\n\n\n%b %Y'))

ax.xaxis.grid(True, which="minor")
ax.yaxis.grid()
plt.ylabel("Value")
plt.xlabel("time")
plottitle = "Plotted for col_1" + str(col_1) + " col_2" + str(col_2) + " col_3" + str(col_3)
plt.title(plottitle)
# plt.gcf().autofmt_xdate()
oplotfile = "Values_for_" + str(col_1) + "_col1_" + str(col_2) + "_col_2_" + str(col_3) + "_col_3_for_times_" + tstartstr + "_" + tendstr + ".png"
plt.savefig(oplotfile)
