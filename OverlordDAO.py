#!/usr/bin/env python

# @author Javon Negahban
# @author Andrew Ly 
# OverlordDAO is the data access object for Overlord which performs reads and writes on OverlordDB.

import pymysql

from Conn import getConn
from OverlordUtil import currentUTC

# Returns all schedules from OverlordDB
def getSchedules():
  with getConn().cursor() as cur:
    cur.execute("select * from Schedule")
    return cur.fetchall()

# Returns the schedule with the given schedule id from OverlordDB
def getSchedule(scheduleId):
  with getConn().cursor() as cur:
    cur.execute("select * from Schedule where sched_id = %s", (scheduleId,))
    return cur.fetchone()

# Returns all scripts in the given schedule id in script order from OverlordDB
def getScripts(scheduleId):
  with getConn().cursor() as cur:
    cur.execute("select * from Script s join Run r on r.sid = s.sid where r.sched_id = %s order by r.script_order ASC", (scheduleId,))
    return cur.fetchall()

# Returns the script with the given script id from OverlordDB
def getScript(scriptId):
  with getConn().cursor() as cur:
    cur.execute("select * from Script where sid = %s", (scriptId,))
    return cur.fetchone()

# Inserts a script run summary into OverlordDB
def insertScriptStatus(sid, execType, startTime, endTime, status, notes):
  with getConn().cursor() as cur:
    try:
      cur.execute("insert into ScriptRunSummary (sid, exec_type, start_time, end_time, status, notes) values (%s, %s, %s, %s, %s, %s)", (sid, execType, startTime, endTime, status, notes))
      getConn().commit()
    except pymysql.Error as e:
      print(e.args[0], e.args[1])
      getConn().rollback()

# Returns the last insert id from OverlordDB.
def getLastInsertID():
  with getConn().cursor() as cur:
    cur.execute("select last_insert_id()")
    return cur.fetchone()['last_insert_id()']

# Inserts a table status into OverlordDB.
def insertTableStatus(hid, tid, inserted, modified, deleted):
  with getConn().cursor() as cur:
    try:
      cur.execute("insert into TableStatus (hid, tid, inserted, modified, deleted) values (%s, %s, %s, %s, %s)", (hid, tid, inserted, modified, deleted))
      getConn().commit()
    except pymysql.Error as e:
      print(e.args[0], e.args[1])
      getConn().rollback()

# Returns the table with the given table name from OverlordDB.
def getTable(tableName):
  with getConn().cursor() as cur:
    cur.execute("select * from DD_Table where name = %s", (tableName,))
    return cur.fetchone()

# Returns the email recipients from OverlordDB.
def getRecipients():
  with getConn().cursor() as cur:
    cur.execute("select * from Email")
    return cur.fetchall()

# Inserts a message into the journal of OverlordDB.
def log(source, message):
  with getConn().cursor() as cur:
    try:
      cur.execute("insert into Journal (moment, source, message) values (%s, %s, %s)", (currentUTC(), source, message))
      getConn().commit()
    except pymysql.Error as e:
      print(e.args[0], e.args[1])
      getConn().rollback()

