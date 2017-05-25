#!/usr/bin/env python

# @author Javon Negahban
# @author Andrew Ly
# Overlord is a scheduling system for the Data Warehouse team which schedules the database team scripts for Digital Democracy.
# Overlord is run as a background process by utilizing the apscheduler library.
# Overlord reads the scheduling information from OverlordDB.
# Overlord communicates with OverlordDAO in order to read from and write to OverlordDB.
# Overlord communicates with OverlordEmail for sending emails on script failures notifying the database team.
# Overlord communicates with OverlordUtil for utility functions.

import sys
import subprocess
import json
import datetime
import pytz
from apscheduler.schedulers.blocking import BlockingScheduler

from OverlordUtil import currentUTC, timelog, validDay, parseDay, validSummaryDict, validTableDict
from OverlordDAO import getScript, getScripts, getSchedule, getSchedules, insertScriptStatus, getLastInsertID, getTable, insertTableStatus
from Email import email

# Constructs a scheduler, schedules jobs, and starts the scheduler perpetually.
def initScheduler():
  scheduler = BlockingScheduler(timezone=pytz.utc)
  schedules = getSchedules()
  for schedule in schedules:
    time = (datetime.datetime.min + schedule['time']).time()
    if (schedule['day'] == 'Daily'):
      scheduler.add_job(retExecSchedule(schedule['sched_id'], 'Job'), 'cron', hour=time.hour, minute=time.minute, second=time.second)
      print(timelog() + "Overlord scheduled " + schedule['name'] + " (" + str(schedule['sched_id']) + ") " + schedule['day'] + " " + time.strftime("%H:%M:%S"))
    elif validDay(schedule['day']):
      scheduler.add_job(retExecSchedule(schedule['sched_id'], 'Job'), 'cron', day=parseDay(schedule['day']), hour=time['hour'], minute=time['minute'], second=time['second'])
      print(timelog() + "Overlord scheduled " + schedule['name'] + " (" + str(schedule['sched_id']) + ") " + schedule['day'] + " " + time.strftime("%H:%M:%S"))
  try:
    scheduler.start()
  except (KeyboardInterrupt, SystemExit):
    pass

# Returns a closure which executes the given schedule.
def retExecSchedule(scheduleId, execType):
  def clo():
    execSchedule(scheduleId, execType)
  return clo

# Executes the schedule by executing each script which the schedule runs.
def execSchedule(scheduleId, execType):
  schedule = getSchedule(scheduleId)
  if schedule is None:
    print(timelog() + "Schedule " + scheduleId + " not in OverlordDB")
    return
  print(timelog() + "Overlord executing " + schedule['name'] + " (" + str(schedule['sched_id']) + ")")
  outdated = False
  scripts = getScripts(schedule['sched_id'])
  for script in scripts:
    outdated = execScript(script['sid'], execType, outdated)
  print(timelog() + "Overlord finished " + schedule['name'] + " (" + str(schedule['sched_id']) + ")")

# Executes the script with the given absolute path, logs its summary report, and returns the outdated status.
def execScript(sid, execType, outdated):
  script = getScript(sid)
  if script is None:
    print(timelog() + "Script " + sid + " not in OverlordDB")
    return True
  print(timelog() + "Overlord executing " + script['path'] + script['name'] + " (" + str(script['sid']) + ") as a " + execType)
  if outdated == True:
    print(timelog() + script['path'] + script['name'] + " (" + str(script['sid']) + ") outdated")
    recordSummary(script['sid'], execType, currentUTC(), currentUTC(), 'Outdated', '', [])
    return True
  startTime = currentUTC()
  process = subprocess.Popen([script['path'] + script['name']], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  output, error = process.communicate()
  endTime = currentUTC()
  if process.returncode != 0:
    print(timelog() + script['path'] + script['name'] + " (" + str(script['sid']) + ") failed")
    recordSummary(script['sid'], execType, startTime, endTime, 'Failure', output, [])
    email(timelog() + script['name'] + " (" + str(script['sid']) + ")  failed", output)
    return True
  try:
    summary = json.loads(error)
  except ValueError, e:
    print(timelog() + script['path'] + script['name'] + " (" + str(script['sid']) + ") succeeded with no json")
    recordSummary(script['sid'], execType, startTime, endTime, 'Updated', output, [])
    return False
  if not validSummaryDict(summary):
    print(timelog() + script['path'] + script['name'] + " (" + str(script['sid']) + ") succeeded with invalid json")
    recordSummary(script['sid'], execType, startTime, endTime, 'Updated', output, [])
    return False
  print(timelog() + script['path'] + script['name'] + " (" + str(script['sid']) + ") succeeded")
  recordSummary(script['sid'], execType, startTime, endTime, 'Updated', output, summary['tables'])
  return False

# Records the script run summary into OverlordDB.
def recordSummary(sid, execType, startTime, endTime, status, notes, tables):
  insertScriptStatus(sid, execType, startTime, endTime, status, notes)
  hid = getLastInsertID()
  for t in tables:
    if validTableDict(t):
      table = getTable(t['name'])
      if table is not None and table['tid']:
        insertTableStatus(hid, table['tid'], t['inserted'], t['updated'], t['deleted'])

if __name__ == '__main__':
  print(timelog() + "Overlord starting")
  if len(sys.argv) >= 3 and sys.argv[1] == 'script':
    execScript(sys.argv[2], 'Script', False)
  elif len(sys.argv) >= 3 and sys.argv[1] == 'schedule':
    execSchedule(sys.argv[2], 'Schedule') 
  else:
    initScheduler()

