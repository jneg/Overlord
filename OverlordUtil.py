#!/usr/bin/env python

# @author Javon Negahban
# OverlordUtil is a utility library for Overlord.
# OverlordUtil handles the validation and parsing of data.

import datetime

daysOfWeek = {'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3, 'Friday': 4, 'Saturday': 5, 'Sunday': 6}

# Returns true if the summary has keys status, error, and tables and the tables is a list or tuple. False otherwise.
def validSummaryDict(s):
  return "tables" in s and type(s['tables']) in (list, tuple)

# Returns true if the table has keys tid, inserted, updated, and deleted. False otherwise.
def validTableDict(t):
  return all(k in t for k in ('state', 'name', 'inserted', 'updated', 'deleted'))

# Returns true if the day is a day of week. False otherwise.
def validDay(day):
  return day in daysOfWeek

# Returns a corresponding integer value for a day of the week (Monday-Sunday = 0-6)
def parseDay(day):
  return daysOfWeek[day]

# Returns the current UTC in the desired format.
def currentUTC():
  return datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

# Returns the current time with brackets for logging.
def timelog():
  return "[" + currentUTC() + "]   "

