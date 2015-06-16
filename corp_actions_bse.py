""" Downloads Corp Actions for a given security, We are mainly interested in
bonus and splits, but doesn't harm to keep noting others.

BSE corp actions data is one of the worsts you'd find, we are just going to
try harder as much as we can. eg.
 - For a number of splits there's no pre and post split value
 - For a number of corporate actions there is no ex_date. We use heuristics
   there to get previous business day before record date if it's there.
"""
