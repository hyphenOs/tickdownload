""" Stores a scrip in an HDF5 file. Assumes the data exists locally."""

# This is more of a test scrip where we want to explore all ideas.

import pandas as pd

from corp_actions_nse import get_corp_action_csv

csv_filename = '500209.csv'

COL_NAMES = ["Date", "Open Price", "High Price", "Low Price", "Close Price", "No.of Shares", "Deliverable Quantity"]

#
infy = pd.read_csv(csv_filename, index_col='Date', usecols=COL_NAMES, parse_dates=True)
infy.columns = list('OHLCVD')

infy = infy[::-1] # BSE scrip files are reverse latest first

print infy[:10]

hdf_filename = 'infy.h5'


c = get_corp_action_csv('infy')

corp_actions = {'corp_actions': c}

h5store = pd.HDFStore(hdf_filename)

h5store['infy'] = infy
h5store.get_storer('infy').attrs.corp_actions = c

h5store.close()

# open again for reading
h5store = pd.HDFStore(hdf_filename)

infy = h5store['infy']
corp_actions = h5store.get_storer('infy').attrs.corp_actions

print infy[:10]
for act in corp_actions:
    if act.action in ['B', 'S']:
        ts = pd.Timestamp(act.ex_date)
        ratio = act.ratio
        infy['O'][infy.index < ts] = infy['O'] * ratio
        infy['H'][infy.index < ts] = infy['H'] * ratio
        infy['L'][infy.index < ts] = infy['L'] * ratio
        infy['C'][infy.index < ts] = infy['C'] * ratio
        infy['V'][infy.index < ts] = infy['V'] / ratio
        infy['D'][infy.index < ts] = infy['D'] / ratio

print infy[:10]
h5store.close()
