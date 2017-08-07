#!/bin/bash


# This script is to be run daily to download last few days data

if [ $# -lt 1 ]; then
	echo "Usage : ${0} <db-url>"
fi

# First make sure you have all the required packages installed. Assumes
# Virtualenv python package installed.

# 0. Create a virtual environment and install all packages.

VIRTUALENV=$(which virtualenv)

if [ -d 'venv' -a -e 'venv/bin/python' ]; then
	echo "Virtualenv exists...";

else
	echo "Virtualenv is not properly setup. Likely you have not run "
	echo '`firsttime.sh`. First run that and then continue'
	exit -1
fi

PATH=venv/bin/:$PATH

VENV_PYTHON=$(which python)
echo $VENV_PYTHON

# Date from which we need to apply changes

from_date=`date --date='2 weeks ago' +%d-%m-%Y`

# 1. Download historical data for last 2 weeks (name changes get applied)


$VENV_PYTHON get_stocks_nse2.py --yes --from $from_date || {
		echo "Error downloading all stocks historical data.";
		exit -1;
	}

# FIXME: When bonus split for specific time period is implemented, fix below.
# 2. Bonus splits data

$VENV_PYTHON corp_actions_nse.py --all || {
		echo "Error downloading corp actions data.";
		exit -2;
	}

# 3. Indices data

$VENV_PYTHON get_indices_nse.py --all --yes --from $from_date || {
		echo "Downloading Indices data.";
		exit -3;
	}

echo << EOF

Following data was downloaded.
1. All stocks historical data for last 2 weeks.
2. All indices historical data for last 2 weeks.
3. All Bonus/Split historical data for last 2 weeks.(ratios not applied,
   so the original data remains intact.)

EOF

exit 0
