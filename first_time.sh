#!/bin/bash


# Whenever you are trying to populate the data for the first time, run
# this script.

if [ $# -lt 1 ]; then
	COMMAND=`basename ${0}`
	echo "Usage : ${COMMAND} <db-url>"
	echo "       db-url : use SQLAlchemy Supported format like sqlite:///<path-to-file>"
	echo ""
	exit -1
fi

DB_PATH=${1}

echo "Using ${DB_PATH} for storing data."

# First make sure you have all the required packages installed. Assumes
# Virtualenv python package installed.

# 0. Create a virtual environment and install all packages.

VIRTUALENV=$(which virtualenv)

if [ -d 'venv' -a -e 'venv/bin/python' ]; then
	echo "Virtualenv looks already installed and setup."

else
	echo "Installing Virtualenv in venv directory."
	$VIRTUALENV venv && venv/bin/pip install -r requirements.txt
fi

PATH=venv/bin/:$PATH

VENV_PYTHON=$(which python)
echo $VENV_PYTHON


# Note we could potentially run the following things in parallel, but we won't
# to avoid too many 'requests' going to NSE simultaneously.

# 1. Since this is first time, Populate All Scrips Table.

$VENV_PYTHON all_stocks_list.py

# 2. First download all historical data (Name changes already applied this.)

$VENV_PYTHON get_stocks_nse.py --yes --dbpath ${DB_PATH} || {
		echo "Error downloading all stocks historical data.";
		exit -1;
	}

# 3. Bonus splits data

$VENV_PYTHON corp_actions_nse.py --all --dbpath ${DB_PATH} || {
		echo "Error downloading corp actions data.";
		exit -2;
	}

# 4. Indices data

$VENV_PYTHON get_indices_nse.py --all --yes --dbpath ${DB_PATH} || {
		echo "Downloading Indices data.";
		exit -3;
	}

echo << EOF

Following data was downloaded.
1. All stocks historical data (with current name changes applied)
2. All indices historical data.
3. All Bonus/Split historical data (ratios not applied, so the original
   data remains intact.)

EOF

exit 0
