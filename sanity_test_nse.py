# A sanity test for all nse download scrips.
# Sanity test should ensure that following downloads are still working
import random

# 1. Get a list of all nse symbols
from nse_utils import nse_get_all_stocks_list
nse_get_all_stocks_list(start=random.randint(1,10), count=2)
del nse_get_all_stocks_list

# 2. Get Corporate actions for a symbol ('infy')
from corp_actions_nse import get_corp_action_csv
get_corp_action_csv('INFY')
del get_corp_action_csv

# 3. Get a list of symbol name changes
from nse_utils import nse_get_name_change_tuples
nse_get_name_change_tuples()
del nse_get_name_change_tuples

# 4. Get NSE indices for an index
from get_indices_nse import get_index
get_index('NIfTy'.upper())
del get_index
