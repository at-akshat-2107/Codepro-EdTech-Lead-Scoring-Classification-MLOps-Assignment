##############################################################################
# Import the necessary modules
# #############################################################################
from utils import *
from constants import *

import warnings
warnings.filterwarnings("ignore")


###############################################################################
# Write test cases for load_data_into_db() function
# ##############################################################################

def test_load_data_into_db():
    """_summary_
    This function checks if the load_data_into_db function is working properly by
    comparing its output with test cases provided in the db in a table named
    'loaded_data_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_get_data()

    """
    load_data_into_db()

    conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    print("Reading data from loaded_data table")
    loaded_data = pd.read_sql('select * from loaded_data', conn)
    
    conn_ut = sqlite3.connect(DB_PATH+UNIT_TEST_DB_FILE_NAME)
    print("Reading data from loaded_data_test_case table")
    test_case_loaded_data = pd.read_sql('select * from loaded_data_test_case', conn_ut)
    
    conn.close()
    conn_ut.close()
    
    assert test_case_loaded_data.equals(loaded_data), "Dataframes are not same"
    

###############################################################################
# Write test cases for map_city_tier() function
# ##############################################################################
def test_map_city_tier():
    """_summary_
    This function checks if map_city_tier function is working properly by
    comparing its output with test cases provided in the db in a table named
    'city_tier_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_map_city_tier()

    """
    map_city_tier()
    
    conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    print("Reading data from city_tier_mapped table")
    city_tier_map = pd.read_sql('select * from city_tier_mapped', conn)
    
    conn_ut = sqlite3.connect(DB_PATH+UNIT_TEST_DB_FILE_NAME)
    print("Reading data from city_tier_mapped_test_case table")
    test_case_city_tier = pd.read_sql('select * from city_tier_mapped_test_case', conn_ut)
    
    conn.close()
    conn_ut.close()
    
    assert test_case_city_tier.equals(city_tier_map), "Dataframes are not same"
    

###############################################################################
# Write test cases for map_categorical_vars() function
# ##############################################################################    
def test_map_categorical_vars():
    """_summary_
    This function checks if map_cat_vars function is working properly by
    comparing its output with test cases provided in the db in a table named
    'categorical_variables_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'
    
    SAMPLE USAGE
        output=test_map_cat_vars()

    """   
    map_categorical_vars()
    
    conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)
    print("Reading data from categorical_variables_mapped table")
    cat_var_map = pd.read_sql('select * from categorical_variables_mapped', conn)
    
    conn_ut = sqlite3.connect(DB_PATH+UNIT_TEST_DB_FILE_NAME)
    print("Reading data from categorical_variables_mapped_test_case table")
    test_case_cat_var = pd.read_sql('select * from categorical_variables_mapped_test_case', conn_ut)
    
    conn.close()
    conn_ut.close()
    
    assert test_case_cat_var.equals(cat_var_map), "Dataframes are not same"
    

###############################################################################
# Write test cases for interactions_mapping() function
# ##############################################################################    
def test_interactions_mapping():
    """_summary_
    This function checks if test_column_mapping function is working properly by
    comparing its output with test cases provided in the db in a table named
    'interactions_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_column_mapping()

    """ 
    interactions_mapping()
    
    conn = sqlite3.connect(DB_PATH+DB_FILE_NAME)  
    print("Reading data from interactions_mapped table")
    int_map = pd.read_sql('select * from interactions_mapped', conn)
    
    conn_ut = sqlite3.connect(DB_PATH+UNIT_TEST_DB_FILE_NAME)
    print("Reading data from interactions_mapped_test_case table")
    test_case_int_map = pd.read_sql('select * from interactions_mapped_test_case', conn_ut)
    
    conn.close()
    conn_ut.close()
    
    assert test_case_int_map.equals(int_map), "Dataframes are not same"