DB_FILE_NAME = 'lead_scoring_data_cleaning.db'
DB_PATH = "/home/airflow/dags/Lead_scoring_data_pipeline/"
DATA_DIRECTORY = '/home/airflow/dags/Lead_scoring_data_pipeline/data/'

INTERACTION_MAPPING = "/home/airflow/dags/Lead_scoring_data_pipeline/mapping/interaction_mapping.csv" # : path to the csv file containing interaction's mappings
INDEX_COLUMNS_TRAINING = ['created_date', 'city_tier', 'first_platform_c',
                'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped',
                'referred_lead', 'app_complete_flag']

INDEX_COLUMNS_INFERENCE = ['created_date', 'city_tier', 'first_platform_c',
                'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped',
                'referred_lead']

NOT_FEATURES = ['created_date', 'assistance_interaction', 'career_interaction',
                'payment_interaction', 'social_interaction', 'syllabus_interaction']

 