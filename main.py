import base64
import datetime
import pandas as pd
import functions_framework
from google.auth import default
from googleapiclient.discovery import build
from google.cloud import bigquery
from oauth2client.service_account import ServiceAccountCredentials
from google.api_core.exceptions import NotFound
from pandas.api.types import is_integer_dtype

VIEW_ID = '0123456789' #ga4 property id
START_DATE = 'Y-m-d' #e.g.'2022-01-01'  
END_DATE = 'Y-m-d' #e.g.'2022-03-31'

BIGQUERY_PROJECT = 'your-bigquery-project'
BIGQUERY_DATASET = 'your-dataset'
BIGQUERY_TABLE = 'your-table'

def initialize_analyticsreporting():
    # Use automatic authentication with Cloud Functions service account
    credentials, project_id = default()
    analytics = build('analyticsreporting', 'v4', credentials=credentials)
    return analytics

def get_report(analytics):
    return analytics.reports().batchGet(
        body={
            'reportRequests': [
                {
                    'viewId': VIEW_ID,
                    'dateRanges': [{'startDate': START_DATE, 'endDate': END_DATE}],
                    'metrics': [
                        {'expression': 'ga:newUsers'},
                        {'expression': 'ga:users'},
                        {'expression': 'ga:sessions'},
                        {'expression': 'ga:bounceRate'},
                        {'expression': 'ga:avgSessionDuration'},
                        {'expression': 'ga:pageviewsPerSession'},
                        {'expression': 'ga:goal1Completions'},
                    ],
                    'dimensions': [
                        {'name': 'ga:date'},
                        {'name': 'ga:sourceMedium'},
                    ],
                    'pageSize': 100000
                }
            ]
        }
    ).execute()

def response_to_dataframe(response):
    list_rows = []
    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

        for row in report.get('data', {}).get('rows', []):
            dimensions = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])

            row_data = {}
            for header, dimension in zip(dimensionHeaders, dimensions):
                row_data[header] = dimension

            for values in dateRangeValues:
                for metricHeader, value in zip(metricHeaders, values.get('values')):
                    row_data[metricHeader.get('name')] = value

            list_rows.append(row_data)

    return pd.DataFrame(list_rows)

def upload_to_bigquery(df, project_id, dataset_id, table_id):
    # Rename columns from 'ga:' to 'gs_'
    df.columns = [col.replace('ga:', 'gs_') for col in df.columns]
    
    # Ensure gs_date column is present and of type DATE
    if 'gs_date' not in df.columns:
        raise ValueError("The DataFrame does not contain a 'gs_date' column.")

    df['gs_part_date'] = pd.to_datetime(df['gs_date'], format='%Y%m%d', errors='coerce').dt.date

    # Move gs_part_date to the first position
    cols = ['gs_part_date'] + [col for col in df.columns if col != 'gs_part_date']
    df = df[cols]

    # Convert data types explicitly
    for col in df.columns:
        if col in ['gs_part_date', 'gs_sourceMedium']:
            continue  # Skip conversion for the partition date column as it's already in the correct format
        try:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        except ValueError:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
            except ValueError:
                df[col] = df[col].astype(str)  # Fallback to string if all else fails

    bigquery_client = bigquery.Client(project=project_id)
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    schema = []

    # Define the schema of the table based on DataFrame columns
    for col in df.columns:
        # Ensure gs_language is treated as a STRING in BigQuery
        if col in ['gs_sourceMedium']:
            bq_type = 'STRING'
        else:
            dtype = df[col].dtype
            if pd.api.types.is_integer_dtype(dtype):
                bq_type = 'INTEGER'
            elif pd.api.types.is_float_dtype(dtype):
                bq_type = 'FLOAT'
            elif pd.api.types.is_bool_dtype(dtype):
                bq_type = 'BOOLEAN'
            elif pd.api.types.is_datetime64_any_dtype(dtype) or (dtype == 'object' and col == 'gs_part_date'):
                bq_type = 'DATE'
            else:
                bq_type = 'STRING'  # Default type

        schema.append(bigquery.SchemaField(col, bq_type))

    # Define the partitioning options
    partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="gs_part_date",  # Name of the column to use for partitioning
    )

    # Create a new table if it doesn't exist
    try:
        bigquery_client.get_table(table_ref)
    except NotFound as error:
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = partitioning
        bigquery_client.create_table(table)
        print(f"Created table {table_id}")

    # Upload data to BigQuery
    load_job = bigquery_client.load_table_from_dataframe(df, table_ref)
    load_job.result()
    print(f"Data uploaded to table")

def hello_world(request):
    analytics = initialize_analyticsreporting()
    response = get_report(analytics)
    df = response_to_dataframe(response)
    upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_TABLE)
    return "Done!"