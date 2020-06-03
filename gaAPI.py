######### Libraries ################
import argparse
from googleapiclient.discovery import build
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools
import pandas as pd
import time
import os
import errno
from datetime import datetime, timedelta
from time import sleep


#################################################
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')
######### Function: connect to the GA ################
######### Function will be called outside ######
def initialize_analyticsreporting(CLIENT_SECRETS_PATH):
  """Initializes the analyticsreporting service object.
  Returns:
    analytics an authorized analyticsreporting service object.
  """
  # Parse command-line arguments.
  parser = argparse.ArgumentParser(
      formatter_class=argparse.RawDescriptionHelpFormatter,
      parents=[tools.argparser])
  flags = parser.parse_args([])

  # Set up a Flow object to be used if we need to authenticate.
  flow = client.flow_from_clientsecrets(
      CLIENT_SECRETS_PATH, scope=SCOPES,
      message=tools.message_if_missing(CLIENT_SECRETS_PATH))

  # Prepare credentials, and authorize HTTP object with them.
  # If the credentials don't exist or are invalid run through the native client
  # flow. The Storage object will ensure that if successful the good
  # credentials will get written back to a file.
  storage = file.Storage('analyticsreporting.dat')
  credentials = storage.get()
  if credentials is None or credentials.invalid:
      credentials = tools.run_flow(flow, storage, flags)

  http = credentials.authorize(http=httplib2.Http())

  # Build the service object.
  analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI,cache_discovery=False)

  return analytics

######### Functions: configure the report ################
######## Will be called inside function: get_ga_data() ######
def get_report(analytics, start_date, end_date, view_id, metrics, dimensions, segments):
  # Use the Analytics Service Object to query the Analytics Reporting API V4.
  return analytics.reports().batchGet(
      body={
        "reportRequests":
  [
    {
        "viewId": view_id,
        "dateRanges": [{"startDate": start_date, "endDate": end_date}],
        "metrics": metrics,
        "dimensions": dimensions,
        "segments": segments
    }
  ]
      }
  ).execute()

######### Functions: transform the raw data to dataframe ################
#### Also print sample level if sampling happens #############
##### Will be called inside of function: get_ga_data()#########
def print_response(response):
    list = []
    # get report data
    for report in response.get('reports', []):
    # set column headers
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
        rows = report.get('data', {}).get('rows', [])

    for row in rows:
        # create dict for each row
        dict = {}
        dimensions = row.get('dimensions', [])
        dateRangeValues = row.get('metrics', [])

        # fill dict with dimension header (key) and dimension value (value)
        for header, dimension in zip(dimensionHeaders, dimensions):
            dict[header] = dimension

        # fill dict with metric header (key) and metric value (value)
        for i, values in enumerate(dateRangeValues):
            for metric, value in zip(metricHeaders, values.get('values')):
            #set int as int, float a float
                if ',' in value or '.' in value:
                    dict[metric.get('name')] = float(value)
                else:
                    dict[metric.get('name')] = int(value)

        list.append(dict)

    df = pd.DataFrame(list)
    return df

######### Functions: save data to a csv file  ################
####### Function will be called outside ##############
def save_df_to_csv(df, path, filename):
    file_loc = path + '/' + filename
    if not os.path.exists(os.path.dirname(file_loc)):
        try:
            os.makedirs(os.path.dirname(file_loc))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise
    return df.to_csv(path_or_buf = file_loc, index= False )