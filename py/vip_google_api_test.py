# Output files (data separated by pipe):
#  * Success contains the following:
#  ** original address
#  ** polling location as understood by the API
#
#  * Failure contains the following:
#  ** original address
#  ** error code or other (i.e. may include "SUCCESS" if no polling location was returned)

"""The reason for this application is to allow states to easily perform
regression tests against the VIP Google API. This script will take any number
of addresses from a CSV and use them to query the API. It will then generate
success and error log files for verification."""

import argparse
import csv
import urllib, urllib2
import json

def hit_api(address):
    """Queries the VIP Google API with the given address.

    Positional arguments:
    address -- A string of a voter's registered address

    """
    base_url = "https://pollinglocation.googleapis.com/"
    query = "{0}?apiversion={1}&electionid={2}&q={3}".format(
        base_url,
        "1.1",
        "2000",
        urllib.quote(address)
    )
    headers = {
        "Accept": "application/json"
    }

    request = urllib2.Request(query, None, headers)

    try:
        response = json.load(urllib2.urlopen(request, None, 3))
        return (address, response,)
    except urllib2.URLError as e:
        return (address, { 'locations': '', 'error': e, 'status': 'API_ERROR' },)
    except Exception as e:
        # Something more low level
        return (address, { 'locations': '', 'error': e, 'status': 'API_ERROR' },)

def parse_response(unparsed):
    """Parses out the API response into a dict to be consumed by the logger.
    
    Positional arguments:
    unparsed -- A tuple consisting of the original address and API response

    """
    address, response = unparsed
    locations = response.get('locations')
    
    if len(locations) > 0 and response.get('status') == 'SUCCESS':
        location = locations.pop().get('address') or {}

        polling_location = "{0} {1}".format(
            location.get('location_name',''),
            location.get('line1','')
        ).strip()

        polling_location += " " + "{0}, {1} {2}".format(
            location.get('city',''),
            location.get('state',''),
            location.get('zip','')
        ).strip()

        if polling_location.startswith(','):
            polling_location = ''

        parsed = {
            'response': response.get('status'),
            'registered address': address,
            'polling location': polling_location.strip()
        }


    else:
        parsed = {
            'response': response.get('status'),
            'registered address': address,
            'polling location': ''
        }

    return parsed

def log_response(result, success, error):
    """Logs the result to, either, the success or error logs.
    
    Positional arguments:
    result -- The dict to be written to the log
    success -- DictWriter for successful data
    error -- DictWriter for error data

    """

    if result.get('polling location') == '':
        error.writerow(result)
    else:
        success.writerow(result)
    

parser = argparse.ArgumentParser(description='''Process a list of registered
addresses and verify the polling locations with the Google API''')

parser.add_argument('--input', dest='input', type=argparse.FileType('r'),
                    help='the file containing the addresses')
parser.add_argument('--error', dest='error', type=argparse.FileType('w'),
                    default='error.log', help='the error log')
parser.add_argument('--success', dest='success', type=argparse.FileType('w'),
                    default='success.log', help='the success log')

error_headers = ('response', 'registered address', )
success_headers = ('registered address', 'polling location', )

args = parser.parse_args()
reader = csv.reader(args.input, delimiter='|')
error_log = csv.DictWriter(
    args.error,
    fieldnames=error_headers,
    extrasaction='ignore',
    delimiter='|',
    quoting=csv.QUOTE_MINIMAL
)
success_log = csv.DictWriter(
    args.success,
    fieldnames=success_headers,
    extrasaction='ignore',
    delimiter='|',
    quoting=csv.QUOTE_MINIMAL
)

responses = (hit_api(line[0]) for line in reader) #returns tuple
parsed_responses = (parse_response(response) for response in responses) #returns dict

error_log.writeheader()
success_log.writeheader()

for parsed in parsed_responses:
    log_response(parsed, success_log, error_log)
