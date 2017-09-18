# Imports
import sys
import os
import requests
import urllib.request
import csv
from datetime import datetime
import time
import concurrent.futures 
import threading
import socket
#from multiprocessing import Pool

#osname = sys.platform
#if osname == "darwin":
#	print("MacOS detected: set No Proxy to avoid system config crash.")
os.environ['no_proxy'] = "*"

http_proxies = {
  "http": None,
  "https": None
}

# initialize counters
pagesize = 10
timeout = 5 # wait up to 5 seconds for data source to respond
socket.setdefaulttimeout(timeout)

# name of report file containing results of all data sources
common_report_file = 'data.gov_mon_rpt_' + datetime.now().strftime("%m_%d_%Y") + '.csv'

with open(common_report_file, 'w') as f:
    writer = csv.writer(f)
    #write header row
    writer.writerow(['URL', 'Name', 'Description', 'Resource State', 'Protocol', 'Status', 'Link State'])

# lock to keep threads from writing report file over each other
report_file_lock = threading.Lock()

if len(sys.argv) > 1:
    search_string = sys.argv[1]
else:
    search_string = "climate"
print("Searching for resources containing: ", search_string)

# reusable functions
def handle_ftplink(resources, report_row, i):
    ftplinks = 0
    good = 0
    bad = 0
    ftplinks += 1
    # use urllib since requests package is only for HTTP
    ftplink = urllib.request.urlopen(resources[i]['url'], data=None)
    report_row.append('FTP')
    report_row.append('NA')
    good += 1
    report_row.append('GOOD')
    return ftplinks, good, bad

def handle_httplink(resources, report_row, i):
    httplinks = 0
    good = 0
    bad = 0
    testlink = requests.get(resources[i]['url'], timeout=timeout, proxies=http_proxies)
    report_row.append('HTTP')
    report_row.append(testlink.status_code)
    httplinks += 1
    if testlink.status_code == 200:
        good += 1
        report_row.append('GOOD')
    else:
        bad += 1
        report_row.append('BAD')
    return httplinks, good, bad

# worker function
def get_results(row_range):
    # Initialize counts for this run
    num_resources = 0
    start = row_range[0]
    end = row_range[1]
    resource_report = []
    resources = []
    httplinks = 0
    ftplinks = 0
    good = 0
    bad = 0
    unknown = 0


    report_file = common_report_file 

    # Now get all results page by page
    for startrow in range (start, end, pagesize):
        parameters = {'q': search_string, 'rows': pagesize, 'start': startrow}
        try:
            r = requests.get('https://catalog.data.gov/api/3/action/package_search', params = parameters, proxies=http_proxies)
            json_dict = r.json()
            num_results_total = json_dict['result']['count']
            num_resources_in_response = len(json_dict['result']['results'])
        except:
		#skip
            continue
        results = []
        resources = []
        previous_url = None
        # build list of resources within results
        for i in range(0, num_resources_in_response):
            try:
                results.append(json_dict['result']['results'][i])
                for j in range(0, len(results[i]['resources'])):
                    rsrc = results[i]['resources'][j]
                # check for URL same as previous - if so, skip
                    if rsrc['url'] == previous_url:
                        continue
                    else:
                        previous_url = rsrc['url']
                        resources.append(rsrc)
            except:
		    # just skip bad JSON resource
                continue

        # now go through and test all resources
        num_resources = len(resources)
        for i in range(0, num_resources):
            report_row = [resources[i]['url'], resources[i]['name'],resources[i]['description'], resources[i]['state']]
            # initialize internal function return values
            f = 0 # ftplinks count
            h = 0 # httplinks count
            g = 0 # good count
            b = 0 # bad count
            # test resource URL
            try:
                # Check HTTP resources
                if resources[i]['resource_locator_protocol'] == 'HTTP' or resources[i]['url'][:4] == 'http':
                    h, g, b = handle_httplink(resources, report_row, i)
                # Check FTP resources
                if resources[i]['url'][:3] == 'ftp':
                    f, g, b = handle_ftplink(resources, report_row, i)
            except requests.exceptions.RequestException:
                bad += 1
                report_row.append('UNKNOWN')
                report_row.append('NONE')
                report_row.append('BAD')
            except:
                # maybe bad JSON - check URL directly
                try:
                    if resources[i]['url'][:3] == 'ftp':
                        f, g, b = handle_ftplink(resources, report_row, i)
                    else:
                        if resources[i]['url'][:4] == 'http':
                            h,g,b = handle_httplink(resources, report_row, i)
                        else:
                            unknown += 1
                            report_row.append('UNKNOWN')
                            report_row.append('NONE')
                            report_row.append('UNKNOWN')
                except:
                    bad += 1
                    report_row.append('UNKNOWN')
                    report_row.append('NONE')
                    report_row.append('BAD')

            httplinks += h
            ftplinks += f
            good += g
            bad += b

    	    # write result row to CSV
            with report_file_lock:
                with open(report_file, 'a') as f:
                    writer = csv.writer(f)
                    writer.writerow(report_row)

    # create return result
    results = [num_resources,httplinks, ftplinks, good, bad, unknown]
    return results


# Main logic ...
def main():

	# We will report elapsed time
	start_time = time.time()

	# Get count of total results
	parameters = {'q': search_string, 'rows': 0}
	r = requests.get('https://catalog.data.gov/api/3/action/package_search', params = parameters)
	json_dict = r.json()
	num_results_total = json_dict['result']['count']

	r = requests.get('https://catalog.data.gov/api/3/action/package_search?q=climate&rows=0', timeout=10, proxies=http_proxies)
	json_dict = r.json()
	print ('Request success = ', json_dict['success'])
	num_results_total = json_dict['result']['count']
	print('Total results: ', num_results_total)


	# Create thread pool and run
	poolsize = 10 
	results_per_thread = 10 
	batch_size = 100
	num_results_test = num_results_total # for testing only
	# create list of ranges
	ranges = []

	# Reset result counts
	total_resources = 0
	good = 0
	bad = 0
	unknown = 0
	httplinks = 0
	ftplinks = 0

	for batch_no in range(0, num_results_test, batch_size):
    		ranges.append([batch_no, batch_no+batch_size-1])
	with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
	   list_of_results = executor.map(get_results, ranges) 
		
	# Consolidate counts
	for each_result in list_of_results:
		total_resources += each_result[0]
		httplinks += each_result[1]
		ftplinks += each_result[2]
		good += each_result[3]
		bad += each_result[4]
		unknown += each_result[5]

	# Print summary of run
	print ('Total number of resources: ', total_resources)
	print ('HTTP Links: ', httplinks)
	print ('FTP links: ', ftplinks)
	print ('Good links: ', good)
	print ('Bad links: ', bad)
	print ("Unknown: ", unknown)
	print ('See detailed report in ', common_report_file)
	# Print elapsed time needed to create report
	elapsed_time = time.time() - start_time
	print ('Elapsed Time: ', round(elapsed_time), ' seconds')

if __name__ == '__main__':
	main()









