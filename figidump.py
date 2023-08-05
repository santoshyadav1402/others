import json
import urllib.request
import urllib.parse
import csv
import time

csv_file_path = "D:/PythonProject/figi/testdata.csv"

# Define the output CSV file path
output_csv_file_path = 'D:/PythonProject/figi/output.csv'


# Define the chunk size
max_jobs_per_request = 10


# Initialize an empty list to store chunks of ticker data
ticker_chunks = []

# Open the CSV file and read it in chunks
with open(csv_file_path, 'r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    chunk_tickers = []
    
    for row in csv_reader:
        job = {"idType":"TICKER","idValue":row.get("Ticker"),"exchCode":row.get("Exchange Code")}
        
        if job:
            chunk_tickers.append(job)
        
        if len(chunk_tickers) == max_jobs_per_request:
            ticker_chunks.append(chunk_tickers)
            chunk_tickers = []
    
    # Append any remaining tickers as the last chunk
    if chunk_tickers:
        ticker_chunks.append(chunk_tickers)


openfigi_apikey = ''  # Put API Key here
def map_jobs(jobs):
    
    handler = urllib.request.HTTPHandler()
    opener = urllib.request.build_opener(handler)
    openfigi_url = 'https://api.openfigi.com/v2/mapping'
    request = urllib.request.Request(openfigi_url, data=bytes(json.dumps(jobs), encoding='utf-8'))
    request.add_header('Content-Type','application/json')
    if openfigi_apikey:
        request.add_header('X-OPENFIGI-APIKEY', openfigi_apikey)
    request.get_method = lambda: 'POST'
    connection = opener.open(request)
    if connection.code != 200:
        raise Exception('Bad response code {}'.format(str(connection.status_code)))
    data = json.loads(connection.read().decode('utf-8'))
    if 'ratelimit-remaining' in connection.info():  # Check if the key exists
        # Access the key's value  
        ratelimitremaining = connection.info()["ratelimit-remaining"]
        if ratelimitremaining == 0:
            interval_duration = connection.info()["ratelimit-reset"]
            print(f"Remaining rate limit: {ratelimitremaining}\nRate limit reset: {interval_duration}")
            time.sleep(interval_duration)
    
    
    return data


def job_results_handler(jobs, job_results):
    
    for job, result in zip(jobs, job_results):
        job_str = job.get("idValue")
        figis_str = ','.join([d['figi'] for d in result.get('data', [])])
        result_str = figis_str or result.get('error')
        output_strings =[{'figicode':result_str,'idValue':job_str,'exchCode':job.get("exchCode")}]
        print(output_strings)
        # Write the output strings to the CSV file
        with open(output_csv_file_path, 'a', newline='') as output_csv_file:
            fieldnames = ["figicode", "idValue","exchCode"]
            csv_writer = csv.DictWriter(output_csv_file, fieldnames=fieldnames)
            csv_writer.writerows(output_strings)







def main():
    
    # Now you have the tickers divided into chunks of 100 tickers each
    for index, chunk in enumerate(ticker_chunks):
        print(f"Chunk {index + 1}\n")
        jobs = chunk
        job_results = map_jobs(jobs)
        job_results_handler(jobs, job_results)

main()