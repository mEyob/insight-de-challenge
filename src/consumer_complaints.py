import os
import argparse
import multiprocessing
import subprocess
from csv import reader, writer
from collections import Counter
from datetime import datetime
from itertools import islice


def parse_list(line, date_index, prod_index, comp_index):
    '''Parse date, product, and company values from a given line
    inputs:
        'line': input line (type list).
        'date_index': index of date string in the list (type int)
        'prod_index': index of product (type int)
        'comp_index': index of company (type int)
    output: a tuple (product, year, company) extracted from input line
    '''
    year = None
    product = None
    company = None
    try:
        date = datetime.strptime(line[date_index], '%Y-%m-%d')
        year = date.year
        product = line[prod_index]
        company = line[comp_index]
    except:
        pass
    return product, year, company

def most_common(lst):
    '''Returns the relative frequency of the most common element in the 
    list 'lst' as a percentage''' 
    occurance_counter = Counter(lst)
    most_common_item = occurance_counter.most_common()[0][1]
    return round( 100 * most_common_item / len(lst) )

def collect_stats(queue):
    '''
    Returns statistics calculated from the dictionaries stored in the FIFO 'queue'.
    The return variable 'stats' is a list of tuples where the tuple elements are:
    product, year, total number of companies with at least one complaint, and
    highest percentage of total complaints filed against one company.
    '''
    stats = []
    data_dict ={}

    while not queue.empty():
        data = queue.get()
        for product, year_companies in data.items():
            for year, companies in year_companies.items():
                try:
                    data_dict[product][year] = data_dict[product].get(year, []) + companies
                except KeyError:
                    data_dict[product] = {year: companies}
    for product, year_companies in data_dict.items():
        for year, companies in year_companies.items():
            record = (product, year, len(companies), len(set(companies)), most_common(companies))
            stats.append(record)
    return stats

def write(output_file_path, data):
    '''A function for writing a list of comma separated rows into file'''
    with open(output_file_path, 'w') as csvfile:
        file_writer = writer(csvfile)
        file_writer.writerows(data)

def read_chunk(input_file_path, start, end, date_index, product_index, company_index, queue):
    '''A function for reading a set of lines from a particular file. Date, 
    product and company information is extracted from each line and stored in a 
    data dictionary having following form 
    {
        product1: {
            year1: companies, 
            year2: companies,
            ...
            },
        product2: {
            year1: companies, 
            year2: companies,
            ...
            }
        ...
    }

    inputs:
        'input_file_path': path to input file
        'start' and 'end': starting and ending line numbers 
        'date_index', 'product_index', 'company_index': expected positions of date, 
        product and company information in each line of the given file
        'queue': A FIFO queue for storing the data dictionary
    '''
    data_dict = {}
    with open(input_file_path, 'r') as in_file:
        csv_reader = reader(in_file)
        for line in islice(csv_reader, start, end):
            product, year, company = parse_list(line, date_index, product_index, company_index)
            product = product.lower()
            company = company.lower()
            if all([product, year, company]):
                try:
                    data_dict[product][year] = data_dict[product].get(year, []) + [company]
                except KeyError:
                    data_dict[product] = {year: [company]}

    queue.put(data_dict)

def chunkfy(file_path, num_of_workers):
    '''A function that divids a file (file_path) into approximately equal sizes 
    to determine the chunk_size each worker in num_of_workers will work on
    '''
    num_of_lines = subprocess.run(['wc', '-l', file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    num_of_lines = int(num_of_lines.stdout.split()[0]) - 1
    chunk_size = num_of_lines // num_of_workers
    return chunk_size, num_of_lines

def main(input_file_path, output_file_path, num_of_workers):
    '''
    Takes an input file, divides the file into chunks and assigns each 
    chunk to a an individual process, collects the result from each process and 
    writes a summary statistics to an output file.
    inputs:
        input_file_path: input file with comma separated values
        output_file_path: path for writing final output
        num_of_workers: number of processes to work on the input file concurrently.
    '''
    processes = []
    queue = multiprocessing.Manager().Queue()

    with open(input_file_path, 'r') as in_file:
        header = reader(in_file).__next__()
        
        try:
            date_index = header.index('Date received')
            product_index = header.index('Product')
            company_index = header.index('Company')
        except ValueError as e:
            print('One or more of the necessary columns cannot be found')
            raise e

        start = 1
        chunk_size, num_of_lines = chunkfy(input_file_path, num_of_workers)
        end = start + chunk_size
        for _ in range(num_of_workers):
            # start process
            process = multiprocessing.Process(target=read_chunk, args=(input_file_path, start, end, date_index, product_index, company_index, queue))
            processes.append(process)
            process.start()
            start = end
            end = min(start + chunk_size, num_of_lines) + 1
        
        for p in processes:
            p.join()
    stats = collect_stats(queue)
    stats.sort()
    write(output_file_path, stats)



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("inFilePath", help='Path to input file')
    parser.add_argument("outFilePath", help='Path to output file')
    parser.add_argument("-n","--numWorkers", help='Number of worker processes', type=int, default=4)
    args = parser.parse_args()

    main(args.inFilePath, args.outFilePath, args.numWorkers)
