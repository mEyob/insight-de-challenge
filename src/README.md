This module accepts a csv file and writes some key statistics to an output file.
It can be run as follows:

```consumer_complaints.py input_file output_file [number_of_workers]```

*input_file*: Is a csv file that contains the input data. The file should contain 'product', 'date' and 'company' columns as given 
in [this](https://cfpb.github.io/api/ccdb/fields.html) format

*output_file*: Is a csv file where each line is of the form

- product 
- year
- total number of complaints received for that product and year
- total number of companies receiving at least one complaint for that product and year
- highest percentage of total complaints filed against one company for that product and year

*number_of_workers*: is an optional parameter (default=4) that determines the number of concurrent processes.
