import csv
import os
import sys

maxInt = sys.maxsize

while True:
    # decrease the maxInt value by factor 10 
    # as long as the OverflowError occurs.

    try:
        csv.field_size_limit(maxInt)

        # Path to the csv_path containing CSV files
        csv_path = 'D:/STUDY/Projects/FYP/updated/Wiki_Data_Crawl/en/csv'

        # Path and name of the merged file
        merged_path = 'en_merged.csv'

        # Get a list of all CSV files in the csv_path
        csv_files = [file for file in os.listdir(csv_path) if file.endswith('.csv')]

        # Open the merged file in write mode
        with open(merged_path, 'w', newline='', encoding='utf8') as outfile:
            writer = csv.writer(outfile)
            
            # Iterate through each CSV file
            for filename in csv_files:
                file_path = os.path.join(csv_path, filename)
                
                # Open the individual file in read mode
                with open(file_path, 'r', encoding='utf8') as infile:
                    reader = csv.reader(infile)
                    
                    # Copy the header from the first file
                    if filename != csv_files[0]:
                        next(reader)                        
                        
                    # Copy the remaining rows
                    for row in reader:
                        writer.writerow(row)
                        
                # Close the individual file
                infile.close()


        print('Done.')

        break
    except OverflowError:
        maxInt = int(maxInt/10)
        print(maxInt)

