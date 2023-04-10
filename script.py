import csv
from collections import defaultdict
import argparse
import sys
import mysql.connector
import os

parser = argparse.ArgumentParser()
parser.add_argument('-i', '--input', nargs='+', dest="workFiles", help="Files to process", required=True)
parser.add_argument('-x', '--xytech', dest='work_order', help='path to work order', required=True)
parser.add_argument('-v', '--verbose', action='store_true', help='Output to console if flagged') #if flagged, output to console, otherwise dont
parser.add_argument('--db', action='store_true', help='output to DB')
parser.add_argument('--csv', action='store_true', help='output to CSV')
parser.add_argument('--output', required=True, choices=['db', 'csv'], help='output format (db or csv)')
# parser.add_argument('baselight_file', type=str, help='path to baselight file')
args = parser.parse_args()

script_runner = os.getlogin()
print(script_runner)

# Determine the file type based on its extension
def get_file_type(file_path):
    extension = os.path.splitext(file_path)[1]
    if extension == '.txt':
        with open(file_path, 'r') as file:
            first_line = file.readline().strip()
            if first_line.startswith('/net/flame-archive'):
                return 'flame'
            else:
                return 'baselight'
    elif extension == '.csv':
        return 'csv'
    else:
        return None

#Open Xytech file
xytech_folders = []

read_xytech_file = open(args.work_order, "r")
for line in read_xytech_file:
    if "/" in line:
        xytech_folders.append(line)

# Loop through input files
for file_path in args.workFiles:
    # Open input file
    with open(file_path, "r") as read_input_file:
        # Read each line from input file
        for line in read_input_file:
            line_parse = line.split(" ")
            current_folder = line_parse.pop(0)
            sub_folder = current_folder.replace("/images1/", "")
            new_location = ""
            # Folder replace check
            for xytech_line in xytech_folders:
                if sub_folder in xytech_line:
                    new_location = xytech_line.strip()
            first = ""
            pointer = ""
            last = ""
            for numeral in line_parse:
                # Skip <err> and <null>
                if not numeral.strip().isnumeric():
                    continue
                # Assign first number
                if first == "":
                    first = int(numeral)
                    pointer = first
                    continue
                # Keeping to range if succession
                if int(numeral) == (pointer + 1):
                    pointer = int(numeral)
                    continue
                else:
                    # Range ends or no sucession, output
                    last = pointer
                    if first == last:
                        if args.verbose:
                            print("%s %s" % (new_location, first))
                        else:
                            break
                    else:
                        if args.verbose:
                            print("%s %s-%s" % (new_location, first, last))
                        else:
                            break
                    first = int(numeral)
                    pointer = first
                    last = ""
            # Working with last number each line
            last = pointer
            if first != "":
                if first == last:
                    if args.verbose:
                        print("%s %s" % (new_location, first))
                    else:
                        break
                else:
                    if args.verbose:
                        print("%s %s-%s" % (new_location, first, last))
                    else:
                        break

# mydb = mysql.connector.connect(
#     host = "127.0.0.1",
#     user="root",
#     password="the123world",
#     database="project2_467"
# )

# mycursor = mydb.cursor()
