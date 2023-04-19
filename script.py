import csv
from collections import defaultdict
import argparse
import sys
import mysql.connector
import os
import datetime

parser = argparse.ArgumentParser()
parser.add_argument('-i', '--input', nargs='+', dest="workFiles", help="Files to process", required=True)
parser.add_argument('-x', '--xytech', dest='work_order', help='path to work order', required=True)
parser.add_argument('-v', '--verbose', action='store_true', help='Output to console if flagged') #if flagged, output to console, otherwise dont
parser.add_argument('--db', action='store_true', help='output to DB')
parser.add_argument('--csv', action='store_true', help='output to CSV')
parser.add_argument('--output', required=True, choices=['db', 'csv'], help='output format (db or csv)')
# parser.add_argument('baselight_file', type=str, help='path to baselight file')
args = parser.parse_args()

now = datetime.datetime.now()
print (now.strftime("%Y-%m-%d %H:%M:%S"))

mydb = mysql.connector.connect(
    host = "127.0.0.1",
    user="root",
    password="the123world",
    database="project2_467"
)
mycursor = mydb.cursor()

script_runner = os.getlogin()
print(script_runner)

#Open Xytech file
xytech_folders = []

read_xytech_file = open(args.work_order, "r")
for line in read_xytech_file:
    if "/" in line:
        xytech_folders.append(line)


# Loop through input files
if args.workFiles is None:
    print("You didnt select any BL/Flame files")
else:
    if args.output == "csv":
        csv_file = 'project2.csv'
        with open(csv_file, 'w', newline='') as file:
            writer = csv.writer(file)
            for file_path in args.workFiles:
                # Open input file
                with open(file_path, "r") as read_input_file:

                    # Check if input file is a Flame file
                    is_flame_file = file_path.startswith("Flame_")
                    is_baselight_file = file_path.startswith("Baselight_")
                    
                    # Extract Flame name and date
                    if args.output == "db":
                        if is_flame_file:
                            flame_name_date = os.path.splitext(os.path.basename(file_path))[0][6:]
                            flame_name, flame_date = flame_name_date.split("_")
                            print(flame_name_date)
                            query = "INSERT INTO data (script_runner, machine, user_file, file_date, submission_date) VALUES (%s, %s, %s, %s, %s)"
                            values = (script_runner, "Flame", flame_name, flame_date, now.strftime("%Y-%m-%d %H:%M:%S"))
                            mycursor.execute(query, values)
                            mydb.commit()
                        else:
                            baselight_name_date = os.path.splitext(os.path.basename(file_path))[0][10:]
                            baselight_name, baselight_date = baselight_name_date.split("_")
                            print(baselight_name)
                            query = "INSERT INTO data (script_runner, machine, user_file, file_date, submission_date) VALUES (%s, %s, %s, %s, %s)"
                            values = (script_runner, "Baselight", baselight_name, baselight_date, now.strftime("%Y-%m-%d %H:%M:%S"))
                            mycursor.execute(query, values)
                            mydb.commit()

                    # Read each line from input file
                    for line in read_input_file:
                            # Extract Flame name and date
                        if is_flame_file:
                            flame_parse = line.split(" ")
                            flame_storage = flame_parse.pop(0)
                            flame_current_folder = flame_parse.pop(0)
                            flame_new_location = ""
                            for xytech_line in xytech_folders:
                                if flame_current_folder in xytech_line:
                                    flame_new_location = xytech_line.strip()
                            first = ""
                            pointer = ""
                            last = ""
                            for numeral in flame_parse:
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
                                        if args.output == "db":
                                            print("DBBB")
                                            # query = "INSERT INTO frames (user_file, file_date, location, frames) VALUES (%s, %s, %s, %s)"
                                            # values = (flame_name, flame_date, flame_new_location, first)
                                            # mycursor.execute(query, values)
                                            # mydb.commit()
                                        elif args.output == "csv":
                                            writer.writerow([flame_new_location, first])
                                        else:
                                            print("None worked")
                                        if args.verbose:
                                            print("%s %s" % (flame_new_location, first))
                                        else:
                                            break
                                    else:
                                        if args.output == "db":
                                            ""
                                            # frame_range = "%s-%s" % (first, last)
                                            # query = "INSERT INTO frames (user_file, file_date, location, frames) VALUES (%s, %s, %s, %s)"
                                            # values = (flame_name, flame_date, flame_new_location, frame_range)
                                            # mycursor.execute(query, values)
                                            # mydb.commit()
                                        frame_range = "%s-%s" % (first, last)
                                        writer.writerow([flame_new_location, frame_range])
                                        if args.verbose:
                                            print("%s %s-%s" % (flame_new_location, first, last))
                                        else:
                                            break
                                    first = int(numeral)
                                    pointer = first
                                    last = ""

                            # Working with last number each line
                            last = pointer
                            if first != "":
                                if first == last:
                                    if args.output == "db":
                                        ""
                                        # query = "INSERT INTO frames (user_file, file_date, location, frames) VALUES (%s, %s, %s, %s)"
                                        # values = (flame_name, flame_date, flame_new_location, first)
                                        # mycursor.execute(query, values)
                                        # mydb.commit()
                                    writer.writerow([flame_new_location, first])

                                    if args.verbose:
                                        print("%s %s" % (flame_new_location, first))
                                    else:
                                        break
                                else:
                                    if args.output == "db":
                                        ""
                                        # frame_range = "%s-%s" % (first, last)
                                        # query = "INSERT INTO frames (user_file, file_date, location, frames) VALUES (%s, %s, %s, %s)"
                                        # values = (flame_name, flame_date, flame_new_location, frame_range)
                                        # mycursor.execute(query, values)
                                        # mydb.commit()
                                    frame_range = "%s-%s" % (first, last)
                                    writer.writerow([flame_new_location, frame_range])
                                    if args.verbose:
                                        print("%s %s-%s" % (flame_new_location, first, last))
                                    else:
                                        break
                        elif is_baselight_file:
                            

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
                                        if args.output == "db":
                                            ""
                                            # query = "INSERT INTO frames (user_file, file_date, location, frames) VALUES (%s, %s, %s, %s)"
                                            # values = (baselight_name, baselight_date, new_location, first)
                                            # mycursor.execute(query, values)
                                            # mydb.commit()
                                        writer.writerow([new_location, first])
                                        if args.verbose:
                                            print("%s %s" % (new_location, first))
                                        else:
                                            break
                                    else:
                                        if args.output == "db":
                                            ""
                                            # frame_range = "%s-%s" % (first, last)
                                            # query = "INSERT INTO frames (user_file, file_date, location, frames) VALUES (%s, %s, %s, %s)"
                                            # values = (baselight_name, baselight_date, new_location, frame_range)
                                            # mycursor.execute(query, values)
                                            # mydb.commit()
                                        frame_range = "%s-%s" % (first, last)
                                        writer.writerow([new_location, frame_range])
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
                                    if args.output == "db":
                                        ""
                                        # query = "INSERT INTO frames (user_file, file_date, location, frames) VALUES (%s, %s, %s, %s)"
                                        # values = (baselight_name, baselight_date, new_location, first)
                                        # mycursor.execute(query, values)
                                        # mydb.commit()
                                    writer.writerow([new_location, first])
                                    if args.verbose:
                                        print("%s %s" % (new_location, first))
                                    else:
                                        break
                                else:
                                    if args.output == "db":
                                        ""
                                        # frame_range = "%s-%s" % (first, last)
                                        # query = "INSERT INTO frames (user_file, file_date, location, frames) VALUES (%s, %s, %s, %s)"
                                        # values = (baselight_name, baselight_date, new_location, frame_range)
                                        # mycursor.execute(query, values)
                                        # mydb.commit()
                                    frame_range = "%s-%s" % (first, last)
                                    writer.writerow([new_location, frame_range])
                                    if args.verbose:
                                        print("%s %s-%s" % (new_location, first, last))
                                    else:
                                        break
    elif args.output == "db":
        for file_path in args.workFiles:
                # Open input file
                with open(file_path, "r") as read_input_file:

                    # Check if input file is a Flame file
                    is_flame_file = file_path.startswith("Flame_")
                    is_baselight_file = file_path.startswith("Baselight_")
                    
                    # Extract Flame name and date
                    if is_flame_file:
                        flame_name_date = os.path.splitext(os.path.basename(file_path))[0][6:]
                        flame_name, flame_date = flame_name_date.split("_")
                        print(flame_name_date)
                        query = "INSERT INTO data (script_runner, machine, user_file, file_date, submission_date) VALUES (%s, %s, %s, %s, %s)"
                        values = (script_runner, "Flame", flame_name, flame_date, now.strftime("%Y-%m-%d %H:%M:%S"))
                        mycursor.execute(query, values)
                        mydb.commit()
                    else:
                        baselight_name_date = os.path.splitext(os.path.basename(file_path))[0][10:]
                        baselight_name, baselight_date = baselight_name_date.split("_")
                        print(baselight_name)
                        query = "INSERT INTO data (script_runner, machine, user_file, file_date, submission_date) VALUES (%s, %s, %s, %s, %s)"
                        values = (script_runner, "Baselight", baselight_name, baselight_date, now.strftime("%Y-%m-%d %H:%M:%S"))
                        mycursor.execute(query, values)
                        mydb.commit()

                    # Read each line from input file
                    for line in read_input_file:
                            # Extract Flame name and date
                        if is_flame_file:
                            flame_parse = line.split(" ")
                            flame_storage = flame_parse.pop(0)
                            flame_current_folder = flame_parse.pop(0)
                            flame_new_location = ""
                            for xytech_line in xytech_folders:
                                if flame_current_folder in xytech_line:
                                    flame_new_location = xytech_line.strip()
                            first = ""
                            pointer = ""
                            last = ""
                            for numeral in flame_parse:
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
                                        query = "INSERT INTO frames (user_file, file_date, location, frames) VALUES (%s, %s, %s, %s)"
                                        values = (flame_name, flame_date, flame_new_location, first)
                                        mycursor.execute(query, values)
                                        mydb.commit()

                                        if args.verbose:
                                            print("%s %s" % (flame_new_location, first))
                                        else:
                                            break
                                    else:
                                        frame_range = "%s-%s" % (first, last)
                                        query = "INSERT INTO frames (user_file, file_date, location, frames) VALUES (%s, %s, %s, %s)"
                                        values = (flame_name, flame_date, flame_new_location, frame_range)
                                        mycursor.execute(query, values)
                                        mydb.commit()
                                        frame_range = "%s-%s" % (first, last)

                                        if args.verbose:
                                            print("%s %s-%s" % (flame_new_location, first, last))
                                        else:
                                            break
                                    first = int(numeral)
                                    pointer = first
                                    last = ""

                            # Working with last number each line
                            last = pointer
                            if first != "":
                                if first == last:
                                    query = "INSERT INTO frames (user_file, file_date, location, frames) VALUES (%s, %s, %s, %s)"
                                    values = (flame_name, flame_date, flame_new_location, first)
                                    mycursor.execute(query, values)
                                    mydb.commit()

                                    if args.verbose:
                                        print("%s %s" % (flame_new_location, first))
                                    else:
                                        break
                                else:
                                    frame_range = "%s-%s" % (first, last)
                                    query = "INSERT INTO frames (user_file, file_date, location, frames) VALUES (%s, %s, %s, %s)"
                                    values = (flame_name, flame_date, flame_new_location, frame_range)
                                    mycursor.execute(query, values)
                                    mydb.commit()
                                    frame_range = "%s-%s" % (first, last)

                                    if args.verbose:
                                        print("%s %s-%s" % (flame_new_location, first, last))
                                    else:
                                        break
                        elif is_baselight_file:
                            

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
                                        query = "INSERT INTO frames (user_file, file_date, location, frames) VALUES (%s, %s, %s, %s)"
                                        values = (baselight_name, baselight_date, new_location, first)
                                        mycursor.execute(query, values)
                                        mydb.commit()

                                        if args.verbose:
                                            print("%s %s" % (new_location, first))
                                        else:
                                            break
                                    else:
                                        frame_range = "%s-%s" % (first, last)
                                        query = "INSERT INTO frames (user_file, file_date, location, frames) VALUES (%s, %s, %s, %s)"
                                        values = (baselight_name, baselight_date, new_location, frame_range)
                                        mycursor.execute(query, values)
                                        mydb.commit()
                                        frame_range = "%s-%s" % (first, last)

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
                                    query = "INSERT INTO frames (user_file, file_date, location, frames) VALUES (%s, %s, %s, %s)"
                                    values = (baselight_name, baselight_date, new_location, first)
                                    mycursor.execute(query, values)
                                    mydb.commit()

                                    if args.verbose:
                                        print("%s %s" % (new_location, first))
                                    else:
                                        break
                                else:
                                    frame_range = "%s-%s" % (first, last)
                                    query = "INSERT INTO frames (user_file, file_date, location, frames) VALUES (%s, %s, %s, %s)"
                                    values = (baselight_name, baselight_date, new_location, frame_range)
                                    mycursor.execute(query, values)
                                    mydb.commit()
                                    frame_range = "%s-%s" % (first, last)
                                    
                                    if args.verbose:
                                        print("%s %s-%s" % (new_location, first, last))
                                    else:
                                        break
                                           