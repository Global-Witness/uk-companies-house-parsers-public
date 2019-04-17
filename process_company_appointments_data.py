import csv
import os
import sys

COMPANIES_OUTPUT_FILENAME_TEMPLATE = "companies_data_%s.csv"
PERSONS_OUTPUT_FILENAME_TEMPLATE = "persons_data_%s.csv"
SNAPSHOT_HEADER_IDENTIFIER = "DDDDSNAP"
TRAILER_RECORD_IDENTIFIER = "99999999"
COMPANY_RECORD_TYPE = '1'
PERSON_RECORD_TYPE = '2'


def process_header_row(row):
    header_identifier = row[0:8]
    run_number = row[8:12]
    production_date = row[12:20]
    if header_identifier != SNAPSHOT_HEADER_IDENTIFIER:
        print(
            "Unsuported file type from header: '%s'. Expecting a snapshot header: '%s'"
            % (header_identifier, SNAPSHOT_HEADER_IDENTIFIER))
        sys.exit(1)
    print("Processing snapshot file with run number %s from date %s" %
          (run_number, production_date))


def process_company_row(row, output_writer):
    company_number = row[0:8].strip()
    record_type = row[8].strip()
    company_status = row[9].strip()
    number_of_officers = int(row[32:36])
    name_length = int(row[36:40])
    company_name = row[40:(40 + name_length - 1)].strip()
    output_writer.writerow(
        [company_number, company_status, number_of_officers, company_name])


def process_person_row(row, output_writer):
    company_number = row[0:8].strip()
    record_type = row[8].strip()
    app_date_origin = row[9].strip()
    appointment_type = row[10:12].strip()
    person_number = row[12:24].strip()
    corporate_indicator = row[24].strip()
    appointment_date = row[32:40].strip()
    resignation_date = row[40:48].strip()
    postcode = row[48:56].strip()
    partial_date_of_birth = row[56:64].strip()
    full_date_of_birth = row[64:72].strip()
    variable_data_length = int(row[72:76])
    variable_data = row[76:76 + variable_data_length]
    variable_data_array = variable_data.split('<')
    title = variable_data_array[0].strip()
    forenames = variable_data_array[1].strip()
    surname = variable_data_array[2].strip()
    honours = variable_data_array[3].strip()
    care_of = variable_data_array[4].strip()
    po_box = variable_data_array[5].strip()
    address_line_1 = variable_data_array[6].strip()
    address_line_2 = variable_data_array[7].strip()
    post_town = variable_data_array[8].strip()
    county = variable_data_array[9].strip()
    country = variable_data_array[10].strip()
    occupation = variable_data_array[11].strip()
    nationality = variable_data_array[12].strip()
    res_country = variable_data_array[13].strip()
    # print(company_number, record_type, app_date_origin, appointment_type, person_number, corporate_indicator, appointment_date,
    # resignation_date, postcode, partial_date_of_birth, full_date_of_birth, variable_data_length, variable_data)
    # print("title = %s forenames = %s surname = %s honours = %s care_of = %s po_box = %s address_line_1 = %s address_line_2 = %s post_town = %s county = %s country = %s occupation = %s nationality = %s res_country = %s" % (
    # title, forenames, surname, honours, care_of, po_box, address_line_1, address_line_2, post_town, county, country, occupation, nationality, res_country))
    output_writer.writerow([
        company_number, app_date_origin, appointment_type, person_number,
        corporate_indicator, appointment_date, resignation_date, postcode,
        partial_date_of_birth, full_date_of_birth, title, forenames, surname,
        honours, care_of, po_box, address_line_1, address_line_2, post_town,
        county, country, occupation, nationality, res_country
    ])


def init_company_output_file(filename):
    output_companies_file = open(filename, 'w')
    companies_writer = csv.writer(output_companies_file, delimiter=",")
    companies_writer.writerow([
        "Company Number", "Company Status", "Number of Officers",
        "Company Name"
    ])
    return output_companies_file, companies_writer


def init_person_output_file(filename):
    output_persons_file = open(filename, 'w')
    persons_writer = csv.writer(output_persons_file, delimiter=",")
    persons_writer.writerow([
        "Company Number", "App Date Origin", "Appointment Type",
        "Person number", "Corporate indicator", "Appointment Date",
        "Resignation Date", "Person Postcode", "Partial Date of Birth",
        "Full Date of Birth", "Title", "Forenames", "Surname", "Honours",
        "Care_of", "PO_box", "Address line 1", "Address line 2", "Post_town",
        "County", "Country", "Occupation", "Nationality", "Resident Country"
    ])
    return output_persons_file, persons_writer


def init_input_files(output_folder, base_input_name):
    companies_output_filename = os.path.join(
        output_folder, COMPANIES_OUTPUT_FILENAME_TEMPLATE % (base_input_name))
    persons_output_filename = os.path.join(
        output_folder, PERSONS_OUTPUT_FILENAME_TEMPLATE % (base_input_name))
    PERSONS_OUTPUT_FILENAME_TEMPLATE
    print("Saving companies data to %s" % companies_output_filename)
    print("Saving persons data to %s" % persons_output_filename)
    output_companies_file, output_companies_writer = init_company_output_file(
        companies_output_filename)
    output_persons_file, output_persons_writer = init_person_output_file(
        persons_output_filename)
    return output_companies_file, output_companies_writer, output_persons_file, output_persons_writer


def process_company_appointments_data(input_file, output_folder,
                                      base_input_name):
    companies_processed = 0
    persons_processed = 0
    error_code = 0
    output_companies_file, output_companies_writer, output_persons_file, output_persons_writer = init_input_files(
        output_folder, base_input_name)
    for row_num, row in enumerate(input_file):
        if row_num == 0:
            process_header_row(row)
        elif row[0:8] == TRAILER_RECORD_IDENTIFIER:
            # End of file
            record_count = int(row[8:16])
            total_processed = companies_processed + persons_processed
            if record_count == total_processed:
                print(
                    "Reached end of file. Processed %s records: %s companies, %s persons."
                    % (total_processed, companies_processed, persons_processed))
            else:
                print(
                    "Reached end of file. ERROR: Processed %s records out of %s: %s companies, %s persons."
                    % (total_processed, record_count, companies_processed, persons_processed))
                error_code = 1
            output_companies_file.close()
            output_persons_file.close()
            sys.exit(error_code)
        elif row[8] == COMPANY_RECORD_TYPE:
            process_company_row(row, output_companies_writer)
            companies_processed += 1
        elif row[8] == PERSON_RECORD_TYPE:
            process_person_row(row, output_persons_writer)
            persons_processed += 1
    print("ERROR: File ended abbruptly. Did not find a TRAILER_RECORD_IDENTIFIER.")
    sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print(
            'Usage: python process_company_appointments_data.py input_file output_folder\n',
            'E.g. python process_company_appointments_data.py Prod195_1111_ni_sample.dat ./output/'
        )
        sys.exit(1)
    input_filename = sys.argv[1]
    output_folder = sys.argv[2]
    input_file = open(input_filename, 'r')
    base_input_name = os.path.basename(input_filename)
    # Do not include the extension in the base input name
    base_input_name = os.path.splitext(base_input_name)[0]
    process_company_appointments_data(input_file, output_folder,
                                      base_input_name)
