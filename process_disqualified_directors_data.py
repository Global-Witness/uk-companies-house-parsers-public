import csv
import os
import sys
from collections import defaultdict

PERSONS_OUTPUT_FILENAME_TEMPLATE = "persons_data_%s.csv"
DISQUALIFICATIONS_FILENAME_TEMPLATE = "disqualifications_data_%s.csv"
EXEMPTIONS_FILENAME_TEMPLATE = 'exemptions_data_%s.csv'
SNAPSHOT_HEADER_IDENTIFIER = "DISQUALS"
TRAILER_RECORD_IDENTIFIER = "DISQUALS"
PERSON_RECORD_TYPE = '1'
DISQUALIFICATION_RECORD_TYPE = '2'
EXEMPTION_RECORD_TYPE = '3'


def process_header_row(row):
    header_identifier = row[0:8]
    print(header_identifier)
    run_number = row[8:12]
    production_date = row[12:20]
    if header_identifier != SNAPSHOT_HEADER_IDENTIFIER:
        print(
            "Unsuported file type from header: '%s'. Expecting a snapshot header: '%s'"
            % (header_identifier, SNAPSHOT_HEADER_IDENTIFIER))
        sys.exit(1)
    print("Processing snapshot file with run number %s from date %s" %
          (run_number, production_date))


def process_person_row(row, output_writer):
    record_type = row[0]
    person_number = str(row[1:13])
    person_dob = row[13:21]
    person_postcode = row[21:29]
    person_variable_ind = int(row[29:33])
    person_details = row[33:33 + person_variable_ind]
    person_details = person_details.split('<')
    title = person_details[0]
    forenames = person_details[1]
    surname = person_details[2]
    honours = person_details[3]
    address_line_1 = person_details[4]
    address_line_2 = person_details[5]
    posttown = person_details[6]
    county = person_details[7]
    country = person_details[8]
    nationality = person_details[9]
    corporate_number = person_details[10]
    country_registration = person_details[11]
    output_writer.writerow([
        record_type, person_number, person_dob, person_postcode,
        person_details, title, forenames, surname, honours, address_line_1,
        address_line_2, posttown, county, country, nationality,
        corporate_number, country_registration
    ])


def process_disqualification_row(row, output_writer):
    record_type = row[0]
    person_number = str(row[1:13])
    disqual_start_date = row[13:21]
    disqual_end_date = row[21:29]
    section_of_act = row[29:49]
    disqual_type = row[49:79]
    disqual_order_date = row[79:87]
    case_number = row[87:117]
    company_name = row[117:277]
    court_name_variable_ind = int(row[277:281])
    court_name = row[281:281 + court_name_variable_ind]
    output_writer.writerow([
        record_type, person_number, disqual_start_date, disqual_end_date,
        section_of_act, disqual_type, disqual_order_date, case_number,
        company_name, court_name
    ])


def process_exemption_row(row, output_writer):
    record_type = row[0]
    person_number = str(row[1:9])
    exemption_start_date = row[13:21]
    exemption_end_date = row[21:29]
    exemption_purpose = int(row[29:39])
    exemption_purpose_dict = defaultdict(
        lambda: '', {
            1: 'Promotion',
            2: 'Formation',
            3:
            'Directorships or other participation in management of a company',
            4:
            'Designated member/member or other participation in management of an LLP',
            5: 'Receivership in relation to a company or LLP'
        })
    exemption_purpose = exemption_purpose_dict[exemption_purpose]
    exemption_company_name_ind = int(row[39:43])
    exemption_company_name = row[43:43 + exemption_company_name_ind]
    output_writer.writerow([
        record_type, person_number, exemption_start_date, exemption_end_date,
        exemption_purpose, exemption_company_name
    ])


def init_person_output_file(filename):
    output_persons_file = open(filename, 'w')
    persons_writer = csv.writer(output_persons_file, delimiter=",")
    persons_writer.writerow([
        "record_type", "person_number", "person_dob", "person_postcode",
        "person_details", 'title', 'forenames', 'surname', 'honours',
        'address_line_1', 'address_line_2', 'posttown', 'county', 'country',
        'nationality', 'corporate_number', 'country_registration'
    ])
    return output_persons_file, persons_writer


def init_disquals_output_file(filename):
    output_disquals_file = open(filename, 'w')
    disqauls_writer = csv.writer(output_disquals_file, delimiter=",")
    disqauls_writer.writerow([
        "record_type", "person_number", "disqual_start_date",
        "disqual_end_date", "section_of_act", "disqual_type",
        "disqual_order_date", "case_number", "company_name", "court_name"
    ])
    return output_disquals_file, disqauls_writer


def init_exemptions_output_file(filename):
    output_exemptions_file = open(filename, 'w')
    exemptions_writer = csv.writer(output_exemptions_file, delimiter=",")
    exemptions_writer.writerow([
        "record_type", "person_number", "exemption_start_date",
        "exemption_end_date", "exemption_purpose", "exemption_company_name"
    ])
    return output_exemptions_file, exemptions_writer


def init_input_files(output_folder, base_input_name):
    persons_output_filename = os.path.join(
        output_folder, PERSONS_OUTPUT_FILENAME_TEMPLATE % (base_input_name))
    disquals_output_filename = os.path.join(
        output_folder, DISQUALIFICATIONS_FILENAME_TEMPLATE % (base_input_name))
    exemptions_output_filename = os.path.join(
        output_folder, EXEMPTIONS_FILENAME_TEMPLATE % (base_input_name))
    print("Saving companies data to %s" % persons_output_filename)
    print("Saving persons data to %s" % disquals_output_filename)
    print("Saving persons data to %s" % exemptions_output_filename)
    output_persons_file, output_persons_writer = init_person_output_file(
        persons_output_filename)
    output_disquals_file, output_disquals_writer = init_disquals_output_file(
        disquals_output_filename)
    output_exemptions_file, output_exemptions_writer = init_exemptions_output_file(
        exemptions_output_filename)
    return output_persons_file, output_persons_writer, output_disquals_file, output_disquals_writer, output_exemptions_file, output_exemptions_writer


def process_company_appointments_data(input_file, output_folder,
                                      base_input_name):
    persons_processed = 0
    disquals_processed = 0
    exemptions_processed = 0
    output_persons_file, output_persons_writer, output_disquals_file, output_disquals_writer, output_exemptions_file, output_exemptions_writer = init_input_files(
        output_folder, base_input_name)
    for row_num, row in enumerate(input_file):
        if row_num == 0:
            process_header_row(row)
        elif row[0:8] == TRAILER_RECORD_IDENTIFIER:
            # End of file
            record_count = int(row[45:53])
            print(
                "Reached end of file. Processed %s == %s records: %s persons, %s disquals, %s exemptions."
                % (record_count, persons_processed + disquals_processed +
                   exemptions_processed, persons_processed, disquals_processed,
                   exemptions_processed))
            output_persons_file.close()
            output_disquals_file.close()
            output_exemptions_file.close()
            sys.exit(0)
        elif row[0] == PERSON_RECORD_TYPE:
            process_person_row(row, output_persons_writer)
            persons_processed += 1
        elif row[0] == DISQUALIFICATION_RECORD_TYPE:
            process_disqualification_row(row, output_disquals_writer)
            disquals_processed += 1
        elif row[0] == EXEMPTION_RECORD_TYPE:
            process_exemption_row(row, output_exemptions_writer)
            exemptions_processed += 1


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print(
            'Usage: python process_disqualified_directors_data.py input_file output_folder\n',
            'E.g. python process_disqualified_directors_data.py  Prod195_1111_ni_sample.dat ./output/'
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