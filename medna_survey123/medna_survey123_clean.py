"""
medna_survey123_clean
Download from ArcGIS Online, clean and join tables, and upload data to google sheets.
Created By: mkimble
LAST MODIFIED: 09/15/2021
"""

import re, os, arcpy, csv, glob
from shutil import copy2
from distutils.dir_util import copy_tree
from . import settings
from arcgis.gis import GIS, Item
from datetime import date
import pandas as pd
import numpy as np
from .logger_settings import api_logger
# https://developers.arcgis.com/labs/python/download-data/
# https://community.esri.com/t5/python-questions/using-python-to-download-survey123-survey-in-excel/td-p/724556
# Python Standard Library Modules
# from pathlib import Path
from zipfile import ZipFile
# for google sheets upload
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def run_download_upload(formats, download=True, upload=True, overwrite=True,
                        extract_attachments=True, join_tables=True,
                        backup=True, attachments_backup=True):
    """
     run download and upload
    """
    try:
        api_logger.info("[START] run_download_upload")
        if download:
            for fmt in formats:
                download_result = DownloadCleanJoinData(fmt, overwrite, extract_attachments, join_tables,
                                                        backup, attachments_backup)
                download_result.download_data()
        if upload:
            for fmt in formats:
                if fmt == 'CSV':
                    upload = UploadData()
                    upload.upload_data()
        api_logger.info("[END] run_download_upload")
    except Exception as err:
        raise RuntimeError("** Error: run_download_upload Failed (" + str(err) + ")")


class DownloadCleanJoinData:
    def __init__(self, download_format,
                 overwrite=False,
                 extract_attachments=True,
                 join_tables=True,
                 backup=True,
                 attachments_backup=True,
                 main_input_dir=settings.MAIN_INPUT_DIR,
                 main_input_strip_dir=settings.MAIN_INPUT_STRIP_DIR,
                 survey123_item_id=settings.SURVEY123_ITEM_ID,
                 survey123_version=settings.SURVEY123_VERSION,
                 agol_username=settings.AGOL_USERNAME,
                 agol_pass=settings.AGOL_PASS,
                 blob_field=settings.BLOB_FIELD,
                 attachments_field=settings.ATTACHMENTS_FIELD,
                 attachments_dir=settings.ATTACHMENTS_DIR,
                 attachments_backup_dir=settings.ATTACHMENTS_BACKUP_DIR,
                 main_output_dir=settings.MAIN_OUTPUT_DIR,
                 backup_dir = settings.BACKUP_DIR,
                 survey_data=settings.SURVEY_DATA,
                 rep_crew=settings.REP_CREW,
                 rep_envmeas=settings.REP_ENVMEAS,
                 rep_collection=settings.REP_COLLECTION,
                 rep_filter=settings.REP_FILTER,
                 survey_projects=settings.SURVEY_PROJECTS,
                 survey_sub_filename=settings.SURVEY_SUB_FILENAME,
                 survey_collection_join_filename=settings.SURVEY_COLLECTION_JOIN_FILENAME,
                 clean_filter_join_filename=settings.CLEAN_FILTER_JOIN_FILENAME):
        self.overwrite = overwrite
        # input dir
        self.main_input_dir = main_input_dir
        self.main_input_strip_dir = main_input_strip_dir
        # agol settings
        self.survey123_item_id = survey123_item_id
        self.survey123_version = survey123_version
        self.agol_username = agol_username
        self.agol_pass = agol_pass
        self.download_format = download_format
        # fgdb extraction
        self.extract_attachments = extract_attachments
        self.blob_field = blob_field
        self.attachments_field = attachments_field
        self.attachments_dir = attachments_dir
        self.attachments_backup = attachments_backup
        self.attachments_backup_dir = attachments_backup_dir
        # output dir
        self.main_output_dir = main_output_dir
        # backup dir
        self.backup_dir = backup_dir
        self.backup = backup
        # joining data
        self.join_tables = join_tables
        # original cleaned data (strip /n within "")
        self.survey_data = survey_data
        self.rep_crew = rep_crew
        self.rep_envmeas = rep_envmeas
        self.rep_collection = rep_collection
        self.rep_filter = rep_filter
        self.survey_projects = survey_projects
        # output data
        self.survey_sub_filename = survey_sub_filename
        self.survey_collection_join_filename = survey_collection_join_filename
        self.clean_filter_join_filename = clean_filter_join_filename

        export_fmt = ['File Geodatabase', 'Shapefile', 'CSV', 'DF']
        name_fmt = ['FGDB', 'SHP', 'CSV', 'DF']
        fmt_df = pd.DataFrame(list(zip(export_fmt, name_fmt)),
                          columns=['export_fmt', 'name_fmt'])
        self.fmt_df = fmt_df

    def download_data(self):
        try:
            api_logger.info("[START] download_data")
            # https://support.esri.com/en/technical-article/000018909
            overwrite = self.overwrite
            fmt = self.download_format
            extract_attachments = self.extract_attachments
            join_tables = self.join_tables
            fmt_df = self.fmt_df
            output_dir = self.main_input_dir
            survey123_version = self.survey123_version
            survey123_item_id = self.survey123_item_id
            backup_dir = self.backup_dir
            backup = self.backup

            today_date = date.today()
            today_date_filename = str(today_date.strftime('%Y%m%d'))
            fmt_name = fmt_df.loc[fmt_df['export_fmt'] == fmt, 'name_fmt'].values[0]
            output_file_name = 'S123_' + survey123_item_id + '_' + fmt_name + '_' + survey123_version + '_' + today_date_filename + '.zip'
            output_file_path = output_dir+output_file_name
            file_exists = os.path.exists(output_file_path.strip())
            if overwrite or not file_exists:
                # if overwrite is true or if the file does not exist
                agol_gis = GIS(username=self.agol_username,
                               password=self.agol_pass)
                # sm = SurveyManager(agol_gis, baseurl=None)
                # data_item = sm.get(self.survey123_item_id)
                data_item = Item(agol_gis, self.survey123_item_id)
                # if str, convert to list so that str does not iterate through each letter
                result = data_item.export(title=data_item.title, export_format=fmt)
                # survey123_data_item = Survey(result, sm, baseurl=None)
                if data_item is None:
                    api_logger.info("download_data: No data received")
                else:
                    api_logger.info("download_data: Downloading data: "+output_dir+", format: "+fmt)
                    # Download the data
                    result.download(save_path=output_dir, file_name=output_file_name)
                    if backup:
                        api_logger.info("download_data: backing up [" + output_file_name + "] to [" + backup_dir + "]")
                        copy2(output_file_path, backup_dir)
                    api_logger.info("download_data: unzipping "+output_file_name)
                    zip_file = ZipFile(output_file_path)
                    zip_file.extractall(path=output_dir)
                    if fmt_name == 'FGDB' and extract_attachments:
                        fgdb_filename = list(set([os.path.dirname(zfile) for zfile in zip_file.namelist()]))[0]
                        api_logger.info("download_data: Extracting attachments " + fgdb_filename)
                        self.extract_attachments_fgdb(fgdb_filename)
                    if fmt_name == 'CSV':
                        self.clean_data()
                    if fmt_name == 'CSV' and join_tables:
                        self.join_data()
            api_logger.info("[END] download_data")
        except Exception as err:
            raise RuntimeError("** Error: download_data Failed (" + str(err) + ")")

    def extract_attachments_fgdb(self, fgdb_filename):
        try:
            api_logger.info("[START] extract_attachments_fgdb")
            main_input_dir = self.main_input_dir
            blob_field = self.blob_field
            attachments_field = self.attachments_field
            attachments_dir = self.attachments_dir
            attachments_backup = self.attachments_backup
            attachments_backup_dir = self.attachments_backup_dir

            attachments_table = main_input_dir + fgdb_filename + '/rep_img__ATTACH'
            api_logger.info("extract_attachments_fgdb: attachments table " + attachments_table)
            with arcpy.da.SearchCursor(attachments_table, [blob_field, attachments_field]) as cursor:
                for row in cursor:
                    binary_rep = row[0]
                    file_name = row[1]
                    # save to disk
                    open(attachments_dir + os.sep + file_name, 'wb').write(binary_rep.tobytes())
            if attachments_backup:
                api_logger.info("extract_attachments_fgdb: backing up attachments")
                copy_tree(attachments_dir, attachments_backup_dir)
            api_logger.info("[END] extract_attachments_fgdb")
        except Exception as err:
            raise RuntimeError("** Error: extract_attachments_fgdb Failed (" + str(err) + ")")

    def clean_data(self):
        # https://stackoverflow.com/questions/38758450/remove-carriage-return-from-text-file/38776444
        # https://stackoverflow.com/questions/17658055/how-can-i-remove-carriage-return-from-a-text-file-with-python#:~:text=Depending%20on%20the%20type%20of,rstrip()%20.&text=Python%20opens%20files%20in%20so,so%20newlines%20are%20always%20%5Cn%20.
        # import file locations from settings.py so that they only need to be changed in one location
        try:
            api_logger.info("[START] clean_data")
            main_input_dir = self.main_input_dir
            main_input_strip_dir = self.main_input_strip_dir
            file_list = os.listdir(main_input_dir)  # Creates a list of all items within the Input Folder Path.
            for file in file_list:
                old_file_name, file_extension = os.path.splitext(file)
                # Splits all of the items within the InputFolder
                # based on the File Name and it's Extension and sets them to the variables
                # TheFileName and TheFileExtension.
                # Since this is in a for loop, it will go through each file and execute the commands within the for loop
                # on each individual file based on specified parameters.
                if file_extension == ".csv":  # the specified parameters are items with the file extension .csv.
                    input_file = main_input_dir + old_file_name + file_extension
                    output_file = main_input_strip_dir + old_file_name + file_extension
                    # "if" the file extension is .img, then the following functions will be executed.
                    api_logger.info("clean_data: cleaning " + input_file)

                    with open(input_file, 'r') as file_read:
                        with open(output_file, "w") as file_write:
                            content = file_read.read()
                            # If the text fields have carriage returns, then the CSV will have extra
                            # lines. regex is used here to find these carriage returns in text fields and
                            # replace them with a space
                            content_new = re.sub(r'"[^"]*(?:""[^"]*)*"', lambda m: m.group(0).replace("\n", " "), content)
                            # content_new = re.sub('[^|\n\r]\R', r'\1', content, flags=re.M)
                            file_write.write(content_new)
                    api_logger.info("clean_data: cleaned " + output_file)
            api_logger.info("[END] clean_data")
        except Exception as err:
            raise RuntimeError("** Error: clean_data Failed (" + str(err) + ")")

    def join_data(self):
        # import file locations from settings.py so that they only need to be changed in one location
        try:
            api_logger.info("[START] join_data")
            # read df
            survey_data_df = pd.read_csv(self.survey_data)
            rep_collection_df = pd.read_csv(self.rep_collection)
            rep_filter_df = pd.read_csv(self.rep_filter)
            projects_dict = self.survey_projects

            # grab specific fields from the csv
            survey_sub = survey_data_df[['GlobalID', 'Survey DateTime', 'Affiliated Projects', 'Supervisor',
                                         'username', 'Recorder First Name',
                                         'Recorder Last Name', 'Site ID', 'Other Site ID', 'General Location Name',
                                         'EditDate', 'CreationDate']].copy()
            # rename fields to remove spaces
            survey_sub = survey_sub.rename(columns={'GlobalID': 'survey_GlobalID',
                                                    'Survey DateTime': 'survey_date',
                                                    'Supervisor': 'supervisor',
                                                    'Affiliated Projects': 'projects',
                                                    'Recorder First Name': 'recorder_first_name',
                                                    'Recorder Last Name': 'recorder_last_name',
                                                    'Site ID': 'site_id',
                                                    'Other Site ID': 'other_site_id',
                                                    'General Location Name': 'general_location_name',
                                                    'EditDate': 'survey_edit_date',
                                                    'CreationDate': 'survey_create_date'})
            # format date and add month and year columns
            survey_sub['survey_date'] = pd.to_datetime(survey_sub.survey_date)
            survey_sub['survey_month'] = survey_sub['survey_date'].dt.strftime('%m')
            survey_sub['survey_year'] = survey_sub['survey_date'].dt.strftime('%Y')
            survey_sub['survey_date'] = survey_sub['survey_date'].dt.strftime('%m/%d/%Y')

            # replace project codes with project names
            survey_sub['projects'] = survey_sub['projects'].replace(projects_dict, regex=True)

            # convert to category for more efficient indexing
            survey_sub['site_id'] = survey_sub['site_id'].astype('category')
            # grab system type from site_id: ePR_L01 is L: Lake
            survey_sub['system_type'] = survey_sub['site_id'].str[4]
            # if the site_id was other, change system type to other
            survey_sub.loc[survey_sub['site_id'].str.lower() == 'other', 'system_type'] = 'other'

            # when making a copy, it generally has to be specified or the two copies will
            # be linked (changes in one will affect the other)
            survey_sub_output = survey_sub.copy()
            survey_sub_output = survey_sub_output[['survey_GlobalID', 'survey_date', 'survey_month', 'survey_year',
                                                   'projects', 'supervisor', 'username', 'recorder_first_name',
                                                   'recorder_last_name', 'system_type', 'site_id', 'other_site_id',
                                                   'general_location_name', 'survey_edit_date',
                                                   'survey_create_date']].copy()
            survey_sub_output['survey_date'] = pd.to_datetime(survey_sub_output.survey_date)
            survey_sub_output = survey_sub_output.sort_values(by=['survey_date', 'survey_GlobalID']).reset_index(drop=True)

            # write eDNA_Sampling_v14_sub to csv
            api_logger.info("join_data: To CSV " + self.survey_sub_filename)
            output_file = self.main_output_dir + self.survey_sub_filename + ".csv"
            survey_sub_output.to_csv(output_file, encoding='utf-8')

            rep_collection_sub = rep_collection_df[['GlobalID', 'ParentGlobalID', 'Collection Type',
                                                    'Water Collection DateTime', 'Water Vessel Label',
                                                    'Water Collection Notes', 'Core DateTime Start',
                                                    'Core Label', 'Core Notes', 'CreationDate']].copy()
            rep_collection_sub = rep_collection_sub.rename(columns={'GlobalID': 'collection_GlobalID',
                                                                    'ParentGlobalID': 'collection_ParentGlobalID',
                                                                    'Collection Type': 'collection_type',
                                                                    'Water Collection DateTime': 'water_collect_date',
                                                                    'Water Vessel Label': 'water_vessel_label',
                                                                    'Water Collection Notes': 'water_collect_notes',
                                                                    'Core DateTime Start': 'core_start_date',
                                                                    'Core Label': 'core_label',
                                                                    'Core Notes': 'core_notes',
                                                                    'CreationDate': 'collection_create_date'})
            # join eDNA_Sampling_v13_sub to rep_collection
            # survey_collection_join = survey_sub.join(rep_collection_sub.set_index(['collection_ParentGlobalID']), on=['survey_GlobalID'], how='outer')
            survey_collection_join = pd.merge(rep_collection_sub, survey_sub, how='left',
                                              left_on='collection_ParentGlobalID', right_on='survey_GlobalID')

            survey_collection_join_output = survey_collection_join.copy()
            survey_collection_join_output = survey_collection_join_output[['survey_GlobalID', 'survey_date',
                                                                           'survey_month', 'survey_year', 'projects',
                                                                           'supervisor', 'username',
                                                                           'recorder_first_name', 'recorder_last_name',
                                                                           'system_type', 'site_id', 'other_site_id',
                                                                           'general_location_name', 'collection_type',
                                                                           'water_collect_date', 'water_vessel_label',
                                                                           'water_collect_notes', 'core_start_date',
                                                                           'core_label', 'core_notes',
                                                                           'collection_GlobalID',
                                                                           'survey_edit_date', 'survey_create_date',
                                                                           'collection_create_date']].copy()
            survey_collection_join_output['survey_date'] = pd.to_datetime(survey_collection_join_output.survey_date)
            survey_collection_join_output = survey_collection_join_output.sort_values(by=['survey_date', 'survey_GlobalID']).reset_index(drop=True)

            # change all collection_type to lower case
            survey_collection_join_output['collection_type'] = survey_collection_join_output['collection_type'].str.lower()

            # remove records without a specified collection_type, water_vessel_label, and core_label
            survey_collection_join_output['collection_type'].replace('', np.nan, inplace=True)
            survey_collection_join_output['water_vessel_label'].replace('', np.nan, inplace=True)
            survey_collection_join_output['core_label'].replace('', np.nan, inplace=True)
            survey_collection_join_output.dropna(subset=['collection_type', 'water_vessel_label', 'core_label'],
                                                 how='all', inplace=True)

            # write survey_collection_join to csv
            api_logger.info("join_data: To CSV " + self.survey_collection_join_filename)
            output_file = self.main_output_dir + self.survey_collection_join_filename + ".csv"
            survey_collection_join_output.to_csv(output_file, encoding='utf-8')

            rep_filter_sub = rep_filter_df[['GlobalID', 'ParentGlobalID', 'Is Prefilter', 'Filter Sample Label',
                                            'Filter Barcode', 'Filter DateTime', 'Filter Type', 'Filter Notes',
                                            'CreationDate']].copy()
            rep_filter_sub = rep_filter_sub.rename(columns={'GlobalID': 'filter_GlobalID',
                                                            'ParentGlobalID': 'filter_ParentGlobalID',
                                                            'Is Prefilter': 'is_prefilter',
                                                            'Filter Sample Label': 'filter_label',
                                                            'Filter Barcode': 'filter_barcode',
                                                            'Filter DateTime': 'filter_date',
                                                            'Filter Type': 'filter_type',
                                                            'Filter Notes': 'filter_notes',
                                                            'CreationDate': 'filter_create_date'})
            # change all filter_type to lower case
            rep_filter_sub['filter_type'] = rep_filter_sub['filter_type'].str.lower()
            # reformat date
            rep_filter_sub['filter_date'] = pd.to_datetime(rep_filter_sub.filter_date)
            # rep_filter_sub['filter_date'] = rep_filter_sub['filter_date'].dt.strftime('%m/%d/%Y')
            rep_filter_sub['filter_date'] = rep_filter_sub['filter_date'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')

            # filter + sample + survey
            # ss_filter_join = survey_collection_join.join(rep_filter_sub.set_index(['filter_ParentGlobalID']), on=['collection_GlobalID'], how='outer')
            ss_filter_join = pd.merge(rep_filter_sub, survey_collection_join, how='left',
                                      left_on='filter_ParentGlobalID', right_on='collection_GlobalID')

            # filter records
            # remove records only if both filter and label are not specified
            ss_filter_join['filter_type'].replace('', np.nan, inplace=True)
            ss_filter_join['filter_label'].replace('', np.nan, inplace=True)
            ss_filter_join.dropna(subset=['filter_type', 'filter_label'], how='all', inplace=True)
            # reorder columns
            clean_filter_join = ss_filter_join[['survey_GlobalID', 'survey_date', 'survey_month', 'survey_year',
                                                'projects', 'supervisor', 'username', 'recorder_first_name',
                                                'recorder_last_name', 'system_type', 'site_id', 'other_site_id',
                                                'general_location_name', 'collection_type',  'water_collect_date',
                                                'water_vessel_label', 'water_collect_notes', 'filter_date',
                                                'is_prefilter', 'filter_type', 'filter_label', 'filter_barcode',
                                                'filter_notes', 'collection_GlobalID', 'filter_GlobalID',
                                                'survey_edit_date', 'survey_create_date', 'collection_create_date',
                                                'filter_create_date']].copy()

            clean_filter_join['survey_date'] = pd.to_datetime(clean_filter_join.survey_date)
            clean_filter_join = clean_filter_join.sort_values(by=['survey_date', 'survey_GlobalID', 'filter_date']).reset_index(drop=True)

            # write clean_filter_join to csv
            api_logger.info("join_data: To CSV " + self.clean_filter_join_filename)
            output_file = self.main_output_dir + self.clean_filter_join_filename+".csv"
            clean_filter_join.to_csv(output_file, encoding='utf-8')
            api_logger.info("[END] join_data")
        except Exception as err:
            raise RuntimeError("** Error: join_data Failed (" + str(err) + ")")


class UploadData:
    def __init__(self, gdrive_private_key=settings.GDRIVE_PRIVATE_KEY_FILE,
                 main_output_dir=settings.MAIN_OUTPUT_DIR,
                 clean_filter_join_filename=settings.CLEAN_FILTER_JOIN_FILENAME,
                 survey_sub_filename=settings.SURVEY_SUB_FILENAME,
                 survey_collection_join_filename=settings.SURVEY_COLLECTION_JOIN_FILENAME):
        self.gdrive_private_key = gdrive_private_key
        self.main_output_dir = main_output_dir
        self.clean_filter_join_filename = clean_filter_join_filename
        self.survey_sub_filename = survey_sub_filename
        self.survey_collection_join_filename = survey_collection_join_filename
        # self.CLEAN_PREFILTER_JOIN = settings.CLEAN_PREFILTER_JOIN
        # self.CLEAN_PREFILTER_FILTER_JOIN_FILENAME = settings.CLEAN_PREFILTER_FILTER_JOIN_FILENAME

    def upload_data(self):
        # https://medium.com/craftsmenltd/from-csv-to-google-sheet-using-python-ef097cb014f9
        try:
            api_logger.info("[START] upload_data")
            gdrive_private_key = self.gdrive_private_key
            main_output_dir = self.main_output_dir
            survey_sub_filename = self.survey_sub_filename
            survey_collection_join_filename = self.survey_collection_join_filename
            clean_filter_join_filename = self.clean_filter_join_filename

            scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                     "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
            credentials = ServiceAccountCredentials.from_json_keyfile_name(gdrive_private_key, scope)
            client = gspread.authorize(credentials)

            survey_sub = main_output_dir + survey_sub_filename + ".csv"
            survey_collection_join = main_output_dir + survey_collection_join_filename + ".csv"
            clean_filter = main_output_dir + clean_filter_join_filename + ".csv"

            spreadsheet = client.open('Survey123_filter_data')
            worksheet_list = spreadsheet.worksheets()
            upload_list = [survey_sub, survey_collection_join, clean_filter]

            for upload in upload_list:
                old_file_name, file_extension = os.path.splitext(upload)
                sheet_name = os.path.basename(old_file_name)
                for sheet in worksheet_list:
                    if sheet.title == sheet_name:
                        sheet_id = sheet.id
                        api_logger.info("upload_data: sheetName {}, sheetId(GID) {} ".format(sheet.title, sheet.id))
                api_logger.info("upload_data: Uploading " + sheet_name)
                with open(upload, 'r') as csv_file:
                    contents = csv_file.read()
                body = {
                    'requests': [{
                        'pasteData': {
                            'coordinate': {
                                'sheetId': sheet_id,
                                'rowIndex': '0',  # adapt this if you need different positioning
                                'columnIndex': '0',  # adapt this if you need different positioning
                            },
                            'data': contents,
                            'type': 'PASTE_NORMAL',
                            'delimiter': ',',
                        }
                    }]
                }
                spreadsheet.batch_update(body=body)
            api_logger.info("[END] upload_data")
        except Exception as err:
            raise RuntimeError("** Error: upload_data Failed (" + str(err) + ")")
