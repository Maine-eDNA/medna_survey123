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
        # if download is true, then call DownloadCleanJoinData to download from AGOL
        if download:
            for fmt in formats:
                download_result = DownloadCleanJoinData(fmt, overwrite, extract_attachments, join_tables,
                                                        backup, attachments_backup)
                download_result.download_data()
        # if upload is true, then call UploadData to upload to Google Sheets
        if upload:
            for fmt in formats:
                if fmt == 'CSV':
                    upload = UploadData()
                    upload.upload_data()
        api_logger.info("[END] run_download_upload")
    except Exception as err:
        raise RuntimeError("** Error: run_download_upload Failed (" + str(err) + ")")


class DownloadCleanJoinData:
    """
    :param download_format: Expects array of formats, e.g., ['CSV', 'File Geodatabase']
    :param overwrite: Boolean. If true, overwrite existing files with shared filepath.
    :param extract_attachments: Boolean. If true, extract attachments from FGDB.
    :param join_tables: Boolean. If true, join and save cleaned original data to CSV.
    :param backup: Boolean. If true, save copy of zips downloaded from AGOL to desired backup location.
    :param attachments_backup: Boolean. If true, save copy of attachments extracted from FGDB to desired backup location.
    :param main_input_dir: Primary directory for AGOL downloads.
    :param main_input_strip_dir: Primary directory for saving cleaned original data.
    :param survey123_item_id: Item ID of the file geodatabase
    :param survey123_version: Version of the Maine-eDNA field sampling survey.
    :param agol_username: UMaineGIS AGOL username
    :param agol_pass: UMaineGIS AGOL password
    :param blob_field: Field name of Blob data type field in attachment table
    :param attachments_field: Field name in attachment table that contains attachment name
    :param attachments_dir: Primary directory for attachments extracted from FGDB.
    :param attachments_backup_dir: Primary backup directory for attachments extracted from FGDB.
    :param main_output_dir: Primary output directory for joined data.
    :param backup_dir: Primary backup directory for zips downloaded from AGOL.
    :param survey_data: Filepath to cleaned eDNA_Sampling_v14_0.csv (strip /n within "")
    :param rep_crew: Filepath to cleaned rep_crew_1.csv (strip /n within "")
    :param rep_envmeas: Filepath to cleaned rep_envmeas_2.csv (strip /n within "")
    :param rep_collection: Filepath to cleaned rep_collection_3.csv (strip /n within "")
    :param rep_filter: Filepath to cleaned rep_filter_4.csv (strip /n within "")
    :param survey_projects: Expects python dictionary. Used to convert coded project values to label.
    :param survey_sub_filename: Filename for eDNA_Sampling_v14_0 subset CSV.
    :param survey_collection_join_filename: Filename for eDNA_Sampling_v14_0 and rep_collection_3 joined CSV.
    :param clean_filter_join_filename: Filepath for eDNA_Sampling_v14_0, rep_collection_3, and rep_filter_4 joined CSV.
    """
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
                 backup_dir=settings.BACKUP_DIR,
                 survey_data=settings.SURVEY_DATA,
                 rep_crew=settings.REP_CREW,
                 rep_envmeas=settings.REP_ENVMEAS,
                 rep_collection=settings.REP_COLLECTION,
                 rep_filter=settings.REP_FILTER,
                 survey_projects=settings.SURVEY_PROJECTS,
                 survey_sub_filename=settings.SURVEY_SUB_FILENAME,
                 survey_envmeas_join_filename=settings.SURVEY_ENVMEAS_JOIN_FILENAME,
                 survey_collection_join_filename=settings.SURVEY_COLLECTION_JOIN_FILENAME,
                 clean_filter_join_filename=settings.CLEAN_FILTER_JOIN_FILENAME):
        # overwrite boolean
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
        # extract attachments boolean
        self.extract_attachments = extract_attachments
        # FGDB extraction settings
        self.blob_field = blob_field
        self.attachments_field = attachments_field
        # Attachments dir
        self.attachments_dir = attachments_dir
        # Output dir
        self.main_output_dir = main_output_dir
        # Backup data and backup attachments boolean
        self.backup = backup
        self.attachments_backup = attachments_backup
        # Backup dirs
        self.backup_dir = backup_dir
        self.attachments_backup_dir = attachments_backup_dir
        # Cleaned original data (strip /n within "")
        self.survey_data = survey_data
        self.rep_crew = rep_crew
        self.rep_envmeas = rep_envmeas
        self.rep_collection = rep_collection
        self.rep_filter = rep_filter
        self.survey_projects = survey_projects
        # Join data boolean
        self.join_tables = join_tables
        # Joined original data
        self.survey_sub_filename = survey_sub_filename
        self.survey_envmeas_join_filename = survey_envmeas_join_filename
        self.survey_collection_join_filename = survey_collection_join_filename
        self.clean_filter_join_filename = clean_filter_join_filename
        export_fmt = ['File Geodatabase', 'Shapefile', 'CSV', 'DF']
        name_fmt = ['FGDB', 'SHP', 'CSV', 'DF']
        fmt_df = pd.DataFrame(list(zip(export_fmt, name_fmt)), columns=['export_fmt', 'name_fmt'])
        self.fmt_df = fmt_df

    def download_data(self):
        """
        Download formats from AGOL feature layer. If FGDB and extract_attachments is true, extract attachments.
        If overwrite is true, overwrite cleaned data. If join_tables, join clean data tables and export as CSV.
        If backup, save downloaded AGOL zips to backup_dir.
        """
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
        """
        If FGDB and extract_attachments is true, extract attachments. If attachments_backup is true,
        save extracted attachments to attachments_backup_dir.
        """
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
        """
        Note text fields allow carriage returns, but carriage returns are not stripped from original data.
        These new lines cause issues with reading in CSV data directly from AGOL.
        clean_data finds and replaces carriage returns in text fields with a space to resolve this issue.
        """
        # https://stackoverflow.com/questions/38758450/remove-carriage-return-from-text-file/38776444
        # https://stackoverflow.com/questions/17658055/how-can-i-remove-carriage-return-from-a-text-file-with-python#:~:text=Depending%20on%20the%20type%20of,rstrip()%20.&text=Python%20opens%20files%20in%20so,so%20newlines%20are%20always%20%5Cn%20.
        try:
            api_logger.info("[START] clean_data")
            main_input_dir = self.main_input_dir
            main_input_strip_dir = self.main_input_strip_dir

            # Creates a list of all items within the Input Folder Path.
            file_list = os.listdir(main_input_dir)
            for file in file_list:
                original_filename, file_extension = os.path.splitext(file)
                # Splits all of the items within the InputFolder
                # based on the File Name and it's Extension and sets them to the variables
                # TheFileName and TheFileExtension.
                # Since this is in a for loop, it will go through each file and execute the commands within the for loop
                # on each individual file based on specified parameters.
                if file_extension == ".csv":  # the specified parameters are items with the file extension .csv.
                    input_file = main_input_dir + original_filename + file_extension
                    output_file = main_input_strip_dir + original_filename + file_extension
                    # "if" the file extension is .csv, then the following functions will be executed.
                    api_logger.info("clean_data: cleaning " + input_file)
                    with open(input_file, 'r') as file_read:
                        with open(output_file, "w") as file_write:
                            content = file_read.read()
                            # If the text fields have carriage returns, then the CSV will have extra
                            # lines. regex is used here to find these carriage returns in text fields and
                            # replace them with a space
                            content_new = re.sub(r'"[^"]*(?:""[^"]*)*"', lambda m: m.group(0).replace("\n", " "), content)
                            file_write.write(content_new)
                    api_logger.info("clean_data: cleaned " + output_file)
            api_logger.info("[END] clean_data")
        except Exception as err:
            raise RuntimeError("** Error: clean_data Failed (" + str(err) + ")")

    def subset_survey_dataset(self):
        try:
            api_logger.info("[START] subset_survey_data")
            projects_dict = self.survey_projects
            # read in CSVs as df
            survey_data_df = pd.read_csv(self.survey_data)

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

            # when making a copy(), it generally has to be specified or the two copies will
            # be linked (changes in one will affect the other). Also raises SettingwithCopyWarning if not present.
            survey_sub_output = survey_sub.copy()
            # subset
            survey_sub_output = survey_sub_output[['survey_GlobalID', 'survey_date', 'survey_month', 'survey_year',
                                                   'projects', 'supervisor', 'username', 'recorder_first_name',
                                                   'recorder_last_name', 'system_type', 'site_id', 'other_site_id',
                                                   'general_location_name', 'survey_edit_date',
                                                   'survey_create_date']].copy()
            survey_sub_output['survey_date'] = pd.to_datetime(survey_sub_output.survey_date)
            survey_sub_output = survey_sub_output.sort_values(by=['survey_date', 'survey_GlobalID']).reset_index(drop=True)

            # write eDNA_Sampling_v14_sub to csv
            api_logger.info("subset_survey_dataset: To CSV " + self.survey_sub_filename)
            output_file = self.main_output_dir + self.survey_sub_filename + ".csv"
            survey_sub_output.to_csv(output_file, encoding='utf-8')
            api_logger.info("[END] subset_survey_dataset")

            return survey_sub
        except Exception as err:
            raise RuntimeError("** Error: subset_survey_data Failed (" + str(err) + ")")

    def subset_envmeas_dataset(self):
        try:
            api_logger.info("[START] subset_envmeas_dataset")
            # read in CSVs as df
            rep_envmeas_df = pd.read_csv(self.rep_envmeas)

            # subset
            rep_envmeas_sub = rep_envmeas_df[['GlobalID', 'ParentGlobalID', 'Measurement DateTime',
                                              'Measurement Depth', 'Environmental Instrument',
                                              'CTD Filename', 'CTD Notes',
                                              'YSI Filename', 'YSI Model', 'YSI Serial Number', 'YSI Notes',
                                              'Secchi Depth', 'Secchi Notes',
                                              'Niskin Number', 'Niskin Notes',
                                              'Other Instruments', 'Environmental Measurements',
                                              'Flow Rate', 'Water Temp', 'Salinity', 'pH Scale',
                                              'PAR1', 'PAR2', 'Turbidity', 'Conductivity', 'Dissolved Oxygen',
                                              'Pheophytin', 'Chlorophyll a', 'Nitrate and Nitrite', 'Nitrite',
                                              'Ammonium', 'Phosphate', 'Bottom Substrate', 'Lab DateTime',
                                              'Measurement Notes', 'CreationDate']].copy()
            # rename
            rep_envmeas_sub = rep_envmeas_sub.rename(columns={'GlobalID': 'envmeas_GlobalID',
                                                              'ParentGlobalID': 'envmeas_ParentGlobalID',
                                                              'Measurement DateTime': 'envmeas_date',
                                                              'Measurement Depth': 'envmeas_depth',
                                                              'Environmental Instrument': 'envmeas_instrument',
                                                              'CTD Filename': 'ctd_filename',
                                                              'CTD Notes': 'ctd_notes',
                                                              'YSI Filename': 'ysi_filename',
                                                              'YSI Model': 'ysi_model',
                                                              'YSI Serial Number': 'ysi_serial_number',
                                                              'YSI Notes': 'ysi_notes',
                                                              'Secchi Depth': 'secchi_depth',
                                                              'Secchi Notes': 'secchi_notes',
                                                              'Niskin Number': 'niskin_number',
                                                              'Niskin Notes': 'niskin_notes',
                                                              'Other Instruments': 'other_instruments',
                                                              'Environmental Measurements': 'env_measurements',
                                                              'Flow Rate': 'flow_rate',
                                                              'Water Temp': 'water_temp',
                                                              'Salinity': 'salinity',
                                                              'pH Scale': 'ph',
                                                              'PAR1': 'par1',
                                                              'PAR2': 'par2',
                                                              'Turbidity': 'turbidity',
                                                              'Conductivity': 'conductivity',
                                                              'Dissolved Oxygen': 'do',
                                                              'Pheophytin': 'pheophytin',
                                                              'Chlorophyll a': 'chla',
                                                              'Nitrate and Nitrite': 'no3no2',
                                                              'Nitrite': 'no2',
                                                              'Ammonium': 'nh4',
                                                              'Phosphate': 'phosphate',
                                                              'Bottom Substrate': 'bottom_substrate',
                                                              'Lab DateTime': 'lab_date',
                                                              'Measurement Notes': 'envmeas_notes',
                                                              'CreationDate': 'envmeas_create_date'
                                                              })

            api_logger.info("[END] subset_envmeas_dataset")
            return rep_envmeas_sub
        except Exception as err:
            raise RuntimeError("** Error: subset_envmeas_dataset Failed (" + str(err) + ")")

    def subset_collection_dataset(self):
        try:
            api_logger.info("[START] subset_collection_data")
            # read in CSVs as df
            rep_collection_df = pd.read_csv(self.rep_collection)

            # subset
            rep_collection_sub = rep_collection_df[['GlobalID', 'ParentGlobalID', 'Collection Type',
                                                    'Water Collection DateTime', 'Water Vessel Label',
                                                    'Water Collection Notes', 'Core DateTime Start',
                                                    'Core Label', 'Core Notes', 'CreationDate']].copy()
            # rename
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
            api_logger.info("[END] subset_collection_dataset")
            return rep_collection_sub
        except Exception as err:
            raise RuntimeError("** Error: subset_collection_data Failed (" + str(err) + ")")

    def subset_filter_dataset(self):
        try:
            api_logger.info("[START] subset_filter_dataset")
            # read in CSVs as df
            rep_filter_df = pd.read_csv(self.rep_filter)
            # subset
            rep_filter_sub = rep_filter_df[['GlobalID', 'ParentGlobalID', 'Is Prefilter', 'Filter Sample Label',
                                            'Filter Barcode', 'Filter DateTime', 'Filter Type', 'Filter Notes',
                                            'CreationDate']].copy()
            # rename
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
            api_logger.info("[END] subset_filter_dataset")

            return rep_filter_sub
        except Exception as err:
            raise RuntimeError("** Error: subset_filter_dataset Failed (" + str(err) + ")")

    def join_data(self):
        """
        Subset cleaned CSVs to desired headers, join, and export to CSV.
        """
        try:
            api_logger.info("[START] join_data")

            # subset datasets
            survey_sub = self.subset_survey_dataset()
            rep_envmeas_sub = self.subset_envmeas_dataset()
            rep_collection_sub = self.subset_collection_dataset()
            rep_filter_sub = self.subset_filter_dataset()

            # join eDNA_Sampling_v13_sub to rep_envmeas
            survey_envmeas_join = pd.merge(rep_envmeas_sub, survey_sub, how='left',
                                           left_on='envmeas_ParentGlobalID', right_on='survey_GlobalID')

            survey_envmeas_join_output = survey_envmeas_join.copy()
            # subset
            survey_envmeas_join_output = survey_envmeas_join_output[['survey_GlobalID', 'survey_date',
                                                                     'survey_month', 'survey_year', 'projects',
                                                                     'supervisor', 'username',
                                                                     'recorder_first_name', 'recorder_last_name',
                                                                     'system_type', 'site_id', 'other_site_id',
                                                                     'general_location_name',
                                                                     'envmeas_date', 'envmeas_depth',
                                                                     'envmeas_instrument',
                                                                     'ctd_filename', 'ctd_notes',
                                                                     'ysi_filename', 'ysi_model',
                                                                     'ysi_serial_number', 'ysi_notes',
                                                                     'secchi_depth', 'secchi_notes',
                                                                     'niskin_number', 'niskin_notes',
                                                                     'other_instruments', 'env_measurements',
                                                                     'flow_rate', 'water_temp', 'salinity',
                                                                     'ph', 'par1', 'par2',
                                                                     'turbidity', 'conductivity', 'do',
                                                                     'pheophytin',
                                                                     'chla', 'no3no2', 'no2',
                                                                     'nh4', 'phosphate', 'bottom_substrate',
                                                                     'lab_date',
                                                                     'envmeas_notes',
                                                                     'envmeas_GlobalID',
                                                                     'survey_edit_date', 'survey_create_date',
                                                                     'envmeas_create_date']].copy()
            survey_envmeas_join_output['survey_date'] = pd.to_datetime(survey_envmeas_join_output.survey_date)
            survey_envmeas_join_output = survey_envmeas_join_output.sort_values(by=['survey_date',
                                                                                    'survey_GlobalID']).reset_index(drop=True)

            # change all env_measurements to lower case
            survey_envmeas_join_output['env_measurements'] = survey_envmeas_join_output['env_measurements'].str.lower()

            # remove records without a specified env_measurements
            survey_envmeas_join_output['env_measurements'].replace('', np.nan, inplace=True)
            survey_envmeas_join_output.dropna(subset=['env_measurements'], how='all', inplace=True)

            # write survey_envmeas_join to csv
            api_logger.info("join_data: To CSV " + self.survey_envmeas_join_filename)
            output_file = self.main_output_dir + self.survey_envmeas_join_filename + ".csv"
            survey_envmeas_join_output.to_csv(output_file, encoding='utf-8')

            # join eDNA_Sampling_v13_sub to rep_collection
            survey_collection_join = pd.merge(rep_collection_sub, survey_sub, how='left',
                                              left_on='collection_ParentGlobalID', right_on='survey_GlobalID')

            survey_collection_join_output = survey_collection_join.copy()
            # subset
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

            # filter + sample + survey join
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
    """
    :param gdrive_private_key: JSON API file from google drive API
    :param main_output_dir: Directory of joined data.
    :param survey_sub_filename: Filename for eDNA_Sampling_v14_0 subset CSV.
    :param survey_envmeas_join_filename: Filename for eDNA_Sampling_v14_0 and rep_envmeas_2 joined CSV.
    :param survey_collection_join_filename: Filename for eDNA_Sampling_v14_0 and rep_collection_3 joined CSV.
    :param clean_filter_join_filename: Filepath for eDNA_Sampling_v14_0, rep_collection_3, and rep_filter_4 joined CSV.
    """
    def __init__(self, gdrive_private_key=settings.GDRIVE_PRIVATE_KEY_FILE,
                 target_spreadsheet_name=settings.GSHEETS_SPREADSHEET_NAME,
                 main_output_dir=settings.MAIN_OUTPUT_DIR,
                 survey_sub_filename=settings.SURVEY_SUB_FILENAME,
                 survey_envmeas_join_filename=settings.SURVEY_ENVMEAS_JOIN_FILENAME,
                 survey_collection_join_filename=settings.SURVEY_COLLECTION_JOIN_FILENAME,
                 clean_filter_join_filename=settings.CLEAN_FILTER_JOIN_FILENAME):
        self.gdrive_private_key = gdrive_private_key
        self.target_spreadsheet_name = target_spreadsheet_name
        self.main_output_dir = main_output_dir
        self.survey_sub_filename = survey_sub_filename
        self.survey_envmeas_join_filename = survey_envmeas_join_filename
        self.survey_collection_join_filename = survey_collection_join_filename
        self.clean_filter_join_filename = clean_filter_join_filename

    def upload_data(self):
        # https://medium.com/craftsmenltd/from-csv-to-google-sheet-using-python-ef097cb014f9
        try:
            api_logger.info("[START] upload_data")
            gdrive_private_key = self.gdrive_private_key
            target_spreadsheet_name = self.target_spreadsheet_name
            main_output_dir = self.main_output_dir
            survey_sub_filename = self.survey_sub_filename
            survey_envmeas_join_filename = self.survey_envmeas_join_filename
            survey_collection_join_filename = self.survey_collection_join_filename
            clean_filter_join_filename = self.clean_filter_join_filename

            scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                     "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
            # JSON API file from google drive API
            credentials = ServiceAccountCredentials.from_json_keyfile_name(gdrive_private_key, scope)
            client = gspread.authorize(credentials)

            # filepath for joined CSVs to be uploaded
            survey_sub = main_output_dir + survey_sub_filename + ".csv"
            survey_envmeas_join = main_output_dir + survey_envmeas_join_filename + ".csv"
            survey_collection_join = main_output_dir + survey_collection_join_filename + ".csv"
            clean_filter = main_output_dir + clean_filter_join_filename + ".csv"

            # name of target spreadsheet
            spreadsheet = client.open(target_spreadsheet_name)
            worksheet_list = spreadsheet.worksheets()
            upload_list = [survey_sub, survey_envmeas_join, survey_collection_join, clean_filter]

            # the filename of each CSV will be used to name each sheet within the target spreadsheet.
            for upload in upload_list:
                upload_filename, file_extension = os.path.splitext(upload)
                sheet_name = os.path.basename(upload_filename)
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
