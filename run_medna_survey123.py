from medna_survey123.medna_survey123_clean import run_download_upload
from tkinter import *

canvas = Tk()

formats = ['CSV', 'File Geodatabase']
#formats = ['File Geodatabase']
#formats = ['CSV']

run_download_upload(formats, download=True, upload=True, overwrite=True,
                    extract_attachments=True, join_tables=True,
                    backup=True, attachments_backup=True)
