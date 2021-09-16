# Maine-eDNA Survey123


### Conda environment & ArcPy
Requires ArcPy from ArcGIS Pro. To use, clone the `arcgispro-py3` 
conda environment from within ArcGIS Pro.

### Settings.py
To add in your own settings, save a copy of `requirements/settings.py.txt` 
to `medna_survey123/settings.py`. Once saved, modify the settings under the 
`AGOL GEODATABASE AND USER SETTINGS` and `GDRIVE API USER SETTINGS` headings.

These two headings require navigating to your ArcGIS Online organization
and obtaining the Item ID of the Survey123 File Geodatabase. They also
require obtaining a JSON private key file from your personal Google Drive 
API.

Place your JSON private key file from your personal Google Drive API in
`data/01_Original/` or the modified directory if they were changed in 
`medna_survey123/settings.py`


### Run_medna_survey123 options
Once set, run `run_medna_survey123.py` to download Survey123 data, join and clean tables, 
and upload to Google Sheets. `run_medna_survey123.py` calls `run_download_upload` with 
default arguments.

```
run_download_upload(formats,  download=True, upload=True, overwrite=True,
                    extract_attachments=True, join_tables=True,
                    backup=True, attachments_backup=True)
```
`formats = ['CSV', 'File Geodatabase']` 

Data must be downloaded as a CSV to upload to Google Sheets and 
attachments can only be extracted from a File Geodatabase. `formats` expects an array.

`download=True`

Download survey data from ArcGIS Online. Default is `True`.

`upload=True`

Upload transformed CSV data to Google Sheets. Default is `True`.

`overwrite=True`

Overwrite existing files. Default is `True`.

`extract_attachments=True`

Extract attachments from File Geodatabase. Default is `True`.

`join_tables=True`

Join CSV data. Default is `True`.

`backup=True`

Backup location for the zip files downloaded from ArcGIS Online. Default is `True`.

`attachments_backup=True`

Backup location for the attachments (images) extracted from the File Geodatabase. Default is `True`.
