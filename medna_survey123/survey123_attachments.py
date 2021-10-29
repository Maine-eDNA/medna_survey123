"""
survey123_attachments
Extract attachments from ESRI file geodatabase
Created By: mkimble
"""

import os, arcpy
from . import settings
from .logger_settings import api_logger


class DownloadAttachmentsFGDB:
   """
   Arguments
   ATTACHMENTS_TABLE: Path to attachment table
   BLOB_FIELD: Field name of Blob data type field in attachment table
   ATTACHMENTS_FIELD: Field name in attachment table that contains attachment name
   ATTACHMENTS_FOLDER: Output folder to export attachments to
   """
   def __init__(self, ATTACHMENTS_TABLE=settings.ATTACHMENTS_TABLE,
                BLOB_FIELD = settings.BLOB_FIELD,
                ATTACHMENTS_FIELD = settings.ATTACHMENTS_FIELD,
                ATTACHMENTS_FOLDER = settings.ATTACHMENTS_FOLDER):
      self.ATTACHMENTS_TABLE = ATTACHMENTS_TABLE
      self.BLOB_FIELD = BLOB_FIELD
      self.ATTACHMENTS_FIELD = ATTACHMENTS_FIELD
      self.ATTACHMENTS_FOLDER = ATTACHMENTS_FOLDER

   def extract_attachments(self):
      try:
         api_logger.info("[START] extract_attachments")
         with arcpy.da.SearchCursor(self.ATTACHMENTS_TABLE, [self.BLOB_FIELD, self.ATTACHMENTS_FIELD]) as cursor:
            for row in cursor:
               binary_rep = row[0]
               file_name = row[1]
               # save to disk
               open(self.ATTACHMENTS_FOLDER + os.sep + file_name, 'wb').write(binary_rep.tobytes())
         api_logger.info("[END] extract_attachments")
      except Exception as err:
         raise RuntimeError("** Error: extract_attachments Failed (" + str(err) + ")")
