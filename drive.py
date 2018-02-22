import json, sys, os, io, base64
from pprint import pprint
from apiclient.discovery import build
from httplib2 import Http
from oauth2client.service_account import ServiceAccountCredentials
from apiclient import http

scopes = ['https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name('service_secret.json', scopes)
delegated_credentials = credentials.create_delegated('phpcli@even-blueprint-161416.iam.gserviceaccount.com')
http_auth = credentials.authorize(Http())
service = build('drive','v3',  credentials=credentials)

class Drive(object):
    def upload(self, content, name, location, mime = 'application/pdf'):
        print(content, name, location)
        """Upload file to google drive"""
        file_metadata = {
          'name' : name,
          'parents': [ location ]
        }
        media = http.MediaFileUpload(content, mimetype=mime)
        file = service.files().create(body=file_metadata,media_body=media,fields='id').execute()
        return file.get('id')
    def createFolder(self, name, location):
        """Create folder in google drive"""
        file_metadata = {
          'name' : name,
          'mimeType' : 'application/vnd.google-apps.folder',
          'parents' : [location]
        }
        file = service.files().create(body=file_metadata,fields='id').execute()
        return file.get('id')
