#
# Anderson Luiz Alves (alacn1@gmail.com)
# 2015-06-14
#
#
# Dependency installation
# -----------------------
# It requires google-api-python-client:
# https://github.com/google/google-api-python-client
#
# To install execute:
# pip install --upgrade google-api-python-client
#
# or:
# easy_install --upgrade google-api-python-client
#
#
# Configuration
# -------------
# At google developer console:
# https://console.developers.google.com
#
# * Create a project.
# * Enable Drive API for it.
# * Create a Service Account or Installed App credential.
#
#
# ### To configure with Installed App credential:
# * Create a Installed App credential at google developer console.
# * Get **client id** and **client secret key**.
#
# export GDRIVE_APP_TOKEN_FILE='path_to_save_token_file.key'
# export GDRIVE_APP_SECRET='client_secret_key'
# gdrive://installed_app_client_id/path
#
# * Run duplicity to get authorization url.
#
# export GDRIVE_APP_CODE='response_code'
#
# * Run duplicity to generate the token file.
#
# Now GDRIVE_APP_CODE isn't needed any more and can be removed.
#
#
# ### To configure with Service Account credential:
# Note that Service Account has it's own drive, it won't upload to your drive.
# * Create a Service Account credential at google developer console.
# * Get service account email address.
# * Generate a .p12 key file.
#
# export GDRIVE_SERVICE_KEY_FILE='path_to_key_file.p12'
# gdrive://service_account_email/path
#

import duplicity.backend
from duplicity.errors import BackendException
from duplicity import log
import os
import string


class GDriveBackend(duplicity.backend.Backend):
  """Connect to remote store using Google Drive API"""

  def __init__(self, parsed_url):
    duplicity.backend.Backend.__init__(self, parsed_url)

    # Import Google APIs
    try:
      global MediaIoBaseDownload
      global MediaFileUpload
      import httplib2
      from apiclient.discovery import build
      from apiclient.http import MediaFileUpload
      from apiclient.http import MediaIoBaseDownload
      from oauth2client.client import OAuth2Credentials
      from oauth2client.client import OAuth2WebServerFlow
      from oauth2client.client import SignedJwtAssertionCredentials
      global HttpError
      from apiclient.errors import HttpError
      global simplejson
      import simplejson
    except ImportError:
      raise BackendException(
        'Google Drive backend requires google-api-python-client. To install execute: '
        "'pip install --upgrade google-api-python-client'"
        'or:'
        "'easy_install --upgrade google-api-python-client'"
        )

    OAUTH_SCOPE = 'https://www.googleapis.com/auth/drive'

    # authenticate by service account
    if ('GDRIVE_SERVICE_KEY_FILE' in os.environ) and (parsed_url.username) and (parsed_url.hostname):
      SERVICE_EMAIL = parsed_url.username + '@' + parsed_url.hostname
      SERVICE_KEY_FILE = os.environ['GDRIVE_SERVICE_KEY_FILE']

      fd = file(SERVICE_KEY_FILE, 'rb')
      SERVICE_KEY = fd.read()
      fd.close()

      credentials = SignedJwtAssertionCredentials(SERVICE_EMAIL, SERVICE_KEY, OAUTH_SCOPE)

      log.Info('GDRIVE: authenticated by service account')

    # authenticate by installed app
    elif ('GDRIVE_APP_SECRET' in os.environ) and ('GDRIVE_APP_TOKEN_FILE' in os.environ) and (parsed_url.hostname):
      APP_ID = parsed_url.hostname
      APP_SECRET = os.environ['GDRIVE_APP_SECRET']
      TOKEN_FILE = os.environ['GDRIVE_APP_TOKEN_FILE']
      REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'

      # try load token
      try:
        fd = file(TOKEN_FILE, 'rb')
        credentials = OAuth2Credentials.from_json(fd.read())
        fd.close()

        log.Info('GDRIVE: authenticated by app token')

      except:
        log.Info("GDRIVE: couldn't authenticate with token file")
        flow = OAuth2WebServerFlow(APP_ID, APP_SECRET, OAUTH_SCOPE, redirect_uri=REDIRECT_URI)

        # try auth with code
        if 'GDRIVE_APP_CODE' in os.environ:
          APP_CODE = os.environ['GDRIVE_APP_CODE']
          try:
            credentials = flow.step2_exchange(APP_CODE)
            log.Info('GDRIVE: authenticated by response code')
          except:
            authorize_url = flow.step1_get_authorize_url()
            raise BackendException('Invalid GDRIVE_APP_CODE, update it with code from: ' + authorize_url)

          # save token
          fd = file(TOKEN_FILE, 'wb')
          fd.write(credentials.to_json())
          fd.close()
        else:
          authorize_url = flow.step1_get_authorize_url()
          raise BackendException('Authorization required, set GDRIVE_APP_CODE with code from: ' + authorize_url)

    # not configured
    else:
      raise BackendException(
        'Configuration required: '
        '*** for Installed App: GDRIVE_APP_TOKEN_FILE, GDRIVE_APP_SECRET, gdrive://app_client_id/path '
        '*** for Service Account: GDRIVE_SERVICE_KEY_FILE, gdrive://service_account_email/path'
        )

    self.http = credentials.authorize(httplib2.Http())
    self.drive = build('drive', 'v2', http=self.http)

    # find or create folder tree
    parent_id = 'root'
    folder_names = string.split(parsed_url.path, '/')

    for folder_name in folder_names:
      if not folder_name: continue

      param = {'q' : "'" + parent_id + "' in parents and trashed = false"}

      page_token = None
      while True:
        if page_token: param['pageToken'] = page_token

        # list items on parent folder
        file_list = self.drive.files().list(**param).execute()

        # find folder
        folder = next((
          item for item in file_list['items'] if item['title'] == folder_name and
          item['mimeType'] == 'application/vnd.google-apps.folder'), None)
        if folder: break; # found

        page_token = file_list.get('nextPageToken')
        if not page_token: break

      # folder doesn't exist
      if folder is None:
        # create folder
        folder = self.drive.files().insert(body={
          'title': folder_name,
          'mimeType': 'application/vnd.google-apps.folder',
          'parents': [{'id': parent_id}]
          }).execute()

        log.Info("GDRIVE: created folder '%s'" % folder_name)

      # folder_name id
      parent_id = folder['id']

    # parsed_url.path id
    self.parent_id = parent_id


  def __list(self):
    param = {
      'q': "'" + self.parent_id + "' in parents and trashed = false",
      'maxResults': 1000
    }

    res = []
    page_token = None
    while True:
      if page_token: param['pageToken'] = page_token
      file_list = self.drive.files().list(**param).execute()

      res.extend([item['title'] for item in file_list['items']])

      page_token = file_list.get('nextPageToken')
      if not page_token: break

    log.Debug('GDRIVE: gdrive.__list() = %s' % res)

    return res


  def __getInfo(self, filename):
    file_list = self.drive.files().list(**{
      'q': "'" + self.parent_id + "' in parents and "
        "title = '" + filename.replace("\\", "\\\\").replace("'", "\\'") + "' and "
        "trashed = false"
      }).execute()
    if not file_list['items'][0]:
      raise BackendException("GDRIVE: file '%s' not found" % filename)
    return file_list['items'][0]


  def __query(self, filename):
    try:
      f = self.__getInfo(filename)
      size = int(f['fileSize'])
    except:
      size = -1

    log.Debug("GDRIVE: gdrive.__query('%s') = %d" % (filename, size))

    return {'size': size}


  def __delete(self, filename):
    log.Debug("GDRIVE: gdrive.__delete('%s')" % filename)

    file_list = self.drive.files().list(**{
      'q': "'" + self.parent_id + "' in parents and "
        "title = '" + filename + "' and "
        "trashed = false"
      }).execute()

    if file_list and file_list['items']:
      for item in file_list['items']:
        if item['title'] == filename:
          self.drive.files().delete(fileId=item['id']).execute()
          log.Info("GDRIVE: deleted '%s' (id='%s')" % (filename, item['id']))


  def __try_download(self, fid, local_filename, ack = None):
    request = self.drive.files().get_media(fileId=fid, acknowledgeAbuse=ack)

    fd = file(local_filename, 'wb')
    try:
      downloader = MediaIoBaseDownload(fd, request)

      while True:
        status, done = downloader.next_chunk()
        if done:
          fd.close()
          return

    except:
      fd.close()
      raise


  def __get(self, remote_filename, local_filename):
    log.Info("GDRIVE: downloading '%s'" % remote_filename)

    fid = self.__getInfo(remote_filename)['id']
    try:
      self.__try_download(fid, local_filename)

    except HttpError as e:
      try:
        j = simplejson.loads(e.content)
        code = j['error']['code']
        reason = j['error']['errors'][0]['reason']
        msg = j['error']['message']
      except:
        raise e

      if (403 == code) and ('abuse' == reason):
        log.Info("GDRIVE: downloading with acknowledgeAbuse because google-api said error 403 '%s'" % msg)
        self.__try_download(fid, local_filename, 'true')
      else:
        raise

    log.Info("GDRIVE: download done '%s'" % remote_filename)


  def __put(self, local_filename, remote_filename):
    self.__delete(remote_filename)

    log.Info("GDRIVE: uploading '%s'" % remote_filename)

    media_body = MediaFileUpload(local_filename, '*/*', resumable=True)
    body = {
      'title': remote_filename,
      'parents': [{
        'kind': 'drive#fileLink',
        'id': self.parent_id
        }]
      }
    self.drive.files().insert(body=body, media_body=media_body).execute()

    log.Info("GDRIVE: upload done '%s'" % remote_filename)



### for duplicity 0.7.03
  def _get(self, remote_filename, local_path):
    self.__get(remote_filename, local_path.name)

  def _put(self, source_path, remote_filename):
    self.__put(source_path.name, remote_filename)

  def _list(self):
    return self.__list()

  def _delete(self, filename):
    self.__delete(filename)

  def _query(self, filename):
    return self.__query(filename)



### for duplicity 0.6.23
#  def delete(self, filename_list):
#    for filename in filename_list:
#      self.__delete(filename)
#
#  def get(self, remote_filename, local_path):
#    self.__get(remote_filename, local_path.name)
#
#  def put(self, source_path, remote_filename = None):
#    if not remote_filename: remote_filename = source_path.get_filename()
#    self.__put(source_path.name, remote_filename)
#
#  def _list(self):
#    return self.__list()
#
#  def _query_file_info(self, filename):
#    try:
#      size = self.__query(filename)['size']
#    except:
#      size = None
#    if size == -1: size = None
#    return {'size': size}



### register backend
duplicity.backend.register_backend('gdrive', GDriveBackend)
try:
  # for duplicity 0.7.03
  duplicity.backend.uses_netloc.extend(['gdrive'])
except:
  try:
    # for duplicity 0.6.23
    duplicity.backend._ensure_urlparser_initialized()
    duplicity.backend.urlparser.uses_netloc.extend(['gdrive'])
    duplicity.backend.urlparser.clear_cache()
  except:
    pass

