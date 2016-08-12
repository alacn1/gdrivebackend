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
# * Generate a .json key file.
#
# export GDRIVE_SERVICE_KEY_FILE='path_to_key_file.json'
# gdrive://service_account_email/path
#
import duplicity.backend
from duplicity.errors import BackendException
from duplicity import log
import os
import string

### for duplicity 0.6.23
#from duplicity.backend import retry


class GDriveBackend(duplicity.backend.Backend):
  """Connect to remote store using Google Drive API"""

  def __init__(self, parsed_url):
    duplicity.backend.Backend.__init__(self, parsed_url)

    # Import Google APIs
    try:
      global httplib2
      import httplib2
      global build
      from apiclient.discovery import build
      global MediaFileUpload
      from apiclient.http import MediaFileUpload
      global MediaIoBaseDownload
      from apiclient.http import MediaIoBaseDownload
      from oauth2client.client import OAuth2Credentials
      from oauth2client.client import OAuth2WebServerFlow
      from oauth2client.service_account import ServiceAccountCredentials
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
      SERVICE_KEY_FILE = os.environ['GDRIVE_SERVICE_KEY_FILE']

      credentials = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_KEY_FILE, OAUTH_SCOPE)

      log.Info('GDRIVE: auth by service account')

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

        log.Info('GDRIVE: auth by app token')

      except:
        log.Info("GDRIVE: couldn't authenticate with token file")
        flow = OAuth2WebServerFlow(APP_ID, APP_SECRET, OAUTH_SCOPE, redirect_uri=REDIRECT_URI)

        # try auth with code
        if 'GDRIVE_APP_CODE' in os.environ:
          APP_CODE = os.environ['GDRIVE_APP_CODE']
          try:
            credentials = flow.step2_exchange(APP_CODE)
            log.Info('GDRIVE: auth by response code')
          except:
            authorize_url = flow.step1_get_authorize_url()
            raise BackendException("Invalid GDRIVE_APP_CODE, update it with code from: %s" % authorize_url)

          # save token
          fd = file(TOKEN_FILE, 'wb')
          fd.write(credentials.to_json())
          fd.close()
        else:
          authorize_url = flow.step1_get_authorize_url()
          raise BackendException("Authorization required, set GDRIVE_APP_CODE with code from: %s" % authorize_url)

    # not configured
    else:
      raise BackendException(
        'Configuration required: '
        '*** for Installed App: GDRIVE_APP_TOKEN_FILE, GDRIVE_APP_SECRET, gdrive://app_client_id/path '
        '*** for Service Account: GDRIVE_SERVICE_KEY_FILE, gdrive://service_account_email/path'
        )

    self.credentials = credentials
    self.path = parsed_url.path
    self.didInit = None


  def __start(self):
    if self.didInit is not None:
      return

    http = self.credentials.authorize(httplib2.Http())

    try:
      self.drive = build('drive', 'v2', http=http, cache_discovery=False)
    except Exception as e:
      raise BackendException("GDRIVE: build drive instance failed: %s: %s" % (e.__class__.__name__, e))

    # find or create folder tree
    parent_id = 'root'
    folder_names = string.split(self.path, '/')

    for folder_name in folder_names:
      if not folder_name: continue

      param = {'q' : "'" + parent_id + "' in parents and trashed = false"}

      page_token = None
      while True:
        if page_token: param['pageToken'] = page_token

        try:
          # list items on parent folder
          file_list = self.drive.files().list(**param).execute()
        except Exception as e:
          raise BackendException("GDRIVE: list files failed: %s: %s" % (e.__class__.__name__, e))

        # find folder
        folder = next((
          item for item in file_list['items'] if item['title'] == folder_name and
          item['mimeType'] == 'application/vnd.google-apps.folder'), None)
        if folder: break; # found

        page_token = file_list.get('nextPageToken')
        if not page_token: break

      # folder doesn't exist
      if folder is None:
        try:
          # create folder
          folder = self.drive.files().insert(body={
            'title': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [{'id': parent_id}]
            }).execute()
        except Exception as e:
          raise BackendException("GDRIVE: create folder '%s' failed: %s: %s" % (folder_name, e.__class__.__name__, e))

        log.Info("GDRIVE: created folder '%s'" % folder_name)

      # folder_name id
      parent_id = folder['id']

    # parsed_url.path id
    self.parent_id = parent_id

    self.didInit = True


  def __list(self):
    self.__start()

    param = {
      'q': "'" + self.parent_id + "' in parents and trashed = false",
      'maxResults': 1000
    }

    res = []
    page_token = None
    while True:
      if page_token: param['pageToken'] = page_token

      try:
        file_list = self.drive.files().list(**param).execute()
      except Exception as e:
        raise BackendException("GDRIVE: list files failed: %s: %s" % (e.__class__.__name__, e))

      res.extend([item['title'] for item in file_list['items']])

      page_token = file_list.get('nextPageToken')
      if not page_token: break

    log.Debug('GDRIVE: gdrive.__list() = %s' % res)

    return res


  def __getInfo(self, filename):
    try:
      file_list = self.drive.files().list(**{
        'q': "'" + self.parent_id + "' in parents and "
          "title = '" + filename.replace("\\", "\\\\").replace("'", "\\'") + "' and "
          "trashed = false"
        }).execute()
    except Exception as e:
      raise BackendException("GDRIVE: get file info '%s' failed: %s: %s" % (filename, e.__class__.__name__, e))

    if not file_list['items'][0]:
      return None
    return file_list['items'][0]


  def __query(self, filename):
    self.__start()

    f = self.__getInfo(filename)

    if f is None:
      size = -1
    else:
      size = int(f['fileSize'])

    log.Debug("GDRIVE: gdrive.__query('%s') = %d" % (filename, size))
    return {'size': size}


  def __delete(self, filename):
    self.__start()

    log.Debug("GDRIVE: gdrive.__delete('%s')" % filename)

    try:
      file_list = self.drive.files().list(**{
        'q': "'" + self.parent_id + "' in parents and "
          "title = '" + filename + "' and "
          "trashed = false"
        }).execute()
    except Exception as e:
      raise BackendException("GDRIVE: delete '%s' failed: %s: %s" % (filename, e.__class__.__name__, e))

    if file_list and file_list['items']:
      for item in file_list['items']:
        if item['title'] == filename:
          try:
            self.drive.files().delete(fileId=item['id']).execute()
          except Exception as e:
            raise BackendException("GDRIVE: delete '%s' failed: %s: %s" % (filename, e.__class__.__name__, e))

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
    self.__start()

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
      except Exception as e2:
        raise BackendException("GDRIVE: download '%s' failed: %s: %s" % (remote_filename, e2.__class__.__name__, e2))

      if (403 == code) and ('abuse' == reason):
        log.Info("GDRIVE: downloading with acknowledgeAbuse because google-api said error 403 '%s'" % msg)
        self.__try_download(fid, local_filename, 'true')
      else:
        raise BackendException("GDRIVE: download '%s' failed: %s: %s" % (remote_filename, e.__class__.__name__, e))

    log.Info("GDRIVE: download done '%s'" % remote_filename)


  def __put(self, local_filename, remote_filename):
    self.__start()

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
    try:
      self.drive.files().insert(body=body, media_body=media_body).execute()
    except Exception as e:
      raise BackendException("GDRIVE: upload '%s' failed: %s: %s" % (remote_filename, e.__class__.__name__, e))

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

duplicity.backend.register_backend('gdrive', GDriveBackend)
duplicity.backend.uses_netloc.extend(['gdrive'])



### for duplicity 0.6.23
#  @retry
#  def delete(self, filename_list, raise_errors=False):
#    try:
#      for filename in filename_list:
#        self.__delete(filename)
#    except Exception as e:
#      self.__error(str(e), raise_errors)
#
#  @retry
#  def get(self, remote_filename, local_path, raise_errors=False):
#    try:
#      self.__get(remote_filename, local_path.name)
#    except Exception as e:
#      self.__error(str(e), raise_errors)
#
#  @retry
#  def put(self, source_path, remote_filename=None, raise_errors=False):
#    try:
#      if not remote_filename: remote_filename = source_path.get_filename()
#      self.__put(source_path.name, remote_filename)
#    except Exception as e:
#      self.__error(str(e), raise_errors)
#
#  @retry
#  def _list(self, raise_errors=False):
#    try:
#      return self.__list()
#    except Exception as e:
#      self.__error(str(e), raise_errors)
#
#  @retry
#  def _query_file_info(self, filename, raise_errors=False):
#    try:
#      return self.__query(filename)
#    except Exception as e:
#      self.__error(str(e), raise_errors)
#      return {'size': None}
#
#  def __error(self, msg, raise_errors=True):
#    if raise_errors:
#      raise BackendException(msg)
#    else:
#      log.FatalError(msg, log.ErrorCode.backend_error)
#
#duplicity.backend.register_backend('gdrive', GDriveBackend)
#duplicity.backend._ensure_urlparser_initialized()
#duplicity.backend.urlparser.uses_netloc.extend(['gdrive'])
#duplicity.backend.urlparser.clear_cache()
#

