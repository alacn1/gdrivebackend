Duplicity Google Drive Backend
==============================

Backend installation
--------------------
It was tested on duplicity 0.7.03 and 0.6.23.

To get it working on 0.6 need edit it and comment/uncomment some part of code.

To install copy gdrivebackend.py to duplicity's backends directory.

`/usr/lib/python2.7/dist-packages/duplicity/backends/`


Dependency installation
-----------------------
It requires google-api-python-client: https://github.com/google/google-api-python-client

To install execute:

`pip install --upgrade google-api-python-client`

or:

`easy_install --upgrade google-api-python-client`


Configuration
-------------
Create a Service Account or Installed App credential at developer console:
https://console.developers.google.com


### To configure with Installed App credential:
* Create a Installed App credential at google developer console.
* Get **client id** and **client secret key**.

```
export GDRIVE_APP_TOKEN_FILE='path_to_save_token_file.key'
export GDRIVE_APP_SECRET='client_secret_key'
gdrive://installed_app_client_id/path
```

* Run duplicity to get authorization url.

```
export GDRIVE_APP_CODE='response_code'
```

* Run duplicity to generate the token file.

Now GDRIVE_APP_CODE isn't needed any more and can be removed.


### To configure with Service Account credential:
Note that Service Account has it's own drive, it won't upload to your drive.

* Create a Service Account credential at google developer console.
* Get service account email address.
* Generate a .p12 key file.

```
export GDRIVE_SERVICE_KEY_FILE='path_to_key_file.p12'
gdrive://service_account_email/path
```
