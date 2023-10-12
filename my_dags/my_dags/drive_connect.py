from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

my_credentials= 'credentials_module.json'

def login():
    
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(my_credentials)


    if gauth.access_token_expired:
        gauth.Refresh()
        gauth.SaveCredentialsFile(my_credentials)
    else:
        gauth.Authorize()

    credentials = GoogleDrive(gauth)
    return credentials

def upload_csv(path, id_folder):
    credentials=login()
    archivo= credentials.CreateFile({'parents':[{'kind': 'drive#fileLink', 'id': id_folder}]})
    archivo['title']= path.split('/')[-1]
    archivo.SetContentFile(path)
    archivo.Upload()
