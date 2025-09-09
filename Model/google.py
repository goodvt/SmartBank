#Prerequis
# pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
# pip install tabulate requests tqdm

import os.path
import io
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

import os

class Google():
  def __init__(self):
    self.creds=self.__get_authenticated_service()
    self.service=self.__get_drive_connection()
 

  def __get_authenticated_service(self):
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """
    SCOPES = [
      #'https://www.googleapis.com/auth/drive.readonly',
      'https://www.googleapis.com/auth/spreadsheets',
      'https://www.googleapis.com/auth/drive',
      'https://www.googleapis.com/auth/drive.file'
      ]
    creds = None
    
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    
    cheminToken = os.path.join("assets", "token.json")
    cheminCred = os.path.join("assets", "credentials.json")
    
    if os.path.exists(cheminToken):
      creds = Credentials.from_authorized_user_file(cheminToken, SCOPES)
    
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
      if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
      else:
        flow = InstalledAppFlow.from_client_secrets_file(
            cheminCred, SCOPES
        )
        creds = flow.run_local_server(port=0)
      # Save the credentials for the next run
      with open(cheminToken, "w") as token:
        token.write(creds.to_json())
    
    return creds  

  def __get_drive_connection(self):
    # Créer un service Google Drive
    service = build("drive", "v3", credentials=self.creds)
    return service

  def gg_download_file(self, file):
    file_id = self.__check_file_exists(file)
    
    self.__donwload_file(file_id=file_id)


  def __check_file_exists(self,file_to_check):
    try:
      # Call the Drive v3 API
      # Rechercher le fichier par son nom
      results = (
          self.service.files()
          #.list(pageSize=10, fields="nextPageToken, files(id, name)")
          .list(q="name='"+file_to_check+"'", spaces='drive',fields="files(id, name)")

          .execute()
      )
      items = results.get("files", [])


      if not items:
        print("No files found.")
        return
      
      file_id = items[0]['id']
      
      return file_id
    
    except HttpError as error:
      # TODO(developer) - Handle errors from drive API.
      print(f"An error occurred: {error}")

  def __donwload_file(self,file_id):
    try:
      # Télécharger le fichier natif google
      # request = self.service.files().export_media(
      #   fileId=file_id,
      #   mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
      # )

      # Pour les fichiers non-natifs de Google (comme les .zip, .xlsx, .pdf, .jpg, .txt)
        # tu dois utiliser files().get_media()
      request = self.service.files().get_media(fileId=file_id)
      
      #fh = io.BytesIO()
       # 1. Ouvre un fichier local en mode écriture binaire ('wb')
        #    C'est ici que le contenu téléchargé sera écrit.
      destination_path= 'C:/sources/LPF_tool/AppBank/myDB1.db'
      with open(destination_path, 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
          status, done = downloader.next_chunk()
          print("Download %d%%." % int(status.progress() * 100))
      
      print(f"Fichier téléchargé et sauvegardé sous : {destination_path}")

    except HttpError as error:
      print(f"Une erreur est survenue lors du téléchargement : {error}")
    except Exception as e:
      print(f"Une erreur inattendue est survenue : {e}")
          
  
  def __load_data_file(self):
    try:
      # Télécharger le fichier
      request = self.service.files().export_media(fileId=self.file_id,mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
      fh = io.BytesIO()
      downloader = MediaIoBaseDownload(fh, request)
      done = False
      while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%." % int(status.progress() * 100))
      
      #position du curseur au début du fichier
      fh.seek(0)

      return fh

    except HttpError as error:
      # TODO(developer) - Handle errors from drive API.
      print(f"An error occurred: {error}")

  def get_datafile(self):
    return self.datafile

  def get_gsheet_sheet(self,f,sheet):
    self.file=f
    self.file_id=self.__check_file_exists()
    self.datafile=self.__load_data_file()
    self.sheetName=sheet

  def set_gsheet_sheet(self, dataframe, sheet_name_destination):
    try:

      # Convertir le DataFrame en une liste de listes
      values = [dataframe.columns.tolist()] + dataframe.values.tolist()

      # Construire le corps de la requêt
      body = {
          'valueInputOption': 'RAW',
          'data': [
              {
                  'range': f'{sheet_name_destination}',
                  'majorDimension': 'ROWS',
                  'values': values
              }
          ]
      }

      # Appeler l'API Google Sheets pour mettre à jour les valeurs
      sheets_service = build('sheets', 'v4', credentials=self.creds)
      result = sheets_service.spreadsheets().values().batchUpdate(
          spreadsheetId=self.file_id ,
          body=body
      ).execute()

      print(f"Feuille '{sheet_name_destination}' mise à jour avec succès.")
    
      return result
    
    except Exception as e:
        print(f"Erreur lors de la mise à jour de la feuille : {e}")
        return None

  def upload_file_to_drive(self, local_file_path, folder_id=None):
    """
      Uploade un fichier local vers Google Drive.

      Args:
          self: L'instance de ta classe qui contient le service Google Drive (self.service).
          local_file_path (str): Le chemin complet du fichier local à uploader.
          folder_id (str, optional): L'ID du dossier Drive parent où uploader le fichier.
                                      Si None, le fichier sera uploadé à la racine de Drive.

      Returns:
          dict: Les métadonnées du fichier uploadé si succès, None sinon.
      """
    try:
      # Extraire le nom du fichier du chemin local
      file_name = os.path.basename(local_file_path)

      # Déterminer le type MIME du fichier
      # C'est important pour que Drive reconnaisse le type de fichier.
      # Pour des types MIME courants, Python peut souvent deviner, mais c'est mieux d'être explicite.
      # Exemple:
      if file_name.endswith('.xlsx'):
          mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
      elif file_name.endswith('.txt'):
          mime_type = 'text/plain'
      elif file_name.endswith('.pdf'):
          mime_type = 'application/pdf'
      else:
          # Fallback générique si le type n'est pas prédéfini
          import mimetypes
          mime_type = mimetypes.guess_type(file_name)[0] or 'application/octet-stream'

      # Métadonnées du fichier Drive
      file_metadata = {
          'name': file_name,
      }
      if folder_id:
          file_metadata['parents'] = [folder_id] # Associe le fichier à un dossier parent

      # Créer l'objet MediaFileUpload pour le contenu du fichier
      media = MediaFileUpload(local_file_path, mimetype=mime_type, resumable=True)

      # Exécuter la requête d'upload
      file = self.service.files().create(
          body=file_metadata,
          media_body=media,
          fields='id, name, mimeType' # Les champs à récupérer dans la réponse
      ).execute()

      print(f"Fichier '{file.get('name')}' (ID: {file.get('id')}) uploadé avec succès sur Drive.")
      return file

    except HttpError as error:
        print(f"Une erreur est survenue lors de l'upload : {error}")
        return None
    except Exception as e:
        print(f"Une erreur inattendue est survenue : {e}")
        return None


if __name__ == "__main__":
  gg=Google()
  gg.gg_download_file(file='1-Mes relevés')