# main.py crée la classe MyWidget, qui contient un label et un bouton.
# Kivy cherche automatiquement un fichier .kv basé sur le nom de la classe principale de l'application. 
# Ici la classe etant MainApp, il cherche le fichier main.kv (car Kivy retire le App du nom de classe).


from kivy.lang import Builder
from kivymd.app import MDApp
#from Model.google import *
from pandas import DataFrame
from datetime import datetime


import zipfile
import os
import pandas as pd
import glob


def Clean_ToGdrive(df):
        # Prepare les données du dataframe pour les importer dans Google sheet
                
        # Appliquer le format à toutes les colonnes de type datetime
        for col in df.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']):
            df[col] = df[col].apply(
                lambda x: x.strftime('%d/%m/%Y') if pd.notna(x) else '01/01/2999'
            )

        #Supprime toutes les valeurs NaN
        df = df.astype(object).fillna('')
        
        # Convertir toutes les valeurs en chaînes de caractères
        df = df.astype(str)
        df['Montant'] = pd.to_numeric(df['Montant'], errors='coerce').fillna(0).astype("Float64")
        df['Année'] = pd.to_numeric(df['Année'], errors='coerce').fillna(0).astype("Int64")
        df['annee date comptable'] = pd.to_numeric(df['annee date comptable'], errors='coerce').fillna(0).astype("Int64")
        
        return df

def add_new_operations_from_files(df: DataFrame,type_compte) -> DataFrame:
    # Parcours les fichiers issues de FT et CCM pour les ajouter dans le dataframe
    # en paramètre le dataframe source dans lequel seront ajoutés les noubvelles lignes
    if type_compte == 'Individuel':
        num_compteCCM = '00021637201'
        num_compteFT = 'Histo'
        prefix_cle_CCM = 'CCM'
    else:
        num_compteCCM = '00021637401'
        num_compteFT = 'vide'
        prefix_cle_CCM = '401_'
    

    try:
        directory = "./Data/"
        
        # Décompression du fichier Histo FT
        # =================================

        # Colonnes du fichier source : Date opération / Date valeur / libellé / Débit / Crédit
        file_pattern=directory+num_compteFT+"*.zip"

        if glob.glob(file_pattern):
            fileszip = [f for f in os.listdir(directory) if f.startswith(num_compteFT) and f.endswith(".zip") ]    
            
            if len(fileszip) > 0 and os.path.exists(directory+fileszip[0]) :
                print(directory+fileszip[0])

                #extraction du fichier zip
                with zipfile.ZipFile(directory+fileszip[0], 'r') as zip_ref:
                    zip_ref.extractall(directory)    

                # Lecture du fichier de données
                files = [f for f in os.listdir(directory) if f.startswith("Histo") and f.endswith(".csv") ]

                # Charge les données dans un dataframe
                print(directory+files[0])
                dfFT = pd.read_csv(directory+files[0],sep=";",encoding='latin1')

                #Prepare les données       
                dfFT["a"]=dfFT["Crédit"].str.replace(",",".").str.lstrip().fillna(0).astype(float)
                dfFT["b"]=dfFT["Débit"].str.replace(",",".").str.lstrip().fillna(0).astype(float)
        
                dfFT["montant"]=dfFT["a"]+dfFT["b"]
            
                #Calcule la clé
                dfFT['cle']="FT"+dfFT["Date opération"]+dfFT["libellé"]+dfFT["montant"].apply(lambda x: f"{x:.2f}") #dataFT["Montant"].astype(str)
        
                #Analyse si la clé a déjà été intégré dans le fichier final
                dfFT["existe"] = dfFT["cle"].isin(df["cle"]).map({True: "Oui", False: "Non"})
                dfFT["banque"] = "FT"
                dfFT['date_comptable'] = dfFT['Date opération']
                dfFT['compte'] = type_compte

                dfFT = dfFT.rename(columns={                
                    'libellé': 'libelle_simplifie',
                    'Date opération' : 'date_operation'
                })

                #suppression des fichiers traités
                print(directory+files[0])
                
                os.remove(directory+files[0])
                os.remove(directory+fileszip[0])

                #On ne garde que les lignes a ajouter
                dfFT = dfFT.loc[dfFT["existe"] == "Non", ['cle','compte','date_comptable', 'date_operation','libelle_simplifie','montant','banque']]
            else:
                #generation d'un df vide avec juste les champs
                dfFT = pd.DataFrame(['cle','compte','date_comptable', 'date_operation','libelle_simplifie','montant','banque'])
        else:
                #Pas de fichier FT
                #generation d'un df vide avec juste les champs
                dfFT = pd.DataFrame(['cle','compte','date_comptable', 'date_operation','libelle_simplifie','montant','banque'])

        
        # Colonnes du fichier source : Date / Date de valeur / Montant / Libellé / Solde

        # Recherche d'un fichier CCM à importer
        files = [f for f in os.listdir(directory) if f.startswith(num_compteCCM) and f.endswith('.csv') ]    
        
        if len(files) > 0 and os.path.exists(directory+files[0]) :
            # Charge les données dans un dataframe
            dfCCM = pd.read_csv(directory+files[0],sep=";",encoding='latin1')

            #Prepare les données               
            dfCCM["Montant"]=dfCCM["Montant"].str.replace(",",".").fillna(0).astype(float)
                    
            #Calcule la clé
            dfCCM['cle']=prefix_cle_CCM+dfCCM["Date"]+dfCCM["Libellé"]+dfCCM["Montant"].apply(lambda x: f"{x:.2f}") #dataFT["Montant"].astype(str)
        
            #Analyse si la clé a déjà été intégré dans le fichier final
            dfCCM["existe"] = dfCCM["cle"].isin(df["cle"]).map({True: "Oui", False: "Non"})
            dfCCM["banque"] = "CCM"
            dfCCM['date_operation'] = dfCCM["Date"]
            dfCCM['compte'] = type_compte

            dfCCM = dfCCM.rename(columns={
                'Date': 'date_comptable',
                'Montant': 'montant',
                'Libellé': 'libelle_simplifie'
            })
        
            #suppression des fichiers traités
            os.remove(directory+files[0])

            # Sélection des colonnes sans doublons
            dfCCM = dfCCM.loc[dfCCM['existe'] == 'Non', ['cle','compte','date_comptable', 'date_operation','libelle_simplifie','montant','banque']]
            print(dfCCM.head())
        else:
            dfCCM = pd.DataFrame(['cle','compte','date_comptable', 'date_operation','libelle_simplifie','montant','banque'])


        # Ajouter les colonnes manquantes à dft1  pour qu’il ait les mêmes colonnes que dft2
        for col in df.columns:
            if col not in dfCCM.columns:
                dfCCM[col] = None

            if col not in dfFT.columns:
                dfFT[col] = None

        # Réordonner les colonnes de dft1 pour qu’elles soient dans le même ordre que dft2
        dfCCM = dfCCM[df.columns]
        dfFT = dfFT[df.columns]


       
        # Etape : Nettoyage des données
        # -----------------------------
        # print('Nettotage')

        # dataSelection = pd.DataFrame(columns=['cle','compte','date_comptable', 'date_operation','libelle_simplifie','montant','banque'])
        # # Création du dataframe concaténé sans prendre les lignes vides
        # dataSelection = pd.concat([dfCCM.dropna(how='all'), dfFT.dropna(how='all'), df], ignore_index=True)
          
        # #Convertir champ en format datetime
        # dataSelection['date_comptable'] = pd.to_datetime(dataSelection['date_comptable'], errors='coerce')
        # dataSelection['date_operation'] = pd.to_datetime(dataSelection['date_operation'], errors='coerce')
            
        # # Définition des formats des colonnes
        # dataSelection['montant'] = pd.to_numeric(dataSelection['montant'], errors='coerce').astype("Float64")
        # dataSelection['annee'] = pd.to_numeric(dataSelection['annee'], errors='coerce').astype("Int64")
        # dataSelection['annee_comptable'] = pd.to_numeric(dataSelection['annee_comptable'], errors='coerce').astype("Int64")
        # dataSelection['annee_operation'] = pd.to_numeric(dataSelection['annee_operation'], errors='coerce').astype("Int64")
                
        # print('Resultat ',type_compte)
        # print(dfCCM['cle'].count())
        # print(dfFT['cle'].count())
        dataselection = merge_df(dfFT,dfCCM)
        return dataselection
    
    except Exception as e:
        print(f"Erreur lors de l'import : {e}")
        return None


def merge_df(df1: DataFrame, df2: DataFrame):
    dataselection = pd.DataFrame(columns=['cle','compte','date_comptable', 'date_operation','libelle_simplifie','montant','banque'])
    # Création du dataframe concaténé sans prendre les lignes vides
    dataselection = pd.concat([df1.dropna(how='all'), df2.dropna(how='all')], ignore_index=True)
        
    #Convertir champ en format datetime
    dataselection['date_comptable'] = pd.to_datetime(dataselection['date_comptable'], errors='coerce')
    dataselection['date_operation'] = pd.to_datetime(dataselection['date_operation'], errors='coerce')
        
    # Définition des formats des colonnes
    dataselection['montant'] = pd.to_numeric(dataselection['montant'], errors='coerce').astype("Float64")
    dataselection['annee'] = pd.to_numeric(dataselection['annee'], errors='coerce').astype("Int64")
    dataselection['annee_comptable'] = pd.to_numeric(dataselection['annee_comptable'], errors='coerce').astype("Int64")
    dataselection['annee_operation'] = pd.to_numeric(dataselection['annee_operation'], errors='coerce').astype("Int64")
            
    return dataselection

# def add_new_operations_commun_from_files(df: DataFrame) -> DataFrame:

#     try:
#         directory = "./Data/"

#          # Colonnes du fichier source : Date / Date de valeur / Montant / Libellé / Solde

#         print('CCMCommun')
        
#         # Recherche d'un fichier CCM à importer
#         files = [f for f in os.listdir(directory) if f.startswith('00021637401') and f.endswith('.csv') ]    
        
#         if len(files) > 0 and os.path.exists(directory+files[0]) :
#             print("CCM Commun")
#             # Charge les données dans un dataframe
#             dfCCM = pd.read_csv(directory+files[0],sep=";",encoding='latin1')

#             #Prepare les données               
#             dfCCM["Montant"]=dfCCM["Montant"].str.replace(",",".").fillna(0).astype(float)
                    
#             #Calcule la clé
#             dfCCM['cle']="CCM"+dfCCM["Date"]+dfCCM["Libellé"]+dfCCM["Montant"].apply(lambda x: f"{x:.2f}") #dataFT["Montant"].astype(str)
        
#             #Analyse si la clé a déjà été intégré dans le fichier final
#             dfCCM["existe"] = dfCCM["cle"].isin(df["cle"]).map({True: "Oui", False: "Non"})
#             dfCCM["banque"] = "CCM"
#             dfCCM['date_operation'] = dfCCM["Date"]
#             dfCCM['compte'] = 'Commun'
            
#             dfCCM = dfCCM.rename(columns={
#                 'Date': 'date_comptable',
#                 'Montant': 'montant',
#                 'Libellé': 'libelle_simplifie'
#             })
        
#             # Sélection des colonnes sans doublons
#             dfCCM = dfCCM.loc[dfCCM['existe'] == 'Non', ['cle','compte','date_comptable', 'date_operation','libelle_simplifie','montant','banque']]
#         else:
#             dfCCM = pd.DataFrame(['cle','compte','date_comptable', 'date_operation','libelle_simplifie','montant','banque'])


#         # Ajouter les colonnes manquantes à dft1  pour qu’il ait les mêmes colonnes que dft2
#         for col in df.columns:
#             if col not in dfCCM.columns:
#                 dfCCM[col] = None

#             if col not in dfFT.columns:
#                 dfFT[col] = None

#         # Réordonner les colonnes de dft1 pour qu’elles soient dans le même ordre que dft2
#         dfCCM = dfCCM[df.columns]
#         dfFT = dfFT[df.columns]


#         # Etape : Nettoyage des données
#         # -----------------------------
#         print('Nettotage')

#         dataSelection = pd.DataFrame(columns=['cle','compte','date_comptable', 'date_operation','libelle_simplifie','montant','banque'])

#         # Création du dataframe concaténé sans prendre les lignes vides
#         dataSelection = pd.concat([dfCCM.dropna(how='all'), df], ignore_index=True)
        
        
      
#         #Convertir champ en format datetime
#         dataSelection['date_comptable'] = pd.to_datetime(dataSelection['date_comptable'], errors='coerce')
#         dataSelection['date_operation'] = pd.to_datetime(dataSelection['date_operation'], errors='coerce')

     
        
#         # Définition des formats des colonnes
#         dataSelection['montant'] = pd.to_numeric(dataSelection['montant'], errors='coerce').astype("Float64")
#         dataSelection['annee'] = pd.to_numeric(dataSelection['annee'], errors='coerce').astype("Int64")
#         dataSelection['annee_comptable'] = pd.to_numeric(dataSelection['annee_comptable'], errors='coerce').astype("Int64")
#         dataSelection['annee_operation'] = pd.to_numeric(dataSelection['annee_operation'], errors='coerce').astype("Int64")
        
#         print('fin')    
#         return dataSelection
    
#     except Exception as e:
#         print(f"Erreur lors de l'import : {e}")
#         return None



#     #dataSelection = pd.concat([dataCCM.loc[dataCCM['Existe'] == 'Non', ['Clé','Date opération','Libelle simplifié','Montant','Date opération','Banque']].reset_index(drop=	True),df.reset_index(drop=True)], axis=1)
#     #dataSelection = pd.concat([df.reset_index(drop=True), dataCCM.reset_index(drop=True)], axis=1)
#     #print(df.index.is_unique)
#     #print(dataCCM.index.is_unique)
#     #Concatenation CCM
#     #if len(dataCCM) > 0:
#     #dataSelection = pd.concat([df,dataCCM.loc[dataCCM['Existe'] == 'Non', ['Clé','Date opération','Libelle simplifié','Montant','Date opération','Banque']]])
#     #dfinal = pd.concat([df,dataSelection], ignore_index=False)
#     #print(dfinal.head(100))


