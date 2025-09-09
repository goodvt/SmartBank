from kivy.app import App
import sqlite3
import pandas as pd

class MyDataBase():
    
    def __init__(self):
        # Connexion à une base de données (ou création si elle n'existe pas)
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = sqlite3.connect("./assets/db/myDB.db")            
            self.cursor = self.conn.cursor()
            print("connected")
            return True
        except sqlite3.Error as e:
            return False

    def close(self):
        if self.conn:
            self.conn.close()            
            self.conn = None

        

    def getCategories(self):
        try:
            df = pd.read_sql_query("SELECT * FROM categories", self.conn)         
            #self.cursor.execute("SELECT * FROM categories")
            #self.cursor.fetchall()
        except pd.io.sql.DatabaseError as e:
            print(f"Erreur lors de la lecture de la base de données par pandas: {e}")
        except Exception as e:
            print(f"Une erreur inattendue est survenue: {e}")

        return df
    
    def getOperations(self):
        try:
            df = pd.read_sql_query("SELECT * FROM operations", self.conn)         
            #self.cursor.execute("SELECT * FROM categories")
            #self.cursor.fetchall()
        except pd.io.sql.DatabaseError as e:
            print(f"Erreur lors de la lecture de la base de données par pandas: {e}")
        except Exception as e:
            print(f"Une erreur inattendue est survenue: {e}")

        return df
    
    def setCategories1(self, df):
        # --- MÉTHODE : UPSERT via table temporaire + GESTION DES SUPPRESSIONS ---
        try:
            temp_categories = 'categories_temp_sync'
             # 1. Écrire le DataFrame (modifié, ajouté, supprimé) dans une table temporaire
            #df.drop(columns=['id'], inplace=True) #si la colonne id est None pour les nouvelles lignes
            # et que la table est AUTOINCREMENT. Mais si vous avez des IDs existants, conservez 'id'.
            df.to_sql(temp_categories, self.conn, if_exists='replace', index=False)

            # 2. Effectuer l'UPSERT (UPDATE ou INSERT) de la table temporaire vers la table principale
            self.cursor.execute(f"""
            INSERT OR REPLACE INTO categories (id, CodeCategorie, Catgorie, CodeSousCategorie, SousCategorie, CodeRubrique, Rubrique, CodeSecteur, Secteur)
            SELECT id, CodeCategorie, Catgorie, CodeSousCategorie, SousCategorie, CodeRubrique, Rubrique, CodeSecteur, Secteur
            FROM {temp_categories};
            """)

            # 3. Gérer les suppressions : Supprimer les lignes de la table principale
            # qui ne sont PAS présentes dans la table temporaire (c'est-à-dire qui ont été supprimées du DataFrame)
            self.cursor.execute(f"""
            DELETE FROM categories
            WHERE id NOT IN (SELECT id FROM {temp_categories} WHERE id IS NOT NULL);
            """)

            self.conn.commit()

            # 4. Supprimer la table temporaire
            self.cursor.execute(f"DROP TABLE IF EXISTS {temp_categories}")
            self.conn.commit() # Ou un seul commit final est suffisant si toutes les opérations sont groupées

        except sqlite3.Error as e:
            print(f"Erreur SQLite lors de la mise à jour complète (méthode 3 améliorée): {e}")
        except Exception as e:
            print(f"Une erreur inattendue est survenue avec la méthode 3 améliorée: {e}")


    def addOperations(self, df):
        # --- MÉTHODE : UPSERT via table temporaire + GESTION DES SUPPRESSIONS ---
        
        try:
            temp_operations = 'operations_temp_sync'
             # 1. Écrire le DataFrame (modifié, ajouté, supprimé) dans une table temporaire
            #df.drop(columns=['id'], inplace=True) #si la colonne id est None pour les nouvelles lignes
            # et que la table est AUTOINCREMENT. Mais si vous avez des IDs existants, conservez 'id'.
            df.to_sql(temp_operations, self.conn, if_exists='replace', index=False)

            # 2. Effectuer l'UPSERT (UPDATE ou INSERT) de la table temporaire vers la table principale
            self.cursor.execute(f"""
            INSERT OR REPLACE INTO operations(cle, compte, date_comptable, libelle_simplifie, montant, date_operation, 
                banque, note, imputation, code_famille, code_rubrique, code_categorie, code_nature, qui, annee, type_invest,
                annee_comptable, annee_operation)
            SELECT cle, compte, STRFTIME('%Y-%m-%d', date_comptable), libelle_simplifie, montant, STRFTIME('%Y-%m-%d', date_operation) , 
                banque, note, imputation, code_famille, code_rubrique, code_categorie, code_nature, qui, annee, type_invest,
                annee_comptable, annee_operation
            FROM {temp_operations}
            ;
            """)

            # 3. Gérer les suppressions : Supprimer les lignes de la table principale
            # qui ne sont PAS présentes dans la table temporaire (c'est-à-dire qui ont été supprimées du DataFrame)
            # self.cursor.execute(f"""
            # DELETE FROM categories
            # WHERE id NOT IN (SELECT id FROM {temp_categories} WHERE id IS NOT NULL);
            # """)

            self.conn.commit()

            # 4. Supprimer la table temporaire
            self.cursor.execute(f"DROP TABLE IF EXISTS {temp_operations}")
            self.conn.commit() # Ou un seul commit final est suffisant si toutes les opérations sont groupées

        except sqlite3.Error as e:
            print(f"Erreur SQLite lors de la mise à jour complète (méthode 3 améliorée): {e}")
        except Exception as e:
            print(f"Une erreur inattendue est survenue avec la méthode 3 améliorée: {e}")

    def updateOperation(self,row_to_update):
        try:
            cursor = self.conn.cursor()
         
            #, libelle = ?, categorie = ?, date = ?
            cursor.execute("""
                UPDATE operations
                SET code_categorie = ?, note= ?, imputation= ?
                WHERE id = ?
            """, (row_to_update['code_categorie'],row_to_update['note'],row_to_update['imputation'],
                row_to_update['id']))
            
            self.conn.commit()
            print("Base de données mise à jour par itération (méthode 1).")

        except sqlite3.Error as e:
            print(f"Erreur SQLite lors de la mise à jour par itération: {e}")
        except Exception as e:
            print(f"Une erreur inattendue est survenue avec la méthode 1: {e}")


    def autremethode(self,df,index):
        ### Méthode 1: Itérer sur les lignes et exécuter des UPDATE/INSERT (pour quelques modifications ciblées)
        # C'est viable si vous avez modifié un petit nombre de lignes et savez lesquelles.
        # Pour une application interactive où l'utilisateur modifie une ligne à la fois.

        # Pour cet exemple, nous allons recharger le DataFrame tel qu'il est *actuellement*
        # et simuler une mise à jour d'une seule ligne que nous avons modifiée.
        # Dans un cas réel, vous passeriez l'ID et les nouvelles valeurs de la ligne modifiée.

        # Re-charger le DataFrame pour simuler l'état actuel de la base de données avant la mise à jour
        # Cela est nécessaire si vous voulez comparer, sinon utilisez le 'df' modifié directement.
        df_before_update_method1 = pd.read_sql_query("SELECT * FROM operations", self.conn)

        print("\n--- MÉTHODE 1: Itération (pour mises à jour ciblées) ---")
        try:
            cursor = self.conn.cursor()

            # Exemple de mise à jour pour l'opération avec id=2
            # Il faudrait un mécanisme pour détecter quelles lignes ont été modifiées
            # ou passer la ligne modifiée à cette fonction.
            if 2 in df['id'].values:
                row_to_update = df[df['id'] == 2].iloc[0]
                cursor.execute("""
                    UPDATE operations
                    SET montant = ?, libelle = ?, categorie = ?, date = ?
                    WHERE id = ?
                """, (row_to_update['montant'], row_to_update['libelle'],
                    row_to_update['categorie'], row_to_update['date'],
                    row_to_update['id']))
                print(f"Opération ID {row_to_update['id']} mise à jour.")

            # Exemple de mise à jour pour l'opération avec id=1
            if 1 in df['id'].values:
                row_to_update_1 = df[df['id'] == 1].iloc[0]
                cursor.execute("""
                    UPDATE operations
                    SET montant = ?, libelle = ?, categorie = ?, date = ?
                    WHERE id = ?
                """, (row_to_update_1['montant'], row_to_update_1['libelle'],
                    row_to_update_1['categorie'], row_to_update_1['date'],
                    row_to_update_1['id']))
                print(f"Opération ID {row_to_update_1['id']} mise à jour.")

            self.conn.commit()
            print("Base de données mise à jour par itération (méthode 1).")

        except sqlite3.Error as e:
            print(f"Erreur SQLite lors de la mise à jour par itération: {e}")
        except Exception as e:
            print(f"Une erreur inattendue est survenue avec la méthode 1: {e}")

        # Recharger pour voir l'effet de la méthode 1
        df_after_method1 = pd.read_sql_query("SELECT * FROM operations", self.conn)
        print("\n--- État de la DB après MÉTHODE 1 ---")
        print(df_after_method1)
        
    def lire(self):

        # Lecture des données
        self.cursor.execute("SELECT * FROM utilisateurs")
        utilisateurs = self.cursor.fetchall()

        for utilisateur in utilisateurs:
            print(utilisateur)

        # Fermeture de la connexion
        self.cursor.execute("DELETE FROM utilisateurs")
        self.conn.commit()

    def setCategories(self):
        # Création d'une table
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code_famille TEXT NOT NULL,
            famille TEXT NOT NULL,
            code_rubrique TEXT NOT NULL,
            rubrique TEXT NOT NULL,
            code_categorie TEXT NOT NULL,
            categorie TEXT NOT NULL,
            code_nature TEXT NOT NULL,
            nature TEXT NOT NULL,
            icone TEXT NULL,
            actif INTEGER NOT NULL
        )
        """)

        self.conn.commit()
        print("table categorie créé")

        # Insertion de données catégorie
        self.cursor.execute("DELETE FROM categories")
        self.conn.commit()
    
        print("debut insert categorie créé")
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("C","Revenu","REV","Revenu","REV.Dividende","Dividende","S","Salaire","bank-transfer-in","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("C","Revenu","REV","Revenu","REV.Salaire","Salaire","S","Salaire","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("C","Revenu","REV","Revenu","REV.TrfBanque","Virement","T","Transfert","bank-transfer","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("C","Revenu","REV","Revenu","REV.Marie","Vers Fond Marie","T","Transfert","bank-transfer-in","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("C","Revenu","REV","Revenu","REV.Benoit","Vers Fond Benopit","T","Transfert","bank-transfer-in","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("C","Revenu","REV","Apport","Rev.MaisonMarie","Vers Fond Marie","T","Transfert","bank-transfer-in","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("C","Revenu","REV","Apport","Rev.MaisonBenoit","Vers Fond Benopit","T","Transfert","bank-transfer-in","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("D","Depense","DVC","Depense Vie Courante","DVC.Achats","Achats","D","Désir","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("D","Depense","DVC","Depense Vie Courante","DVC.Achats Vétements","Achats Vétements","D","Désir","tshirt-crew","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("D","Depense","DVC","Depense Vie Courante","DVC.Achats Habitat","Achats Habitat","D","Désir","home","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("D","Depense","DVC","Depense Vie Courante","DVC.Achats Exceptionnel","Achats Exceptionnel","D","Désir","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("D","Depense","DVC","Depense Vie Courante","DVC.Cadeaux","Cadeaux","D","Désir","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("D","Depense","DVC","Depense Vie Courante","DVC.Sport","Sport","D","Désir","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("D","Depense","DVC","Depense Vie Courante","DVC.Parking","Parking","D","Désir","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("D","Depense","DVC","Depense Vie Courante","DVC.Retrait Bancaire","Retrait Bancaire","D","Désir","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("D","Depense","DVC","Depense Vie Courante","DVC.Vacances","Vacances","D","Désir","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("E","Epargne","EPA","Epargne","EPA.Virement","Virement","E","Epargne","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("E","Epargne","EPA","Epargne","EPA.Virement mensuel","Virement mensuel","E","Epargne","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("R","Depense Récurente","DME","Depense Mensuelle","DME.Box Internet","Box Internet","B","Besoin","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("R","Depense Récurente","DME","Depense Mensuelle","DME.Forfait Mobile","Forfait Mobile","B","Besoin","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("R","Depense Récurente","IMP","Impots","IMP.Impôt","Impôt","B","Besoin","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("R","Depense Récurente","IMP","Impots","IMP.Impôt foncier","Impôt foncier","B","Besoin","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("R","Depense Récurente","LOI","Logement Immobilier","LOI.Energie Electrique","Energie Electrique","B","Besoin","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("R","Depense Récurente","LOI","Logement Immobilier","LOI.Energie Gaz","Energie Gaz","B","Besoin","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("R","Depense Récurente","LOR","Logement Remboursement","LOR.Prêt 1","Prêt 1","E","Epargne","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("T","Transfert","REV","Revenu","Rev.TrfCommun","Virement","T1","Transfert","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("R","Depense Récurente","SER","Service","SER.BPCE Assurance","BPCE Assurance","B","Besoin","road-variant","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("R","Depense Récurente","SER","Service","SER.Frais Bancaire","Frais Bancaire","B","Besoin","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("R","Depense Récurente","SER","Service","SER.Assurance Habitation","Assurance Habitation","B","Besoin","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("R","Depense Récurente","SER","Service","SER.Assurance Securite","Assurance Securite","B","Besoin","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("R","Depense Récurente","TRV","Transport Véhicule","TRV.Sanef","Sanef","B","Besoin","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("V","Depense","DVC","Depense Vie Courante","DVC.Commissions","Commissions","B","Besoin","food-apple","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("V","Depense","DVC","Depense Vie Courante","DVC.Restaurant","Restaurant","D","Désir","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("V","Depense","LOI","Logement Immobilier","LOI.Ordures Ménagères","Ordures Ménagères","B","Besoin","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("V","Depense","LOI","Logement Immobilier","LOI.EAU","EAU","B","Besoin","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("V","Depense","SAN","Santé Prévoyance","SAN.Santé Docteur","Maladie Doc","B","Besoin","hospital","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("V","Depense","TRV","Transport Véhicule","TRV.Assurance voiture","Assurance voiture","B","Besoin","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("V","Depense","TRV","Transport Véhicule","TRV.Entretien","Entretien","B","Besoin","car","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("V","Depense","TRV","Transport Véhicule","TRV.Essence","Essence","B","Besoin","","1"))
        self.cursor.execute("INSERT INTO categories (code_famille,famille,code_rubrique,rubrique,code_categorie,categorie,code_nature,nature,icone,actif) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("A","Autres","DVC","Apport","DVC.Maison","Achat Maison","D","Désir","","1"))
        self.conn.commit()

    def setOperations(self, df, typecompte):
        # Création d'une table
        #self.cursor.execute("""drop table operations""")
        #self.conn.commit()

        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS operations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cle TEXT NULL,
            compte TEXT NOT NULL,
            date_comptable DATE NOT NULL,
            libelle_simplifie TEXT NULL,
            montant INTEGER NOT NULL,
            date_operation DATE NULL,
            banque TEXT NOT NULL,
            note TEXT NULL,
            imputation DATE NULL,
            code_famille TEXT NULL,
            code_rubrique TEXT NULL,
            code_categorie TEXT NULL,
            code_nature TEXT NULL,
            qui TEXT NULL,         
            annee INTEGER NULL,
            type_invest INTEGER NULL,
            annee_comptable INTEGER NULL,
            annee_operation INTEGER NULL
        )
        """)
        self.conn.commit()

        
        for index, row in df.iterrows():
            #print(str(row["Code Categorie"]).split('.')[0])
            self.cursor.execute(
                "INSERT INTO operations(cle, compte, date_comptable, libelle_simplifie, montant, date_operation, "
                "banque, note, imputation, code_famille, code_rubrique, code_categorie, code_nature, qui, annee, type_invest,"
                "annee_comptable, annee_operation)"
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    row["Clé"],
                    typecompte, #"Commun", #"Commun"
                    row["Date Comptabilisation"],                    
                    row["Libellé simplifié"],
                    row["Montant"],
                    row["Date opération"],
                    row["Banque"],
                    row["Note"],
                    row["Imputation"],
                    row["Code Famille"],
                    #row["Rubrique"],
                    str(row["Code Categorie"]).split('.')[0],
                    row["Code Categorie"],
                    row["Code Nature"],
                    row['Qui'], #"B",
                    row["Année"],
                    row["TypeInvest"],
                    row["annee date comptable"],
                    row["annee date op"]
                )
            )
            
            self.conn.commit()

               
    

if __name__ == "__main__":
    myDB = MyDataBase()
        
    myDB.connect()
    #lst = myDB.getCategories()
    #print(lst.head())

    myDB.close()

