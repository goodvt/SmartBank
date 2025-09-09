from kivy.app import App
import sqlite3
import pandas as pd
import db_init as dbinit

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

    def add_record(db, df, sql):
        try:
            temp_operations = 'operations_temp_sync'
             # 1. Écrire le DataFrame (modifié, ajouté, supprimé) dans une table temporaire
            #df.drop(columns=['id'], inplace=True) #si la colonne id est None pour les nouvelles lignes
            # et que la table est AUTOINCREMENT. Mais si vous avez des IDs existants, conservez 'id'.
            df.to_sql(temp_operations, db.conn, if_exists='replace', index=False)

            # 2. Effectuer l'UPSERT (UPDATE ou INSERT) de la table temporaire vers la table principale
            db.cursor.execute(f"""
            {sql}
            FROM {temp_operations}
            ;
            """)

            # 3. Gérer les suppressions : Supprimer les lignes de la table principale
            # qui ne sont PAS présentes dans la table temporaire (c'est-à-dire qui ont été supprimées du DataFrame)
            # self.cursor.execute(f"""
            # DELETE FROM categories
            # WHERE id NOT IN (SELECT id FROM {temp_categories} WHERE id IS NOT NULL);
            # """)

            db.conn.commit()

            # 4. Supprimer la table temporaire
            db.cursor.execute(f"DROP TABLE IF EXISTS {temp_operations}")
            db.conn.commit() # Ou un seul commit final est suffisant si toutes les opérations sont groupées

        except sqlite3.Error as e:
            print(f"Erreur SQLite lors de la mise à jour complète (méthode 3 améliorée): {e}")
        except Exception as e:
            print(f"Une erreur inattendue est survenue avec la méthode 3 améliorée: {e}")

    


    

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
 
if __name__ == "__main__":
    myDB = MyDataBase()
        
    myDB.connect()

    dbinit.setKilometrage(myDB)


    myDB.close()

