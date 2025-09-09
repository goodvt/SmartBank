from kivy.app import App
import sqlite3
import pandas as pd



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

               
def setKilometrage(db):
    # Création d'une table
    #self.cursor.execute("""drop table operations""")
    #self.conn.commit()

    db.cursor.execute("""
    CREATE TABLE IF NOT EXISTS kilometrage (
        date DATE NOT NULL PRIMARY KEY,
        km INTEGER NOT NULL,
        delta INTEGER NULL
    )
    """)
    db.conn.commit()