-- ============================================================
-- PROJET : Intégration hétérogène avec DuckDB
-- SOURCES : CSV, JSON, SQL, Parquet
-- OBJECTIF : Intégration + reconstruction des clés étrangères
-- ============================================================

-- Active l'affichage du temps d'exécution de chaque requête
.timer on

-- ============================================================
-- 1. INITIALISATION
-- ============================================================

-- Nettoyage si relance du script
DROP TABLE IF EXISTS Categorie;
DROP TABLE IF EXISTS Commentaire;
DROP TABLE IF EXISTS Competence;
DROP TABLE IF EXISTS Membre;
DROP TABLE IF EXISTS MotClef;
DROP TABLE IF EXISTS Qualification;
DROP TABLE IF EXISTS Talent;
DROP TABLE IF EXISTS Transaction;
DROP TABLE IF EXISTS ImageTransaction;


-- ============================================================
-- 2. CRÉATION DU SCHÉMA CIBLE (SANS CLÉS ÉTRANGÈRES)
-- ============================================================

-- Table Categorie (source CSV)
CREATE TABLE Categorie (
    idCategorie INTEGER PRIMARY KEY,
    categorie VARCHAR(100)
);


-- Table Commentaire (source Parquet)
CREATE TABLE Commentaire (
    idCommentaire INTEGER PRIMARY KEY,
    texteCommentaire VARCHAR,
    dateCommentaire TIMESTAMP,
    note INTEGER,
    idAuteur INTEGER,
    idTransaction INTEGER,
    idCommentaireSource INTEGER
);


-- Table Competence (source CSV)
CREATE TABLE Competence (
    idCompetence INTEGER PRIMARY KEY,
    competence VARCHAR(100),
    idCategorie INTEGER
);

-- Table Membre (source CSV)
CREATE TABLE Membre (
    idMembre INTEGER PRIMARY KEY,
    nom VARCHAR(100),
    prenom VARCHAR(100)
);


-- Table MotClef (source JSON)
CREATE TABLE MotClef (
    motClef VARCHAR(50) PRIMARY KEY
);


-- Table Qualification (source JSON)
CREATE Table Qualification (
    idMembre INTEGER,
    idCompetence INTEGER,
    motClef VARCHAR(50),
    PRIMARY KEY (idMembre, idCompetence, motClef)
);

-- Table Talent (source Parquet)
CREATE TABLE Talent (
    idMembre INTEGER,
    idCompetence INTEGER,
    description VARCHAR,
    PRIMARY KEY (idMembre, idCompetence)
);

-- Table Transaction (source CSV)
CREATE TABLE Transaction (
    idTransaction INTEGER PRIMARY KEY,
    dateTransaction TIMESTAMP,
    montantTransaction INTEGER,
    etat INTEGER,
    idBeneficiaire INTEGER,
    idFournisseur INTEGER
);

-- Table ImageTransaction (source Images BLOB)
CREATE TABLE ImageTransaction (
    idTransaction INTEGER,
    numeroPhoto INTEGER,
    image_path VARCHAR,
    PRIMARY KEY (idTransaction, numeroPhoto)
);
-- ============================================================
-- 3. IMPORT DES DONNÉES HÉTÉROGÈNES
-- ============================================================

-- CSV
INSERT INTO Categorie
SELECT *
FROM read_csv_auto('data/categorie.csv');

-- Parquet
INSERT INTO Commentaire
SELECT *
FROM read_parquet('data/commentaire.parquet');

-- CSV
INSERT INTO Competence
SELECT *
FROM read_csv_auto('data/competence.csv');

-- CSV
INSERT INTO Membre
SELECT *
FROM read_csv_auto('data/membre.csv');

-- JSON
INSERT INTO MotClef
SELECT *
FROM read_json_auto('data/mot_clef.json');

-- JSON
INSERT INTO Qualification
SELECT *
FROM read_json_auto('data/qualification.json');

-- Parquet
INSERT INTO Talent
SELECT *
FROM read_parquet('data/talent.parquet');

-- CSV
INSERT INTO Transaction
SELECT *
FROM read_csv_auto('data/transaction.csv');

-- Images BLOB
INSERT INTO ImageTransaction
SELECT
    try_cast(regexp_extract(split_part(file, '/', -1), 'transaction_([0-9]+)_([0-9]+)\.jpg', 1) AS INTEGER) AS idTransaction,
    try_cast(regexp_extract(split_part(file, '/', -1), 'transaction_([0-9]+)_([0-9]+)\.jpg', 2) AS INTEGER) AS numeroPhoto,
    file AS image_path
FROM glob('/data/photos_transactions_massif/*')
WHERE split_part(file, '/', -1) LIKE 'transaction_%_%';

-- ============================================================
-- 4. CONTRÔLES D’INTÉGRITÉ AVANT CLÉS ÉTRANGÈRES
-- ============================================================

------------ Table Commentaire

-- -- Vérification Commentaire -> Membre
-- SELECT COUNT(*) AS invalid_auteur_refs
-- FROM Commentaire
-- WHERE idAuteur NOT IN (
--     SELECT idMembre FROM Membre
-- );

-- -- Vérification Commentaire -> Transaction
-- SELECT COUNT(*) AS invalid_transaction_refs
-- FROM Commentaire
-- WHERE idTransaction NOT IN (
--     SELECT idTransaction FROM Transaction
-- );

-- -- Vérification Commentaire -> Commentaire (auto-référence)
-- SELECT COUNT(*) AS invalid_commentaire_source_refs
-- FROM Commentaire
-- WHERE idCommentaireSource IS NOT NULL
--   AND idCommentaireSource NOT IN (
--     SELECT idCommentaire FROM Commentaire
-- );

-- ------------ Table Competence

-- -- Vérification Competence -> Categorie
-- SELECT COUNT(*) AS invalid_categorie_refs
-- FROM Competence
-- WHERE idCategorie NOT IN (
--     SELECT idCategorie FROM Categorie
-- );

-- ------------ Table Qualification

-- -- Vérification Qualification -> Talent
-- SELECT COUNT(*) AS invalid_talent_refs
-- FROM Qualification q
-- WHERE (q.idMembre, q.idCompetence) NOT IN (
--     SELECT t.idMembre, t.idCompetence FROM Talent t
-- );

-- -- Vérification Qualification -> MotClef
-- SELECT COUNT(*) AS invalid_motclef_refs
-- FROM Qualification
-- WHERE motClef NOT IN (
--     SELECT motClef FROM MotClef
-- );

-- ------------ Table Talent

-- -- Vérification Talent -> Membre
-- SELECT COUNT(*) AS invalid_membre_refs
-- FROM Talent
-- WHERE idMembre NOT IN (
--     SELECT idMembre FROM Membre
-- );

-- -- Vérification Talent -> Competence
-- SELECT COUNT(*) AS invalid_competence_refs
-- FROM Talent
-- WHERE idCompetence NOT IN (
--     SELECT idCompetence FROM Competence
-- );

-- ------------ Table Transaction

-- -- Vérification Transaction -> Membre (Bénéficiaire)
-- SELECT COUNT(*) AS invalid_beneficiaire_refs
-- FROM Transaction
-- WHERE idBeneficiaire NOT IN (
--     SELECT idMembre FROM Membre
-- );

-- -- Vérification Transaction -> Membre (Fournisseur)
-- SELECT COUNT(*) AS invalid_fournisseur_refs
-- FROM Transaction
-- WHERE idFournisseur NOT IN (
--     SELECT idMembre FROM Membre
-- );

-- ===========================================================
-- REQUÊTES DE TEST
-- ===========================================================

-- SELECT pour avoir les 10 membres qui ont le plus dépensé
SELECT idMembre, nom, prenom, SUM(montantTransaction) AS total_depense
FROM Membre m
JOIN Transaction t ON m.idMembre = t.idBeneficiaire
GROUP BY idMembre, nom, prenom
ORDER BY total_depense DESC
LIMIT 10;

-- SELECT pour avoir les compétences d'un membre avec leurs catégories
SELECT *
FROM ImageTransaction;

-- SELECT pour avoir les transactions avec leurs images associées
SELECT
    t.idTransaction,
    t.montantTransaction,
    i.numeroPhoto,
    i.image_path
FROM Transaction t
JOIN ImageTransaction i
ON t.idTransaction = i.idTransaction
WHERE t.idTransaction = 36
ORDER BY i.numeroPhoto;

-- CREATE VIEW pour avoir les transactions avec leurs images associées
DROP VIEW IF EXISTS TransactionAvecImages;

CREATE VIEW TransactionAvecImages AS
SELECT
    t.*,
    i.numeroPhoto,
    i.image_path
FROM Transaction t
LEFT JOIN ImageTransaction i
ON t.idTransaction = i.idTransaction;



