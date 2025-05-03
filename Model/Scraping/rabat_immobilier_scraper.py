import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import random
from tqdm import tqdm
import os
# Chemin du répertoire pour stocker les données
SCRIPT_DIR = "Rabat_Immobilier_Prediction\\Model\\Scraping"

# Dossier pour stocker les données
DATA_DIR = SCRIPT_DIR

def get_listing_links(page_url):
    """Returns: list: Liste des liens des annonces immobilières trouvées sur la page"""
    try:
        # Ajout d'un délai pour éviter de surcharger le serveur
        time.sleep(random.uniform(1, 3))
        # Envoi de la requête HTTP avec les en-têtes simulant un navigateur
        response = requests.get(page_url, timeout=30)
        
        if response.status_code == 200:
            # Analyse du contenu HTML avec BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Recherche des annonces immobilières dans la liste
            listing_container = soup.find('div', class_='ulListing')
            
            if listing_container:
                listings = listing_container.find_all('div', class_='listingBox')
                return [listing['linkref'] for listing in listings if 'linkref' in listing.attrs]
            else:
                print(f"Aucune liste d'annonces trouvée sur la page: {page_url}")
                return []
        else:
            print(f"Échec de récupération de la page. Code de statut: {response.status_code}")
            return []
    except Exception as e:
        print(f"Erreur lors de l'extraction des liens: {e}")
        return []

def extract_listing_details(link):
    """Args:link: URL de l'annonce immobilière
    Returns:dict: Dictionnaire contenant les détails de l'annonce"""
    # Initialisation des données par défaut
    property_data = {
        'Prix': 'N/A',
        'Quartier': 'N/A',
        'Superficie': 'N/A',
        'Pièces': 'N/A',
        'Chambres': 'N/A',
        'Salles_de_bain': 'N/A',
        'Type_bien': 'N/A',
        'Etat': 'N/A',
        'Standing': 'N/A',
        'Etat_bien': 'N/A',
        'Ascenseur': 0,
        'Garage': 0,
        'Jardin': 0,
        'Piscine': 0,
        'Cuisine_équipée': 0,
        'Vue_sur_mer': 0,
        'Terrasse': 0,
        'Sécurité': 0,
        'URL': link
    }
    
    try:
        # Ajout d'un délai pour éviter de surcharger le serveur
        time.sleep(random.uniform(1, 3))
        
        # Envoi de la requête HTTP
        response = requests.get(link, timeout=30)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extraction du prix
            price_tag = soup.find('h3', class_='orangeTit')
            if price_tag:
                property_data['Prix'] = ''.join(filter(str.isdigit, price_tag.get_text(strip=True)))
            
            # Extraction du quartier (localisation)
            quartier_div = soup.find('div', class_='col-8 vAlignM adBread')
            if quartier_div:
                quartier_tags = quartier_div.find_all('a', class_='darkblue')
                if quartier_tags:
                    property_data['Quartier'] = quartier_tags[-1].get_text(strip=True)
            
            # Extraction des détails à partir des icônes
            details_div = soup.find('div', class_='disFlex adDetails')
            if details_div:
                details = details_div.find_all('div', class_='adDetailFeature')
                for detail in details:
                    icon = detail.find('i', class_='adDetailFeatureIcon')
                    value = detail.find('span').get_text(strip=True) if detail.find('span') else 'N/A'
                    
                    if icon and 'class' in icon.attrs:
                        # Extraction de la superficie
                        if 'icon-triangle' in icon['class']:
                            superficie_match = re.findall(r'\d+', value)
                            property_data['Superficie'] = superficie_match[0] if superficie_match else 'N/A'
                        
                        # Extraction du nombre de pièces
                        elif 'icon-house-boxes' in icon['class']:
                            pieces_match = re.findall(r'\d+', value)
                            property_data['Pièces'] = pieces_match[0] if pieces_match else 'N/A'
                        
                        # Extraction du nombre de chambres
                        elif 'icon-bed' in icon['class']:
                            chambres_match = re.findall(r'\d+', value)
                            property_data['Chambres'] = chambres_match[0] if chambres_match else 'N/A'
                        
                        # Extraction du nombre de salles de bain
                        elif 'icon-bath' in icon['class']:
                            sdb_match = re.findall(r'\d+', value)
                            property_data['Salles_de_bain'] = sdb_match[0] if sdb_match else 'N/A'
            
            # Extraction des caractéristiques principales
            features_div = soup.find('div', class_='adFeatures')
            if features_div:
                main_features = features_div.find_all('div', class_='adMainFeature')
                for feature in main_features:
                    label_tag = feature.find('p', class_='adMainFeatureContentLabel')
                    value_tag = feature.find('p', class_='adMainFeatureContentValue')
                    
                    if label_tag and value_tag:
                        label = label_tag.get_text(strip=True)
                        value = value_tag.get_text(strip=True)
                        
                        # Assignation des valeurs selon les étiquettes
                        if label == 'Type de bien':
                            property_data['Type_bien'] = value
                        elif label == 'Etat':
                            property_data['Etat'] = value
                        elif label == 'Standing':
                            property_data['Standing'] = value
                        elif label == 'Etat du bien':
                            property_data['Etat_bien'] = value
                
                # Extraction des caractéristiques supplémentaires
                extra_tags_div = features_div.find_next('div', class_='adFeatures')
                if extra_tags_div:
                    extra_tags = extra_tags_div.find_all('div', class_='adFeature')
                    for extra_tag in extra_tags:
                        tag_span = extra_tag.find('span')
                        if tag_span:
                            tag_value = tag_span.get_text(strip=True)
                            
                            # Vérification des caractéristiques spécifiques
                            if 'Ascenseur' in tag_value:
                                property_data['Ascenseur'] = 1
                            elif 'Garage' in tag_value or 'Parking' in tag_value:
                                property_data['Garage'] = 1
                            elif 'Jardin' in tag_value:
                                property_data['Jardin'] = 1
                            elif 'Piscine' in tag_value:
                                property_data['Piscine'] = 1
                            elif 'Cuisine équipée' in tag_value:
                                property_data['Cuisine_équipée'] = 1
                            elif 'Vue sur mer' in tag_value:
                                property_data['Vue_sur_mer'] = 1
                            elif 'Terrasse' in tag_value:
                                property_data['Terrasse'] = 1
                            elif 'Sécurité' in tag_value or 'Gardien' in tag_value:
                                property_data['Sécurité'] = 1
            
            return property_data
        else:
            print(f"Échec de récupération de l'annonce: {link}. Code de statut: {response.status_code}")
            return property_data
    except Exception as e:
        print(f"Erreur lors de l'extraction des détails de l'annonce {link}: {e}")
        return property_data

def main(pages=37):
    # URL de base pour les pages d'annonces immobilières à Rabat
    base_url = 'https://www.mubawab.ma/fr/ct/rabat/immobilier-a-vendre'
    
    print("🏠 Démarrage du scraping des annonces immobilières à Rabat...")
    # Initialisation d'une liste pour stocker tous les liens d'annonces
    all_listing_links = []
    # Nombre de pages à scraper
    nb_pages = pages
    # Récupération des liens d'annonces pour chaque page
    print(f"📋 Collecte des liens d'annonces sur {nb_pages} pages...")
    for page_number in tqdm(range(1, nb_pages + 1), desc="Progression"):
        page_url = f"{base_url}:p:{page_number}"
        links = get_listing_links(page_url)
        all_listing_links.extend(links)
        print(f"  Page {page_number}: {len(links)} liens collectés.")
 
    total_links = len(all_listing_links)
    print(f"✅ Total des liens collectés: {total_links}")
    
    # Sauvegarde des liens dans un fichier CSV
    links_df = pd.DataFrame(all_listing_links, columns=['URL'])
    links_file = os.path.join(DATA_DIR, 'rabat_liens_annonces.csv')
    links_df.to_csv(links_file, index=False)
    
    # Extraction des détails pour chaque annonce
    print("🔍 Extraction des détails des annonces...")
    all_property_data = []
    
    for idx, link in enumerate(tqdm(all_listing_links, desc="Progression")):
        print(f"  Traitement de l'annonce {idx+1}/{total_links}: {link}")
        property_data = extract_listing_details(link)
        all_property_data.append(property_data)
        
        # Sauvegarde intermédiaire tous les 20 enregistrements
        if (idx + 1) % 20 == 0 or idx == len(all_listing_links) - 1:
            # Création d'un DataFrame avec les données collectées
            data_df = pd.DataFrame(all_property_data)
            
            # Sauvegarde des données brutes dans un fichier CSV, sans aucun traitement
            output_file = os.path.join(DATA_DIR, 'rabat_données_immobilières_brutes.csv')
            data_df.to_csv(output_file, index=False)
            print(f"💾 Sauvegarde intermédiaire: {idx+1} annonces traitées et sauvegardées dans {output_file}")
    
    print("✨ Scraping terminé avec succès!")
    print(f"📊 Les données brutes ont été sauvegardées dans: {DATA_DIR}")
if __name__ == "__main__":
    import argparse
    
    # Configuration du parser d'arguments
    parser = argparse.ArgumentParser(description="Script de scraping des annonces immobilières à Rabat")
    parser.add_argument("--pages", "-p", type=int, default=37, help="Nombre de pages à scraper (défaut: 37)")
    
    # Analyse des arguments
    args = parser.parse_args()
    
    # Lancement du scraping avec le nombre de pages spécifié
    main(pages=args.pages)