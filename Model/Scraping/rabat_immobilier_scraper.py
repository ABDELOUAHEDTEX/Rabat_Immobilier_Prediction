import requests
from bs4 import BeautifulSoup
import csv
import time
import random
import re
import os
from urllib.parse import urljoin
from datetime import datetime

class MubawabScraper:
    def __init__(self, base_url):
        self.base_url = base_url
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
        }
        self.session = requests.Session()
        self.all_links = set()

    def get_page_content(self, url):
        """Fetch page content with error handling"""
        try:
            response = self.session.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return None

    def extract_listing_links(self, max_pages=37):
        """Extract all unique property listing links with improved logic"""
        for page in range(1, max_pages + 1):
            page_url = f"{self.base_url}:p:{page}"
            print(f"Scraping page {page}/{max_pages}: {page_url}")
            
            html_content = self.get_page_content(page_url)
            if not html_content:
                continue
                
            soup = BeautifulSoup(html_content, 'html.parser')
            
            listings = soup.find_all('div', class_=['listingBox', 'listingBoxsPremium'])
            if not listings:
                listings = soup.find_all('a', href=re.compile(r'/fr/[pa]/\d+'))
            
            if not listings:
                script_tags = soup.find_all('script', type='text/javascript')
                for script in script_tags:
                    if 'listingBox' in str(script):
                        listings = re.findall(r'href=[\'"]?([^\'" >]+)', str(script))
                        listings = [l for l in listings if '/fr/[pa]/\d+' in l]
                        break
            
            new_links = set()
            for listing in listings:
                if hasattr(listing, 'attrs'):
                    link = listing.find('a', href=True)
                    if link:
                        href = link['href']
                    else:
                        href = listing.get('linkref', '')
                else:
                    href = listing
                
                if not href:
                    continue
                
                if not href.startswith('http'):
                    href = urljoin('https://www.mubawab.ma', href)
                clean_url = re.sub(r'\?.*', '', href)
                
                if re.match(r'https://www.mubawab.ma/fr/[pa]/\d+', clean_url):
                    new_links.add(clean_url)
            
            new_count = len(new_links - self.all_links)
            self.all_links.update(new_links)
            
            print(f"Found {len(listings)} listing elements, {new_count} new unique links")
            print(f"Total unique links: {len(self.all_links)}")
            
            self.save_links_to_csv()
            time.sleep(random.uniform(3, 7))
            
            if new_count == 0 and page > 5:
                print("No new links found on multiple pages. Stopping early.")
                break
        
        return self.all_links

    def save_links_to_csv(self, filename="mubawab_links.csv"):
        """Save all unique links to CSV"""
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['URL'])
            writer.writerows([[link] for link in sorted(self.all_links)])
        print(f"Saved {len(self.all_links)} unique links to {filename}")

    def scrape_listing_details(self, url):
        """Scrape detailed information from a single property page with additional features"""
        print(f"Scraping details from: {url}")
        html_content = self.get_page_content(url)
        if not html_content:
            return None
            
        soup = BeautifulSoup(html_content, 'html.parser')
        data = {'url': url}
        
        try:
            prop_id = re.search(r'/fr/[pa]/(\d+)', url)
            data['id'] = prop_id.group(1) if prop_id else 'N/A'
            
            title = soup.find('h1', class_='titleListing')
            data['title'] = title.get_text(strip=True) if title else 'N/A'
            
            price = soup.find('h3', class_='orangeTit')
            if price:
                price_text = price.get_text(strip=True)
                data['price'] = re.sub(r'[^\d]', '', price_text) or 'N/A'
            else:
                data['price'] = 'N/A'
            
            location = soup.find('h2', class_='greyTit')
            data['location'] = location.get_text(strip=True) if location else 'N/A'
            
            features = {
                'area': ('icon-triangle', 'm²'),
                'rooms': ('icon-house-boxes', 'Pièces|places'),
                'bedrooms': ('icon-bed', 'Chambres'),
                'bathrooms': ('icon-bath', 'Salles de bain')
            }
            
            for field, (icon_class, pattern) in features.items():
                icon = soup.find('i', class_=icon_class)
                if icon:
                    parent = icon.find_parent('div', class_='adDetailFeature')
                    if parent:
                        span = parent.find('span')
                        if span:
                            text = span.get_text(strip=True)
                            match = re.search(r'(\d+)', text)
                            data[field] = match.group(1) if match else 'N/A'
                else:
                    data[field] = 'N/A'
            
            desc = soup.find('div', class_='blockDescription')
            desc_text = desc.get_text(strip=True) if desc else ''
            data['description'] = desc_text
            
            types = ['appartement', 'maison', 'villa', 'terrain', 'bureau', 'studio']
            lower_title = data['title'].lower()
            data['type'] = next((t for t in types if t in url.lower() or t in lower_title), 'N/A')

            # Property State
            property_state = 'N/A'
            state_tags = soup.find_all('span', class_='tag')
            state_mapping = {
                'neuf': 'Neuf',
                'nouveau': 'Neuf',
                'ancien': 'Ancien',
                'bon état': 'Bon état',
                'à rénover': 'À rénover'
            }
            for tag in state_tags:
                text = tag.get_text(strip=True).lower()
                for key, value in state_mapping.items():
                    if key in text:
                        property_state = value
                        break
                if property_state != 'N/A':
                    break
            data['property_state'] = property_state

            # Amenities
            amenities = {
                'garden': False,
                'pool': False,
                'equipped_kitchen': False
            }
            
            desc_lower = desc_text.lower()
            amenities['garden'] = 'jardin' in desc_lower or 'verdoyant' in desc_lower
            amenities['pool'] = 'piscine' in desc_lower
            amenities['equipped_kitchen'] = 'cuisine équipée' in desc_lower or 'cuisine equipee' in desc_lower
            
            features_section = soup.find('div', class_='featuresList')
            if features_section:
                features_text = features_section.get_text(strip=True).lower()
                amenities['garden'] = amenities['garden'] or 'jardin' in features_text
                amenities['pool'] = amenities['pool'] or 'piscine' in features_text
                amenities['equipped_kitchen'] = amenities['equipped_kitchen'] or 'cuisine équipée' in features_text or 'cuisine equipee' in features_text
            
            data.update({
                'jardin': 'Oui' if amenities['garden'] else 'Non',
                'piscine': 'Oui' if amenities['pool'] else 'Non',
                'cuisine_equiped': 'Oui' if amenities['equipped_kitchen'] else 'Non'
            })

            # Neighborhood
            neighborhood = 'N/A'
            if data['location'] != 'N/A':
                parts = [p.strip() for p in data['location'].split(',')]
                if len(parts) > 1:
                    neighborhood = parts[0]
            
            if neighborhood == 'N/A' and desc_text:
                neighborhood_keywords = ['quartier', 'secteur', 'zone', 'hay']
                for keyword in neighborhood_keywords:
                    if keyword in desc_text.lower():
                        start_idx = desc_text.lower().find(keyword)
                        neighborhood = desc_text[start_idx:].split(',')[0].strip()
                        break
            
            data['quartier'] = neighborhood

            # Status
            status = 'N/A'
            status_mapping = {
                'à vendre': 'À vendre',
                'à louer': 'À louer',
                'vendu': 'Vendu',
                'loué': 'Loué'
            }
            for tag in state_tags:
                text = tag.get_text(strip=True).lower()
                for key, value in status_mapping.items():
                    if key in text:
                        status = value
                        break
                if status != 'N/A':
                    break
            
            if status == 'N/A':
                if '/a-vendre/' in url.lower():
                    status = 'À vendre'
                elif '/a-louer/' in url.lower():
                    status = 'À louer'
            
            data['status'] = status

            return data
            
        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return None

    def scrape_all_listings(self, links_file="mubawab_links.csv", output_file="mubawab_data.csv"):
        """Scrape details from all saved links with additional fields"""
        try:
            with open(links_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)
                links = [row[0] for row in reader if row]
        except FileNotFoundError:
            print(f"Error: {links_file} not found")
            return
        
        print(f"Found {len(links)} listings to scrape")
        
        file_exists = os.path.exists(output_file)
        fieldnames = [
            'id', 'url', 'title', 'price', 'location', 'type', 
            'area', 'rooms', 'bedrooms', 'bathrooms', 'description',
            'property_state', 'jardin', 'piscine', 'cuisine_equiped', 
            'quartier', 'status'
        ]
        
        with open(output_file, 'a' if file_exists else 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            
            for i, link in enumerate(links, 1):
                print(f"Processing {i}/{len(links)}")
                data = self.scrape_listing_details(link)
                if data:
                    writer.writerow(data)
                    f.flush()
                
                time.sleep(random.uniform(5, 10))

if __name__ == "__main__":
    BASE_URL = "https://www.mubawab.ma/fr/ct/rabat/immobilier-a-vendre"
    scraper = MubawabScraper(BASE_URL)
    
    print("Mubawab Property Scraper")
    print("1. Extract listing links only")
    print("2. Scrape property details only")
    print("3. Run full scraping (links + details)")
    
    choice = input("Choose an option (1-3): ")
    
    if choice in ('1', '3'):
        scraper.extract_listing_links(max_pages=37)
    
    if choice in ('2', '3'):
        scraper.scrape_all_listings()
    
    print("Scraping completed!")