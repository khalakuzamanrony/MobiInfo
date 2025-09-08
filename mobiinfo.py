import requests
from bs4 import BeautifulSoup
import json
import time
import random
import os
import re
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
from functools import lru_cache

class MobiInfoScraper:
    def __init__(self):
        self.base_url = "https://www.mobiledokan.com"
        self.brands_url = "https://www.mobiledokan.com/mobile-brands"
        self.output_dir = "MobiInfo"
        os.makedirs(self.output_dir, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
        self.min_delay = 0.3  # Reduced minimum delay
        self.max_delay = 0.8  # Reduced maximum delay
        self.max_retries = 3
        self.final_data_path = os.path.join(self.output_dir, 'allbrands.json')
        self.brands_dir = os.path.join(self.output_dir, 'Brands')
        self.progress_path = os.path.join(self.output_dir, 'progress.json')
        self.changelog_path = os.path.join(self.output_dir, 'changelog.md')
        self.error_log_path = os.path.join(self.output_dir, 'error_log.txt')
        self.debug_log_path = os.path.join(self.output_dir, 'debug_log.txt')
        self.verbose_debug = False
        
        # Connection pooling for faster requests
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20,
            pool_maxsize=20,
            max_retries=3
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        # Determine the best available parser
        self.parser = self._get_best_parser()
        print(f"Using parser: {self.parser}")
        
        # Clean up old log files on initialization
        self.cleanup_old_logs()
        
        abs_output_dir = os.path.abspath(self.output_dir)
        print(f"MobiInfo data will be saved to: {abs_output_dir}")
        
    def _get_best_parser(self):
        """Determine the best available HTML parser"""
        try:
            # Try lxml first (fastest)
            from bs4 import BeautifulSoup
            BeautifulSoup("<html></html>", "lxml")
            return "lxml"
        except:
            try:
                # Try html5lib (most lenient)
                from bs4 import BeautifulSoup
                BeautifulSoup("<html></html>", "html5lib")
                return "html5lib"
            except:
                # Fall back to Python's built-in html.parser
                print("Warning: Neither lxml nor html5lib found. Using html.parser (slower).")
                print("For better performance, install lxml: pip install lxml")
                return "html.parser"
    
    def cleanup_old_logs(self, max_days=10):
        """Clean up log entries older than max_days"""
        try:
            # Clean debug log
            self._cleanup_log_file(self.debug_log_path, r'\[(DEBUG|STEP|SUCCESS) (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\]', max_days)
            
            # Clean error log
            self._cleanup_log_file(self.error_log_path, r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]', max_days)
            
            # Clean changelog (both JSON and Markdown)
            self._cleanup_changelog(max_days)
            
            print(f"Cleaned up log entries older than {max_days} days")
        except Exception as e:
            print(f"Error cleaning up old logs: {str(e)}")
    
    def _cleanup_log_file(self, file_path, date_pattern, max_days=10):
        """Clean up a log file, keeping only entries from the last max_days"""
        try:
            if not os.path.exists(file_path):
                return
                
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            if not lines:
                return
                
            pattern = re.compile(date_pattern)
            current_date = datetime.now()
            cutoff_date = current_date - timedelta(days=max_days)
            
            new_lines = []
            for line in lines:
                match = pattern.search(line)
                if match:
                    date_str = match.group(2) if len(match.groups()) > 1 else match.group(1)
                    try:
                        # Try different date formats
                        for fmt in ['%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S']:
                            try:
                                log_date = datetime.strptime(date_str, fmt)
                                break
                            except ValueError:
                                continue
                        else:
                            # If none of the formats worked, keep the line to be safe
                            new_lines.append(line)
                            continue
                            
                        if log_date >= cutoff_date:
                            new_lines.append(line)
                    except Exception as e:
                        # If there's an error parsing, keep the line
                        new_lines.append(line)
                else:
                    # If the line doesn't match the pattern, keep it
                    new_lines.append(line)
            
            # Write back the filtered lines
            with open(file_path, 'w', encoding='utf-8') as f:
                f.writelines(new_lines)
        except Exception as e:
            print(f"Error cleaning up log file {file_path}: {str(e)}")
    
    def _cleanup_changelog(self, max_days=10):
        """Clean up changelog entries, keeping only entries from the last max_days"""
        try:
            json_changelog_path = os.path.join(self.output_dir, 'changelog.json')
            if not os.path.exists(json_changelog_path):
                return
                
            with open(json_changelog_path, 'r', encoding='utf-8') as f:
                changelog = json.load(f)
            
            current_date = datetime.now()
            cutoff_date = current_date - timedelta(days=max_days)
            
            # Filter the changelog
            filtered_changelog = []
            for entry in changelog:
                try:
                    # Parse the timestamp from the entry
                    entry_date = datetime.strptime(entry['timestamp'], '%Y-%m-%d %H:%M:%S')
                    if entry_date >= cutoff_date:
                        filtered_changelog.append(entry)
                except Exception as e:
                    # If there's an error, keep the entry
                    filtered_changelog.append(entry)
            
            # Save the filtered changelog
            with open(json_changelog_path, 'w', encoding='utf-8') as f:
                json.dump(filtered_changelog, f, ensure_ascii=False, indent=2)
            
            # Also update the Markdown changelog
            self.save_changelog(filtered_changelog)
        except Exception as e:
            print(f"Error cleaning up changelog: {str(e)}")
    
    def log_debug(self, message):
        if self.verbose_debug:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            debug_msg = f"[DEBUG {timestamp}] {message}"
            print(debug_msg)
            try:
                # Clean up old entries before writing
                self._cleanup_log_file(self.debug_log_path, r'\[(DEBUG|STEP|SUCCESS) (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\]')
                
                with open(self.debug_log_path, 'a', encoding='utf-8') as f:
                    f.write(f"{debug_msg}\n")
            except Exception as e:
                print(f"Error writing to debug log: {str(e)}")
    
    def log_step(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        step_msg = f"[STEP {timestamp}] {message}"
        print(step_msg)
        try:
            # Clean up old entries before writing
            self._cleanup_log_file(self.debug_log_path, r'\[(DEBUG|STEP|SUCCESS) (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\]')
            
            with open(self.debug_log_path, 'a', encoding='utf-8') as f:
                f.write(f"{step_msg}\n")
        except Exception as e:
            print(f"Error writing to debug log: {str(e)}")
    
    def log_success(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        success_msg = f"[SUCCESS {timestamp}] {message}"
        print(success_msg)
        try:
            # Clean up old entries before writing
            self._cleanup_log_file(self.debug_log_path, r'\[(DEBUG|STEP|SUCCESS) (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\]')
            
            with open(self.debug_log_path, 'a', encoding='utf-8') as f:
                f.write(f"{success_msg}\n")
        except Exception as e:
            print(f"Error writing to debug log: {str(e)}")
    
    def log_error(self, error_msg):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Clean up old entries before writing
        self._cleanup_log_file(self.error_log_path, r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]')
        
        with open(self.error_log_path, 'a', encoding='utf-8') as f:
            f.write(f"[{timestamp}] {error_msg}\n")
    
    def get_page(self, url, max_retries=None, timeout=15):
        if max_retries is None:
            max_retries = self.max_retries
            
        for attempt in range(max_retries):
            try:
                # Reduced delay for faster processing
                delay = random.uniform(self.min_delay, self.max_delay)
                time.sleep(delay)
                
                self.log_debug(f"Requesting URL: {url} (Attempt {attempt + 1}/{max_retries})")
                
                response = self.session.get(url, timeout=timeout)
                response.raise_for_status()
                
                self.log_debug(f"Successfully fetched {len(response.text)} bytes from {url}")
                return response.text
            except requests.exceptions.Timeout:
                if attempt == max_retries - 1:
                    error_msg = f"Timeout fetching {url} after {max_retries} attempts"
                    print(error_msg)
                    self.log_error(error_msg)
                    return None
                backoff_time = min(2 ** attempt, 5)  # Cap backoff at 5 seconds
                print(f"Timeout for {url}. Retrying in {backoff_time} seconds...")
                time.sleep(backoff_time)
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    error_msg = f"Failed to fetch {url} after {max_retries} attempts: {str(e)}"
                    print(error_msg)
                    self.log_error(error_msg)
                    return None
                backoff_time = min(2 ** attempt, 5)
                print(f"Attempt {attempt + 1} failed for {url}. Retrying in {backoff_time} seconds...")
                time.sleep(backoff_time)
            except Exception as e:
                error_msg = f"Unexpected error fetching {url}: {str(e)}"
                print(error_msg)
                self.log_error(error_msg)
                return None
        return None
    
    @lru_cache(maxsize=50)
    def get_all_brands(self, max_brands=None):
        self.log_debug("Starting to fetch brands list")
        try:
            html_content = self.get_page(self.brands_url)
            
            if not html_content:
                print("Failed to fetch brands page")
                return []
            
            soup = BeautifulSoup(html_content, self.parser)
            brands = []
            
            for item in soup.select('ul.brand-list li.brand-list-item'):
                try:
                    a_tag = item.select_one('a.list-item-link')
                    if not a_tag or 'href' not in a_tag.attrs:
                        continue
                        
                    brand_url = a_tag['href']
                    brand_name = a_tag.select_one('h3.title').text.strip()
                    brand_id = self.generate_brand_id(brand_name)
                    
                    img_tag = a_tag.select_one('img')
                    img_url = img_tag['src'] if img_tag else None
                    
                    brands.append({
                        'id': brand_id,
                        'name': brand_name,
                        'url': brand_url,
                        'image_url': img_url
                    })
                    
                    if max_brands is not None and len(brands) >= max_brands:
                        break
                except Exception as e:
                    error_msg = f"Error processing brand item: {str(e)}"
                    print(error_msg)
                    self.log_error(error_msg)
                    continue
            
            self.log_success(f"Successfully fetched {len(brands)} brands")
            return brands
        except Exception as e:
            error_msg = f"Error in get_all_brands: {str(e)}"
            print(error_msg)
            self.log_error(error_msg)
            return []
    
    def find_brand_by_name(self, brand_name):
        self.log_debug(f"Searching for brand: {brand_name}")
        try:
            html_content = self.get_page(self.brands_url)
            
            if not html_content:
                print("Failed to fetch brands page")
                return None
            
            soup = BeautifulSoup(html_content, self.parser)
            
            for item in soup.select('ul.brand-list li.brand-list-item'):
                try:
                    a_tag = item.select_one('a.list-item-link')
                    if not a_tag or 'href' not in a_tag.attrs:
                        continue
                        
                    current_brand_name = a_tag.select_one('h3.title').text.strip()
                    
                    if current_brand_name.lower() == brand_name.lower():
                        brand_url = a_tag['href']
                        brand_id = self.generate_brand_id(current_brand_name)
                        
                        img_tag = a_tag.select_one('img')
                        img_url = img_tag['src'] if img_tag else None
                        
                        brand_data = {
                            'id': brand_id,
                            'name': current_brand_name,
                            'url': brand_url,
                            'image_url': img_url
                        }
                        
                        self.log_success(f"Found brand: {current_brand_name}")
                        return brand_data
                except Exception as e:
                    error_msg = f"Error processing brand item: {str(e)}"
                    print(error_msg)
                    self.log_error(error_msg)
                    continue
            
            print(f"Brand '{brand_name}' not found")
            return None
        except Exception as e:
            error_msg = f"Error in find_brand_by_name: {str(e)}"
            print(error_msg)
            self.log_error(error_msg)
            return None
    
    def generate_brand_id(self, brand_name):
        try:
            clean_name = re.sub(r'[^\w\s]', '', brand_name)
            clean_name = re.sub(r'\s+', '_', clean_name)
            clean_name = clean_name.lower()
            return clean_name
        except Exception as e:
            print(f"Error generating brand ID for {brand_name}: {str(e)}")
            self.log_error(f"Error generating brand ID for {brand_name}: {str(e)}")
            return brand_name.lower().replace(' ', '_')
    
    def get_phone_list(self, brand_url, max_pages=None, max_products=None):
        phones = []
        page = 1
        products_collected = 0
        
        self.log_debug(f"Starting to fetch phones for brand: {brand_url}")
        
        while True:
            if max_pages is not None and page > max_pages:
                break
                
            url = brand_url if page == 1 else f"{brand_url}?page={page}"
            
            self.log_debug(f"Fetching page {page} from {url}")
            
            try:
                html_content = self.get_page(url)
                
                if not html_content:
                    print(f"Failed to fetch page {page}")
                    break
                    
                soup = BeautifulSoup(html_content, self.parser)
                page_phones = []
                
                for item in soup.select('div.mobile-showcase-body ul li'):
                    try:
                        a_tag = item.select_one('a')
                        if not a_tag or 'href' not in a_tag.attrs:
                            continue
                            
                        phone_url = urljoin(self.base_url, a_tag['href'])
                        phone_name = a_tag.select_one('h3.product-title').text.strip()
                        
                        img_tag = a_tag.select_one('img.product-img')
                        img_url = img_tag['src'] if img_tag else None
                        
                        phone_id = self.generate_phone_id(phone_name)
                        
                        page_phones.append({
                            'id': phone_id,
                            'name': phone_name,
                            'url': phone_url,
                            'image_url': img_url
                        })
                    except Exception as e:
                        error_msg = f"Error processing phone item on page {page}: {str(e)}"
                        print(error_msg)
                        self.log_error(error_msg)
                        continue
                
                if not page_phones:
                    self.log_debug(f"No phones found on page {page}")
                    break
                    
                if max_products is not None:
                    remaining = max_products - products_collected
                    if remaining <= 0:
                        break
                    page_phones = page_phones[:remaining]
                
                phones.extend(page_phones)
                products_collected += len(page_phones)
                self.log_success(f"Found {len(page_phones)} phones on page {page} (Total: {products_collected})")
                
                pagination = soup.select('ul.pagination li')
                if not pagination or not any('Next' in li.text for li in pagination):
                    self.log_debug(f"No more pages found after page {page}")
                    break
                    
                page += 1
            except Exception as e:
                error_msg = f"Error processing page {page}: {str(e)}"
                print(error_msg)
                self.log_error(error_msg)
                break
        
        self.log_success(f"Successfully fetched {len(phones)} total phones for brand")
        return phones
    
    def generate_phone_id(self, phone_name):
        try:
            clean_name = re.sub(r'[^\w\s]', '', phone_name)
            clean_name = re.sub(r'\s+', '_', clean_name)
            clean_name = clean_name.lower()
            return clean_name
        except Exception as e:
            print(f"Error generating phone ID for {phone_name}: {str(e)}")
            self.log_error(f"Error generating phone ID for {phone_name}: {str(e)}")
            return phone_name.lower().replace(' ', '_')
    
    def get_phone_variants(self, phone_url):
        """Extract variant information for a phone with error handling"""
        try:
            html_content = self.get_page(phone_url)
            
            if not html_content:
                return []
            
            soup = BeautifulSoup(html_content, self.parser)
            variants = []
            
            # Find variant container
            variant_container = soup.select_one('ul.varcont')
            if variant_container:
                for variant_item in variant_container.select('li'):
                    try:
                        variant_link = variant_item.select_one('a')
                        if variant_link and 'href' in variant_link.attrs:
                            variant_url = urljoin(self.base_url, variant_link['href'])
                            
                            # Get variant name and price
                            variant_name_tag = variant_link.select_one('span.vtst')
                            variant_price_tag = variant_link.select_one('span.ptst')
                            
                            variant_name = variant_name_tag.text.strip() if variant_name_tag else "N/A"
                            variant_price = variant_price_tag.text.strip() if variant_price_tag else "N/A"
                            
                            variants.append({
                                'name': variant_name,
                                'price': variant_price,
                                'url': variant_url
                            })
                    except Exception as e:
                        error_msg = f"Error processing variant: {str(e)}"
                        print(error_msg)
                        self.log_error(error_msg)
                        continue
            
            return variants
        except Exception as e:
            error_msg = f"Error in get_phone_variants for {phone_url}: {str(e)}"
            print(error_msg)
            self.log_error(error_msg)
            return []
    
    def get_phone_specs(self, phone_url):
        """Extract detailed specifications for a phone with error handling"""
        try:
            html_content = self.get_page(phone_url)
            
            if not html_content:
                return {}
            
            soup = BeautifulSoup(html_content, self.parser)
            specs = {}
            
            # Try multiple approaches to find specification sections
            sections = []
            
            # Approach 1: Look for the product-specs section and then find rows within it
            specs_section = soup.select_one('section#product-specs')
            if specs_section:
                sections = specs_section.select('div.specs-tbl-wrapper > div.row.mb-2.pb-2.border-bottom')
            
            # Approach 2: If approach 1 didn't work, try a more general selector
            if not sections:
                sections = soup.select('section#product-specs div.row.mb-2.pb-2.border-bottom')
            
            # Approach 3: If still no sections, try to find any row with border-bottom that has a heading
            if not sections:
                sections = soup.select('div.row.mb-2.pb-2.border-bottom:has(h3.text-bold)')
            
            # Process the sections we found
            for section in sections:
                try:
                    # Get section title
                    section_title_tag = section.select_one('div.col-md-2 h3.text-bold')
                    if not section_title_tag:
                        continue
                        
                    section_title = section_title_tag.text.strip()
                    specs[section_title] = {}
                    
                    # Special handling for Camera section
                    if section_title == "Cameras":
                        self._extract_camera_specs(section, specs[section_title])
                        continue
                        
                    # Check if there are subgroups
                    subgroups = section.select('div.subgroup')
                    
                    if subgroups:
                        # Process each subgroup
                        for subgroup in subgroups:
                            try:
                                subgroup_title = subgroup.text.strip()
                                specs[section_title][subgroup_title] = {}
                                
                                # Find the next table after this subgroup
                                table = subgroup.find_next('table.spec-grp-tbl')
                                if table:
                                    self._extract_spec_table(table, specs[section_title][subgroup_title])
                            except Exception as e:
                                error_msg = f"Error processing subgroup in {section_title}: {str(e)}"
                                print(error_msg)
                                self.log_error(error_msg)
                                continue
                    else:
                        # No subgroups, just process tables directly
                        tables = section.select('table.spec-grp-tbl')
                        for table in tables:
                            try:
                                self._extract_spec_table(table, specs[section_title])
                            except Exception as e:
                                error_msg = f"Error processing table in {section_title}: {str(e)}"
                                print(error_msg)
                                self.log_error(error_msg)
                                continue
                except Exception as e:
                    error_msg = f"Error processing section: {str(e)}"
                    print(error_msg)
                    self.log_error(error_msg)
                    continue
            
            return specs
        except Exception as e:
            error_msg = f"Error in get_phone_specs for {phone_url}: {str(e)}"
            print(error_msg)
            self.log_error(error_msg)
            return {}
    
    def _extract_camera_specs(self, section, target_dict):
        """Special method to extract camera specifications with nested structure"""
        try:
            # Find the col-md-10 div that contains the camera specs
            col_md_10 = section.select_one('div.col-md-10')
            if not col_md_10:
                return
                
            # Find all subgroups (Primary Camera, Selfie Camera, etc.)
            subgroups = col_md_10.select('div.subgroup')
            
            for subgroup in subgroups:
                try:
                    subgroup_title = subgroup.text.strip()
                    target_dict[subgroup_title] = {}
                    
                    # Find all tables after this subgroup until next subgroup
                    current_element = subgroup.next_sibling
                    while current_element:
                        # If we hit another subgroup, we're done with this camera section
                        if current_element.name == 'div' and 'subgroup' in current_element.get('class', []):
                            break
                            
                        # Process table if found
                        if current_element.name == 'table' and 'spec-grp-tbl' in current_element.get('class', []):
                            self._extract_spec_table(current_element, target_dict[subgroup_title])
                            
                        # Move to next element
                        current_element = current_element.next_sibling
                except Exception as e:
                    error_msg = f"Error processing camera subgroup: {str(e)}"
                    print(error_msg)
                    self.log_error(error_msg)
                    continue
        except Exception as e:
            error_msg = f"Error in _extract_camera_specs: {str(e)}"
            print(error_msg)
            self.log_error(error_msg)
    
    def _extract_spec_table(self, table, target_dict):
        """Extract specifications from a table and add to target dictionary"""
        try:
            for row in table.select('tr'):
                key_td = row.select_one('td.td1')
                value_td = row.select_one('td.td2')
                
                if key_td and value_td:
                    key = key_td.text.strip()
                    
                    # Get the text content, including any text outside of SVG tags
                    value_text = value_td.get_text(strip=True)
                    
                    # Check if there are SVG icons
                    svg_tags = value_td.select('svg')
                    if svg_tags:
                        # If there's text content, use that
                        if value_text:
                            value = value_text
                        else:
                            # Check if it's a checkmark or X
                            if 'check-circle-fill' in str(svg_tags[0]):
                                value = "Yes"
                            elif 'x-circle-fill' in str(svg_tags[0]):
                                value = "No"
                            else:
                                value = "Yes"  # Default to Yes if it's an SVG but not an X
                    else:
                        value = value_text
                    
                    target_dict[key] = value
        except Exception as e:
            error_msg = f"Error in _extract_spec_table: {str(e)}"
            print(error_msg)
            self.log_error(error_msg)
    
    def get_phone_gallery(self, phone_url):
        """Extract all gallery images for a phone with enhanced error handling"""
        try:
            # Construct gallery URL
            parsed_url = urlparse(phone_url)
            gallery_path = parsed_url.path.rstrip('/') + '/gallery'
            gallery_url = urljoin(self.base_url, gallery_path)
            
            html_content = self.get_page(gallery_url)
            
            if not html_content:
                return []
            
            soup = BeautifulSoup(html_content, self.parser)
            images = []
            
            # Try multiple selectors for gallery images
            img_selectors = [
                'div.gallery img',
                'div.gallery-container img',
                'div.product-gallery img',
                'div.image-gallery img',
                'img[src*="media"]'
            ]
            
            for selector in img_selectors:
                try:
                    imgs = soup.select(selector)
                    if imgs:
                        for img in imgs:
                            if 'src' in img.attrs:
                                img_src = img['src']
                                # Make sure it's a full URL
                                if img_src.startswith('//'):
                                    img_src = 'https:' + img_src
                                elif img_src.startswith('/'):
                                    img_src = urljoin(self.base_url, img_src)
                                images.append(img_src)
                        break
                except Exception as e:
                    error_msg = f"Error processing gallery selector {selector}: {str(e)}"
                    print(error_msg)
                    self.log_error(error_msg)
                    continue
            
            return images
        except Exception as e:
            error_msg = f"Error in get_phone_gallery for {phone_url}: {str(e)}"
            print(error_msg)
            self.log_error(error_msg)
            return []
    
    def scrape_brand_phones(self, brand, max_pages=None, max_products=None):
        """Scrape phones for a specific brand with enhanced error handling"""
        self.log_debug(f"Starting to scrape brand: {brand['name']}")
        print(f"\n=== Scraping brand: {brand['name']} ===")
        try:
            # Get phone list for this brand
            phones = self.get_phone_list(brand['url'], max_pages, max_products)
            
            brand_data = {
                "id": brand['id'],
                "name": brand['name'],
                "url": brand['url'],
                "image_url": brand['image_url'],
                "phones": []
            }
            
            # Process each phone
            for i, phone in enumerate(phones):
                try:
                    print(f"\n{'-'*70}")
                    print(f"Processing phone {i+1}/{len(phones)}: {phone['name']}")
                    self.log_debug(f"Processing phone {i+1}/{len(phones)}: {phone['name']}")
                    
                    start_time = time.time()
                    
                    # Get variant information
                    print(f"Fetching variants for {phone['name']}...")
                    variants = self.get_phone_variants(phone['url'])
                    if variants:
                        print(f"Successfully fetched variants for {phone['name']}")
                    else:
                        print(f"Failed to fetch variants for {phone['name']}")
                    
                    # Get detailed specifications
                    print(f"Fetching specifications for {phone['name']}...")
                    specs = self.get_phone_specs(phone['url'])
                    if specs:
                        print(f"Successfully fetched specifications for {phone['name']}")
                    else:
                        print(f"Failed to fetch specifications for {phone['name']}")
                    
                    # Get gallery images
                    print(f"Fetching gallery images for {phone['name']}...")
                    gallery_images = self.get_phone_gallery(phone['url'])
                    if gallery_images:
                        print(f"Successfully fetched gallery images for {phone['name']}")
                    else:
                        print(f"Failed to fetch gallery images for {phone['name']}")
                    
                    phone_data = {
                        "id": phone['id'],
                        "name": phone['name'],
                        "url": phone['url'],
                        "image_url": phone['image_url'],
                        "variants": variants,
                        "specifications": specs,
                        "gallery_images": gallery_images
                    }
                    
                    brand_data["phones"].append(phone_data)
                    
                    # Calculate total time
                    elapsed = time.time() - start_time
                    print(f"Successfully written to JSON for {phone['name']} in {elapsed:.2f} seconds")
                    
                    # Add gap between phones
                    print(f"{'-'*70}")
                    
                except Exception as e:
                    error_msg = f"Error processing phone {phone['name']}: {str(e)}"
                    print(error_msg)
                    self.log_error(error_msg)
                    print(f"Failed to add to JSON for {phone['name']}")
                    continue
            
            self.log_success(f"Successfully scraped {len(brand_data['phones'])} phones for brand: {brand['name']}")
            return brand_data
        except Exception as e:
            error_msg = f"Error in scrape_brand_phones for {brand['name']}: {str(e)}"
            print(error_msg)
            self.log_error(error_msg)
            return {
                "id": brand['id'],
                "name": brand['name'],
                "url": brand['url'],
                "image_url": brand['image_url'],
                "phones": []
            }
    
    def update_and_save_changelog(self, changes_summary):
        """Helper method to update and save the changelog"""
        try:
            # Load existing changelog
            changelog = self.load_changelog()
            
            # Create new changelog entry
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            changelog_entry = {
                "timestamp": timestamp,
                "summary": {
                    "new_brands": changes_summary['new_brands'],
                    "updated_brands": changes_summary['updated_brands'],
                    "failed_brands": changes_summary['failed_brands'],
                    "new_phones": changes_summary['new_phones'],
                    "updated_phones": changes_summary['updated_phones'],
                    "failed_phones": changes_summary['failed_phones']
                },
                "details": changes_summary['changes_details']
            }
            
            # Add new entry to changelog
            changelog.append(changelog_entry)
            
            # Save changelog
            self.save_changelog(changelog)
            
            return True
        except Exception as e:
            error_msg = f"Error updating changelog: {str(e)}"
            print(error_msg)
            self.log_error(error_msg)
            return False
    
    def save_changelog(self, changelog):
        """Save changelog to both JSON and Markdown formats"""
        try:
            abs_path = os.path.abspath(self.changelog_path)
            print(f"\nSaving changelog to: {abs_path}")
            
            # Ensure the directory exists
            os.makedirs(os.path.dirname(self.changelog_path), exist_ok=True)
            
            with open(self.changelog_path, 'w', encoding='utf-8') as f:
                # Write header
                f.write("# MobiInfo Data Changelog\n\n")
                f.write("This document tracks all changes to the MobiInfo phone database.\n\n")
                
                # Write changelog entries in reverse chronological order (latest first)
                for entry in sorted(changelog, key=lambda x: x['timestamp'], reverse=True):
                    f.write(f"## {entry['timestamp']}\n\n")
                    
                    # Write summary
                    summary = entry['summary']
                    f.write("### Summary\n\n")
                    f.write(f"- **New brands**: {summary['new_brands']}\n")
                    f.write(f"- **Updated brands**: {summary['updated_brands']}\n")
                    f.write(f"- **Failed brands**: {summary['failed_brands']}\n")
                    f.write(f"- **New phones**: {summary['new_phones']}\n")
                    f.write(f"- **Updated phones**: {summary['updated_phones']}\n")
                    f.write(f"- **Failed phones**: {summary['failed_phones']}\n\n")
                    
                    # Write detailed changes
                    if entry['details']:
                        f.write("### Detailed Changes\n\n")
                        
                        for brand_change in entry['details']:
                            f.write(f"#### {brand_change['brand_name']}\n\n")
                            
                            if 'type' in brand_change and brand_change['type'] == 'new':
                                f.write(f"- **New brand added** with {brand_change['new_phones']} phones\n\n")
                            else:
                                f.write(f"- **New phones**: {brand_change['new_phones']}\n")
                                f.write(f"- **Updated phones**: {brand_change['updated_phones']}\n\n")
                                
                                if 'phone_changes' in brand_change:
                                    for phone_change in brand_change['phone_changes']:
                                        if phone_change['type'] == 'new':
                                            f.write(f"  - **New phone**: {phone_change['phone_name']}\n")
                                        else:
                                            f.write(f"  - **Updated phone**: {phone_change['phone_name']}\n")
                                            
                                            # Format differences as a nested list
                                            if 'differences' in phone_change:
                                                for diff in phone_change['differences']:
                                                    f.write(f"    - {diff}\n")
                                            
                                            f.write("\n")
                            
                            f.write("\n")
                    
                    f.write("---\n\n")
            
            # Also save the JSON version
            json_changelog_path = os.path.join(self.output_dir, 'changelog.json')
            with open(json_changelog_path, 'w', encoding='utf-8') as f:
                json.dump(changelog, f, ensure_ascii=False, indent=2)
            
            # Check if the file was created and has content
            if os.path.exists(self.changelog_path):
                file_size = os.path.getsize(self.changelog_path)
                print(f"Changelog file created successfully with size: {file_size} bytes")
                
                # Print the first few lines to verify content
                with open(self.changelog_path, 'r', encoding='utf-8') as f:
                    first_lines = ''.join([f.readline() for _ in range(3)])
                    print(f"Changelog preview:\n{first_lines}...")
            else:
                print("ERROR: Changelog file was not created!")
            
            print(f"Changelog saved to: {abs_path}")
            self.log_success(f"Changelog saved to {abs_path}")
        except Exception as e:
            error_msg = f"Error saving changelog: {str(e)}"
            print(error_msg)
            self.log_error(error_msg)
            traceback.print_exc()  # Print full traceback for debugging
    
    def scrape_multiple_brands(self, brand_inputs, max_pages=None, max_products=None):
        """Scrape multiple brands by names or URLs and create a single consolidated changelog
        
        Args:
            brand_inputs (list): List of brand names or URLs
            max_pages (int, optional): Maximum number of pages to scrape per brand
            max_products (int, optional): Maximum number of products to scrape per brand
            
        Returns:
            list: List of brand data with phones
        """
        if isinstance(brand_inputs, str):
            # If a single string is passed, convert to list
            brand_inputs = [brand_inputs]
        
        results = []
        total_brands = len(brand_inputs)
        
        # Initialize consolidated changes summary
        consolidated_changes = {
            'new_brands': 0,
            'updated_brands': 0,
            'failed_brands': 0,
            'new_phones': 0,
            'updated_phones': 0,
            'failed_phones': 0,
            'changes_details': []
        }
        
        print(f"\n=== Starting to scrape {total_brands} brands ===")
        
        for i, brand_input in enumerate(brand_inputs, 1):
            print(f"\n--- Processing brand {i}/{total_brands}: {brand_input} ---")
            result = self.scrape_single_brand_without_changelog(brand_input, max_pages, max_products)
            if result:
                results.append(result['brand_data'])
                # Aggregate the changes
                changes = result['changes_summary']
                consolidated_changes['new_brands'] += changes['new_brands']
                consolidated_changes['updated_brands'] += changes['updated_brands']
                consolidated_changes['failed_brands'] += changes['failed_brands']
                consolidated_changes['new_phones'] += changes['new_phones']
                consolidated_changes['updated_phones'] += changes['updated_phones']
                consolidated_changes['failed_phones'] += changes['failed_phones']
                consolidated_changes['changes_details'].extend(changes['changes_details'])
            else:
                print(f"Failed to scrape brand: {brand_input}")
                consolidated_changes['failed_brands'] += 1
        
        # Create a single consolidated changelog entry
        if consolidated_changes['changes_details']:
            print(f"\n=== Creating consolidated changelog for {len(results)} brands ===")
            self.update_and_save_changelog(consolidated_changes)
        
        print(f"\n=== Completed scraping {len(results)}/{total_brands} brands successfully ===")
        return results

    def scrape_single_brand_without_changelog(self, brand_input, max_pages=None, max_products=None, separate_files_mode=False):
        """Scrape a single brand by name or URL without creating changelog entry
        
        Args:
            brand_input (str): Brand name or URL
            max_pages (int, optional): Maximum number of pages to scrape
            max_products (int, optional): Maximum number of products to scrape
            separate_files_mode (bool, optional): If True, check individual brand files instead of consolidated data
            
        Returns:
            dict: Contains 'brand_data' and 'changes_summary'
        """
        try:
            # Check if brand_input is a URL
            if brand_input.startswith('http'):
                # It's a URL, extract brand name from URL
                parsed_url = urlparse(brand_input)
                path_parts = parsed_url.path.strip('/').split('/')
                if len(path_parts) >= 2 and path_parts[0] == 'mobile-brand':
                    brand_name = path_parts[1].replace('-', ' ').title()
                    brand_url = brand_input
                else:
                    print(f"Invalid brand URL: {brand_input}")
                    return None
            else:
                # It's a brand name
                brand_name = brand_input
                brand_info = self.find_brand_by_name(brand_name)
                if not brand_info:
                    return None
                brand_url = brand_info['url']
            
            # Create brand object
            brand_id = self.generate_brand_id(brand_name)
            brand = {
                'id': brand_id,
                'name': brand_name,
                'url': brand_url,
                'image_url': None
            }
            
            # Load existing data
            existing_data = self.load_existing_data()
            
            # Initialize data structure if it doesn't exist
            if not existing_data:
                existing_data = {
                    "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "brands": []
                }
            
            # Create a dictionary of existing brands by ID for easier lookup
            existing_brands_dict = {brand['id']: brand for brand in existing_data['brands']}
            
            # Scrape the brand
            print(f"\n=== Scraping brand: {brand_name} ===")
            brand_data = self.scrape_brand_phones(brand, max_pages, max_products)
            
            # Initialize changes summary
            changes_summary = {
                'new_brands': 0,
                'updated_brands': 0,
                'failed_brands': 0,
                'new_phones': 0,
                'updated_phones': 0,
                'failed_phones': 0,
                'changes_details': []
            }
            
            # Initialize phone changes list
            phone_changes_list = []
            
            # Check if this is a new brand or an update to an existing one
            # For separate files mode, check if individual brand file exists
            if separate_files_mode:
                brand_file_path = os.path.join(self.brands_dir, f"{brand['id']}.json")
                is_existing_brand = os.path.exists(brand_file_path)
                
                if is_existing_brand:
                    # Load existing brand data from separate file
                    try:
                        with open(brand_file_path, 'r', encoding='utf-8') as f:
                            existing_brand_file = json.load(f)
                            existing_brand = {
                                'id': existing_brand_file['brand_info']['id'],
                                'name': existing_brand_file['brand_info']['name'],
                                'url': existing_brand_file['brand_info']['url'],
                                'image_url': existing_brand_file['brand_info']['image_url'],
                                'phones': existing_brand_file['phones']
                            }
                    except Exception as e:
                        print(f"Error loading existing brand file: {str(e)}")
                        is_existing_brand = False
                        existing_brand = None
                else:
                    existing_brand = None
            else:
                is_existing_brand = brand['id'] in existing_brands_dict
                existing_brand = existing_brands_dict.get(brand['id'])
            
            if is_existing_brand and existing_brand:
                # Existing brand - update it
                changes_summary['updated_brands'] = 1
                
                # Create a dictionary of existing phones by ID
                existing_phones_dict = {phone['id']: phone for phone in existing_brand['phones']}
                
                # Track changes
                new_phones = 0
                updated_phones = 0
                
                # Process each phone in the scraped data
                for phone in brand_data['phones']:
                    if phone['id'] in existing_phones_dict:
                        # Existing phone - check for updates
                        existing_phone = existing_phones_dict[phone['id']]
                        
                        # Compare phone data
                        if json.dumps(existing_phone, sort_keys=True) != json.dumps(phone, sort_keys=True):
                            updated_phones += 1
                            changes_summary['updated_phones'] += 1
                            
                            # Find specific differences
                            differences = self.find_differences(existing_phone, phone)
                            print(f"\nUpdated phone: {phone['name']}")
                            for diff in differences:
                                print(f"  - {diff}")
                            
                            # Add to phone changes list
                            phone_changes_list.append({
                                'type': 'updated',
                                'phone_name': phone['name'],
                                'phone_id': phone['id'],
                                'differences': differences
                            })
                    else:
                        # New phone
                        new_phones += 1
                        changes_summary['new_phones'] += 1
                        print(f"\nNew phone: {phone['name']}")
                        
                        # Add to phone changes list
                        phone_changes_list.append({
                            'type': 'new',
                            'phone_name': phone['name'],
                            'phone_id': phone['id']
                        })
                
                # Create brand change entry
                brand_change = {
                    'brand_id': brand['id'],
                    'brand_name': brand['name'],
                    'new_phones': new_phones,
                    'updated_phones': updated_phones,
                    'phone_changes': phone_changes_list
                }
                
                # Add to changes details
                changes_summary['changes_details'].append(brand_change)
                
                # Merge the phone data
                existing_phones_dict.update({phone['id']: phone for phone in brand_data['phones']})
                existing_brand['phones'] = list(existing_phones_dict.values())
                existing_brand['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                print(f"\nBrand {brand_name} updated: {new_phones} new phones, {updated_phones} updated phones")
            else:
                # New brand
                changes_summary['new_brands'] = 1
                changes_summary['new_phones'] = len(brand_data['phones'])
                
                # Create phone changes list for new brand
                for phone in brand_data['phones']:
                    phone_changes_list.append({
                        'type': 'new',
                        'phone_name': phone['name'],
                        'phone_id': phone['id']
                    })
                
                # Create brand change entry
                brand_change = {
                    'brand_id': brand['id'],
                    'brand_name': brand['name'],
                    'type': 'new',
                    'new_phones': len(brand_data['phones']),
                    'updated_phones': 0,
                    'phone_changes': phone_changes_list
                }
                
                # Add to changes details
                changes_summary['changes_details'].append(brand_change)
                
                # Add the new brand to our data
                brand_data['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                existing_data['brands'].append(brand_data)
                existing_brands_dict[brand['id']] = brand_data
                
                print(f"\nNew brand added: {brand_name} with {len(brand_data['phones'])} phones")
            
            # Save the updated data (but don't create changelog yet)
            self.save_final_data(existing_data)
            
            return {
                'brand_data': brand_data,
                'changes_summary': changes_summary
            }
        except Exception as e:
            error_msg = f"Error in scrape_single_brand_without_changelog: {str(e)}"
            print(error_msg)
            self.log_error(error_msg)
            return None

    def scrape_single_brand(self, brand_input, max_pages=None, max_products=None):
        """Scrape a single brand by name or URL
        
{{ ... }}
        Args:
            brand_input (str): Brand name or URL
            max_pages (int, optional): Maximum number of pages to scrape
            max_products (int, optional): Maximum number of products to scrape
            
        Returns:
            dict: Brand data with phones
        """
        try:
            # Check if brand_input is a URL
            if brand_input.startswith('http'):
                # It's a URL, extract brand name from URL
                parsed_url = urlparse(brand_input)
                path_parts = parsed_url.path.strip('/').split('/')
                if len(path_parts) >= 2 and path_parts[0] == 'mobile-brand':
                    brand_name = path_parts[1].replace('-', ' ').title()
                    brand_url = brand_input
                else:
                    print(f"Invalid brand URL: {brand_input}")
                    return None
            else:
                # It's a brand name
                brand_name = brand_input
                brand_info = self.find_brand_by_name(brand_name)
                if not brand_info:
                    return None
                brand_url = brand_info['url']
            
            # Create brand object
            brand_id = self.generate_brand_id(brand_name)
            brand = {
                'id': brand_id,
                'name': brand_name,
                'url': brand_url,
                'image_url': None
            }
            
            # Load existing data
            existing_data = self.load_existing_data()
            
            # Initialize data structure if it doesn't exist
            if not existing_data:
                existing_data = {
                    "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "brands": []
                }
            
            # Create a dictionary of existing brands by ID for easier lookup
            existing_brands_dict = {brand['id']: brand for brand in existing_data['brands']}
            
            # Scrape the brand
            print(f"\n=== Scraping single brand: {brand_name} ===")
            brand_data = self.scrape_brand_phones(brand, max_pages, max_products)
            
            # Initialize changes summary
            changes_summary = {
                'new_brands': 0,
                'updated_brands': 0,
                'failed_brands': 0,
                'new_phones': 0,
                'updated_phones': 0,
                'failed_phones': 0,
                'changes_details': []
            }
            
            # Initialize phone changes list
            phone_changes_list = []
            
            # Check if this is a new brand or an update to an existing one
            if brand['id'] in existing_brands_dict:
                # Existing brand - update it
                existing_brand = existing_brands_dict[brand['id']]
                changes_summary['updated_brands'] = 1
                
                # Create a dictionary of existing phones by ID
                existing_phones_dict = {phone['id']: phone for phone in existing_brand['phones']}
                
                # Track changes
                new_phones = 0
                updated_phones = 0
                
                # Process each phone in the scraped data
                for phone in brand_data['phones']:
                    if phone['id'] in existing_phones_dict:
                        # Existing phone - check for updates
                        existing_phone = existing_phones_dict[phone['id']]
                        
                        # Compare phone data
                        if json.dumps(existing_phone, sort_keys=True) != json.dumps(phone, sort_keys=True):
                            updated_phones += 1
                            changes_summary['updated_phones'] += 1
                            
                            # Find specific differences
                            differences = self.find_differences(existing_phone, phone)
                            print(f"\nUpdated phone: {phone['name']}")
                            for diff in differences:
                                print(f"  - {diff}")
                            
                            # Add to phone changes list
                            phone_changes_list.append({
                                'type': 'updated',
                                'phone_name': phone['name'],
                                'phone_id': phone['id'],
                                'differences': differences
                            })
                    else:
                        # New phone
                        new_phones += 1
                        changes_summary['new_phones'] += 1
                        print(f"\nNew phone: {phone['name']}")
                        
                        # Add to phone changes list
                        phone_changes_list.append({
                            'type': 'new',
                            'phone_name': phone['name'],
                            'phone_id': phone['id']
                        })
                
                # Create brand change entry
                brand_change = {
                    'brand_id': brand['id'],
                    'brand_name': brand['name'],
                    'new_phones': new_phones,
                    'updated_phones': updated_phones,
                    'phone_changes': phone_changes_list
                }
                
                # Add to changes details
                changes_summary['changes_details'].append(brand_change)
                
                # Merge the phone data
                existing_phones_dict.update({phone['id']: phone for phone in brand_data['phones']})
                existing_brand['phones'] = list(existing_phones_dict.values())
                existing_brand['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                print(f"\nBrand {brand_name} updated: {new_phones} new phones, {updated_phones} updated phones")
            else:
                # New brand
                changes_summary['new_brands'] = 1
                changes_summary['new_phones'] = len(brand_data['phones'])
                
                # Create phone changes list for new brand
                for phone in brand_data['phones']:
                    phone_changes_list.append({
                        'type': 'new',
                        'phone_name': phone['name'],
                        'phone_id': phone['id']
                    })
                
                # Create brand change entry
                brand_change = {
                    'brand_id': brand['id'],
                    'brand_name': brand['name'],
                    'type': 'new',
                    'new_phones': len(brand_data['phones']),
                    'updated_phones': 0,
                    'phone_changes': phone_changes_list
                }
                
                # Add to changes details
                changes_summary['changes_details'].append(brand_change)
                
                # Add the new brand to our data
                brand_data['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                existing_data['brands'].append(brand_data)
                existing_brands_dict[brand['id']] = brand_data
                
                print(f"\nNew brand added: {brand_name} with {len(brand_data['phones'])} phones")
            
            # Save the updated data
            self.save_final_data(existing_data)
            
            # Update and save changelog
            self.update_and_save_changelog(changes_summary)
            
            return brand_data
        except Exception as e:
            error_msg = f"Error in scrape_single_brand: {str(e)}"
            print(error_msg)
            self.log_error(error_msg)
            return None
    
    def scrape_all_brands(self, max_brands=None, max_pages_per_brand=None, max_products_per_brand=None, max_workers=2):
        """Scrape data for multiple brands with enhanced error handling
        
        Args:
            max_brands (int, optional): Maximum number of brands to scrape. If None, scrape all brands.
            max_pages_per_brand (int, optional): Maximum number of pages to scrape per brand.
            max_products_per_brand (int, optional): Maximum number of products to scrape per brand.
            max_workers (int, optional): Maximum number of concurrent workers. Default is 2.
        """
        try:
            # Store the number of workers for adaptive delay calculation
            self._current_workers = max_workers
            self.log_debug(f"Starting scrape with {max_workers} workers")
            
            # Get all brands
            brands = self.get_all_brands(max_brands)
            
            if not brands:
                print("No brands found to scrape")
                return
            
            # Load existing data and changelog
            existing_data = self.load_existing_data()
            changelog = self.load_changelog()
            
            # Initialize data structure
            all_brands_data = {
                "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "brands": []
            }
            
            # If existing data exists, use it as a starting point
            if existing_data:
                all_brands_data = existing_data
            
            # Create a dictionary of existing brands by ID for easier lookup
            existing_brands_dict = {brand['id']: brand for brand in all_brands_data['brands']}
            
            # Track changes
            changes_summary = {
                'new_brands': 0,
                'updated_brands': 0,
                'failed_brands': 0,
                'new_phones': 0,
                'updated_phones': 0,
                'failed_phones': 0,
                'changes_details': []
            }
            
            # Use ThreadPoolExecutor for concurrent scraping
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Create a list of futures for each brand
                future_to_brand = {
                    executor.submit(
                        self.scrape_brand_phones, 
                        brand, 
                        max_pages_per_brand, 
                        max_products_per_brand
                    ): brand for brand in brands
                }
                
                # Process each future as it completes
                for future in as_completed(future_to_brand):
                    brand = future_to_brand[future]
                    try:
                        brand_data = future.result()
                        
                        # Check if this is a new brand or an update to an existing one
                        if brand['id'] in existing_brands_dict:
                            # Existing brand - check for updates
                            existing_brand = existing_brands_dict[brand['id']]
                            
                            # Create a dictionary of existing phones by ID
                            existing_phones_dict = {phone['id']: phone for phone in existing_brand['phones']}
                            
                            # Track brand changes
                            brand_changes = {
                                'brand_id': brand['id'],
                                'brand_name': brand['name'],
                                'new_phones': 0,
                                'updated_phones': 0,
                                'phone_changes': []
                            }
                            
                            # Process each phone in the scraped data
                            for phone in brand_data['phones']:
                                if phone['id'] in existing_phones_dict:
                                    # Existing phone - check for updates
                                    existing_phone = existing_phones_dict[phone['id']]
                                    
                                    # Compare phone data
                                    if json.dumps(existing_phone, sort_keys=True) != json.dumps(phone, sort_keys=True):
                                        brand_changes['updated_phones'] += 1
                                        changes_summary['updated_phones'] += 1
                                        
                                        # Find specific differences
                                        differences = self.find_differences(existing_phone, phone)
                                        brand_changes['phone_changes'].append({
                                            'type': 'updated',
                                            'phone_name': phone['name'],
                                            'phone_id': phone['id'],
                                            'differences': differences
                                        })
                                else:
                                    # New phone
                                    brand_changes['new_phones'] += 1
                                    changes_summary['new_phones'] += 1
                                    brand_changes['phone_changes'].append({
                                        'type': 'new',
                                        'phone_name': phone['name'],
                                        'phone_id': phone['id']
                                    })
                            
                            # Update the brand data if there were changes
                            if brand_changes['new_phones'] > 0 or brand_changes['updated_phones'] > 0:
                                changes_summary['updated_brands'] += 1
                                changes_summary['changes_details'].append(brand_changes)
                                
                                # Merge the phone data
                                existing_phones_dict.update({phone['id']: phone for phone in brand_data['phones']})
                                existing_brand['phones'] = list(existing_phones_dict.values())
                                existing_brand['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        else:
                            # New brand
                            changes_summary['new_brands'] += 1
                            changes_summary['changes_details'].append({
                                'brand_id': brand['id'],
                                'brand_name': brand['name'],
                                'type': 'new',
                                'new_phones': len(brand_data['phones']),
                                'updated_phones': 0,
                                'phone_changes': [
                                    {
                                        'type': 'new',
                                        'phone_name': phone['name'],
                                        'phone_id': phone['id']
                                    } for phone in brand_data['phones']
                                ]
                            })
                            
                            # Add the new brand to our data
                            brand_data['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            all_brands_data['brands'].append(brand_data)
                            existing_brands_dict[brand['id']] = brand_data
                        
                        # Save progress after each brand
                        self.save_progress(all_brands_data)
                        
                    except Exception as e:
                        error_msg = f"Error processing brand {brand['name']}: {str(e)}"
                        print(error_msg)
                        self.log_error(error_msg)
                        changes_summary['failed_brands'] += 1
                        continue
            
            # Print summary of changes
            print("\n=== Changes Summary ===")
            print(f"New brands: {changes_summary['new_brands']}")
            print(f"Updated brands: {changes_summary['updated_brands']}")
            print(f"Failed brands: {changes_summary['failed_brands']}")
            print(f"New phones: {changes_summary['new_phones']}")
            print(f"Updated phones: {changes_summary['updated_phones']}")
            print(f"Failed phones: {changes_summary['failed_phones']}")
            
            # Print detailed changes if any
            if changes_summary['changes_details']:
                print("\n=== Detailed Changes ===")
                for brand_change in changes_summary['changes_details']:
                    print(f"\nBrand: {brand_change['brand_name']} (ID: {brand_change['brand_id']})")
                    if 'type' in brand_change and brand_change['type'] == 'new':
                        print(f"  - New brand added with {brand_change['new_phones']} phones")
                    else:
                        print(f"  - New phones: {brand_change['new_phones']}")
                        print(f"  - Updated phones: {brand_change['updated_phones']}")
                        
                        if 'phone_changes' in brand_change:
                            for phone_change in brand_change['phone_changes']:
                                if phone_change['type'] == 'new':
                                    print(f"    * New phone: {phone_change['phone_name']}")
                                else:
                                    print(f"    * Updated phone: {phone_change['phone_name']}")
                                    for diff in phone_change['differences']:
                                        print(f"      - {diff}")
            
            # Update changelog
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            changelog_entry = {
                "timestamp": timestamp,
                "summary": {
                    "new_brands": changes_summary['new_brands'],
                    "updated_brands": changes_summary['updated_brands'],
                    "failed_brands": changes_summary['failed_brands'],
                    "new_phones": changes_summary['new_phones'],
                    "updated_phones": changes_summary['updated_phones'],
                    "failed_phones": changes_summary['failed_phones']
                },
                "details": changes_summary['changes_details']
            }
            
            # Add new entry to changelog
            changelog.append(changelog_entry)
            self.save_changelog(changelog)
            
            # Save final data
            self.save_final_data(all_brands_data)
            
            # Clean up temporary files and old logs
            self.cleanup()
            
            self.log_success("Scraping completed successfully")
            return all_brands_data
        except Exception as e:
            error_msg = f"Error in scrape_all_brands: {str(e)}"
            print(error_msg)
            self.log_error(error_msg)
            return None
    
    def save_progress(self, data):
        """Save progress to a JSON file with error handling"""
        try:
            with open(self.progress_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.log_success(f"Progress saved to {self.progress_path}")
            print(f"\nSaving changelog to: {abs_path}")
            
            # Ensure the directory exists
            os.makedirs(os.path.dirname(self.changelog_path), exist_ok=True)
            
            with open(self.changelog_path, 'w', encoding='utf-8') as f:
                # Write header
                f.write("# MobiInfo Data Changelog\n\n")
                f.write("This document tracks all changes to the MobiInfo phone database.\n\n")
                
                # Write changelog entries in reverse chronological order (latest first)
                for entry in sorted(changelog, key=lambda x: x['timestamp'], reverse=True):
                    f.write(f"## {entry['timestamp']}\n\n")
                    
                    # Write summary
                    summary = entry['summary']
                    f.write("### Summary\n\n")
                    f.write(f"- **New brands**: {summary['new_brands']}\n")
                    f.write(f"- **Updated brands**: {summary['updated_brands']}\n")
                    f.write(f"- **Failed brands**: {summary['failed_brands']}\n")
                    f.write(f"- **New phones**: {summary['new_phones']}\n")
                    f.write(f"- **Updated phones**: {summary['updated_phones']}\n")
                    f.write(f"- **Failed phones**: {summary['failed_phones']}\n\n")
                    
                    # Write detailed changes
                    if entry['details']:
                        f.write("### Detailed Changes\n\n")
                        
                        for brand_change in entry['details']:
                            f.write(f"#### {brand_change['brand_name']}\n\n")
                            
                            if 'type' in brand_change and brand_change['type'] == 'new':
                                f.write(f"- **New brand added** with {brand_change['new_phones']} phones\n\n")
                            else:
                                f.write(f"- **New phones**: {brand_change['new_phones']}\n")
                                f.write(f"- **Updated phones**: {brand_change['updated_phones']}\n\n")
                                
                                if 'phone_changes' in brand_change:
                                    for phone_change in brand_change['phone_changes']:
                                        if phone_change['type'] == 'new':
                                            f.write(f"  - **New phone**: {phone_change['phone_name']}\n")
                                        else:
                                            f.write(f"  - **Updated phone**: {phone_change['phone_name']}\n")
                                            
                                            # Format differences as a nested list
                                            if 'differences' in phone_change:
                                                for diff in phone_change['differences']:
                                                    f.write(f"    - {diff}\n")
                                            
                                            f.write("\n")
                            
                            f.write("\n")
                    
                    f.write("---\n\n")
            
            # Also save the JSON version
            json_changelog_path = os.path.join(self.output_dir, 'changelog.json')
            with open(json_changelog_path, 'w', encoding='utf-8') as f:
                json.dump(changelog, f, ensure_ascii=False, indent=2)
            
            # Check if the file was created and has content
            if os.path.exists(self.changelog_path):
                file_size = os.path.getsize(self.changelog_path)
                print(f"Changelog file created successfully with size: {file_size} bytes")
                
                # Print the first few lines to verify content
                with open(self.changelog_path, 'r', encoding='utf-8') as f:
                    first_lines = ''.join([f.readline() for _ in range(3)])
                    print(f"Changelog preview:\n{first_lines}...")
            else:
                print("ERROR: Changelog file was not created!")
            
            print(f"Changelog saved to: {abs_path}")
            self.log_success(f"Changelog saved to {abs_path}")
        except Exception as e:
            error_msg = f"Error saving changelog: {str(e)}"
            print(error_msg)
            self.log_error(error_msg)
            traceback.print_exc()  # Print full traceback for debugging
    
    def load_existing_data(self):
        """Load existing data from the final JSON file if it exists"""
        try:
            if os.path.exists(self.final_data_path):
                self.log_debug(f"Loading existing data from {self.final_data_path}")
                with open(self.final_data_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.log_success(f"Successfully loaded existing data with {len(data['brands'])} brands")
                    return data
            return None
        except Exception as e:
            error_msg = f"Error loading existing data: {str(e)}"
            print(error_msg)
            self.log_error(error_msg)
            return None
    
    def load_changelog(self):
        """Load existing changelog if it exists"""
        try:
            # For Markdown changelog, we'll maintain a JSON version internally
            json_changelog_path = os.path.join(self.output_dir, 'changelog.json')
            if os.path.exists(json_changelog_path):
                self.log_debug(f"Loading existing changelog from {json_changelog_path}")
                with open(json_changelog_path, 'r', encoding='utf-8') as f:
                    changelog = json.load(f)
                    self.log_success(f"Successfully loaded changelog with {len(changelog)} entries")
                    return changelog
            return []
        except Exception as e:
            error_msg = f"Error loading changelog: {str(e)}"
            print(error_msg)
            self.log_error(error_msg)
            return []
    
    def find_differences(self, old_phone, new_phone):
        """Find specific differences between two phone data objects"""
        differences = []
        
        try:
            # Check each field
            for key in new_phone:
                if key not in old_phone:
                    differences.append(f"Added new field: {key}")
                elif old_phone[key] != new_phone[key]:
                    if isinstance(new_phone[key], dict) and isinstance(old_phone[key], dict):
                        # Recursively compare dictionaries
                        dict_diff = self.find_dict_differences(old_phone[key], new_phone[key], key)
                        differences.extend(dict_diff)
                    elif isinstance(new_phone[key], list) and isinstance(old_phone[key], list):
                        # Compare lists
                        if len(old_phone[key]) != len(new_phone[key]):
                            differences.append(f"{key}: List length changed from {len(old_phone[key])} to {len(new_phone[key])}")
                        else:
                            for i, (old_item, new_item) in enumerate(zip(old_phone[key], new_phone[key])):
                                if old_item != new_item:
                                    differences.append(f"{key}[{i}]: Changed from '{old_item}' to '{new_item}'")
                    else:
                        differences.append(f"{key}: Changed from '{old_phone[key]}' to '{new_phone[key]}'")
        except Exception as e:
            error_msg = f"Error in find_differences: {str(e)}"
            print(error_msg)
            self.log_error(error_msg)
        
        return differences
    
    def find_dict_differences(self, old_dict, new_dict, parent_key=""):
        """Find differences between two dictionaries"""
        differences = []
        
        try:
            # Check for new or changed keys
            for key in new_dict:
                full_key = f"{parent_key}.{key}" if parent_key else key
                
                if key not in old_dict:
                    differences.append(f"Added new field: {full_key}")
                elif old_dict[key] != new_dict[key]:
                    if isinstance(new_dict[key], dict) and isinstance(old_dict[key], dict):
                        # Recursively compare nested dictionaries
                        nested_diff = self.find_dict_differences(old_dict[key], new_dict[key], full_key)
                        differences.extend(nested_diff)
                    else:
                        differences.append(f"{full_key}: Changed from '{old_dict[key]}' to '{new_dict[key]}'")
            
            # Check for removed keys
            for key in old_dict:
                if key not in new_dict:
                    full_key = f"{parent_key}.{key}" if parent_key else key
                    differences.append(f"Removed field: {full_key}")
        except Exception as e:
            error_msg = f"Error in find_dict_differences: {str(e)}"
            print(error_msg)
            self.log_error(error_msg)
        
        return differences
    
    def save_final_data(self, data):
        """Save final data to JSON file with error handling"""
        try:
            with open(self.final_data_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"\nFinal data saved to {self.final_data_path}")
            self.log_success(f"Final data saved to {self.final_data_path}")
        except Exception as e:
            error_msg = f"Error saving final data: {str(e)}"
            print(error_msg)
            self.log_error(error_msg)
    
    def save_brand_as_separate_file(self, brand_data):
        """Save a single brand as a separate JSON file in the Brands directory"""
        try:
            # Create Brands directory if it doesn't exist
            os.makedirs(self.brands_dir, exist_ok=True)
            
            # Generate filename from brand name
            brand_filename = f"{brand_data['id']}.json"
            brand_file_path = os.path.join(self.brands_dir, brand_filename)
            
            # Create brand file data structure
            brand_file_data = {
                "brand_info": {
                    "id": brand_data['id'],
                    "name": brand_data['name'],
                    "url": brand_data['url'],
                    "image_url": brand_data['image_url'],
                    "last_updated": brand_data.get('last_updated', datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                    "total_phones": len(brand_data['phones'])
                },
                "phones": brand_data['phones']
            }
            
            # Save to separate file
            with open(brand_file_path, 'w', encoding='utf-8') as f:
                json.dump(brand_file_data, f, ensure_ascii=False, indent=2)
            
            print(f"Brand '{brand_data['name']}' saved to {brand_file_path}")
            self.log_success(f"Brand '{brand_data['name']}' saved to {brand_file_path}")
            return brand_file_path
        except Exception as e:
            error_msg = f"Error saving brand '{brand_data['name']}' as separate file: {str(e)}"
            print(error_msg)
            self.log_error(error_msg)
            return None
    
    def save_brands_as_separate_files(self, brands_data):
        """Save multiple brands as separate JSON files"""
        try:
            saved_files = []
            total_brands = len(brands_data)
            
            print(f"\n=== Saving {total_brands} brands as separate JSON files ===")
            
            for i, brand_data in enumerate(brands_data, 1):
                print(f"Saving brand {i}/{total_brands}: {brand_data['name']}")
                file_path = self.save_brand_as_separate_file(brand_data)
                if file_path:
                    saved_files.append(file_path)
            
            print(f"\n=== Successfully saved {len(saved_files)}/{total_brands} brands as separate files ===")
            return saved_files
        except Exception as e:
            error_msg = f"Error saving brands as separate files: {str(e)}"
            print(error_msg)
            self.log_error(error_msg)
            return []
    
    def scrape_multiple_brands_separate_files(self, brand_inputs, max_brands=None, max_pages=None, max_products=None, max_workers=2):
        """Scrape multiple brands and save each as separate JSON files with consolidated changelog
        
        Args:
            brand_inputs (list): List of brand names or URLs
            max_brands (int, optional): Maximum number of brands to scrape. If None, scrape all brands.
            max_pages (int, optional): Maximum number of pages to scrape per brand
            max_products (int, optional): Maximum number of products to scrape per brand
            max_workers (int, optional): Maximum number of concurrent workers. Default is 2.
            
        Returns:
            dict: Contains 'saved_files' list and 'results' list
        """
        if isinstance(brand_inputs, str):
            # If a single string is passed, convert to list
            brand_inputs = [brand_inputs]
        
        # Apply max_brands limit if specified
        if max_brands is not None and max_brands > 0:
            brand_inputs = brand_inputs[:max_brands]
            print(f"Limited to first {max_brands} brands from the input list")
        
        results = []
        saved_files = []
        total_brands = len(brand_inputs)
        
        # Store the number of workers for adaptive delay calculation
        self.max_workers = max_workers
        
        # Initialize consolidated changes summary
        consolidated_changes = {
            'new_brands': 0,
            'updated_brands': 0,
            'failed_brands': 0,
            'new_phones': 0,
            'updated_phones': 0,
            'failed_phones': 0,
            'changes_details': []
        }
        
        print(f"\n=== Starting to scrape {total_brands} brands and save as separate files ===")
        print(f"=== Using {max_workers} concurrent workers ===")
        
        # Process brands with concurrent workers if max_workers > 1
        if max_workers > 1:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            import threading
            
            # Thread-safe collections
            results_lock = threading.Lock()
            
            def process_brand_with_lock(brand_input, brand_index):
                """Process a single brand with thread safety"""
                try:
                    print(f"\n--- Processing brand {brand_index}/{total_brands}: {brand_input} ---")
                    result = self.scrape_single_brand_without_changelog(brand_input, max_pages, max_products, separate_files_mode=True)
                    
                    if result:
                        brand_data = result['brand_data']
                        
                        # Save as separate file
                        file_path = self.save_brand_as_separate_file(brand_data)
                        
                        # Thread-safe updates
                        with results_lock:
                            results.append(brand_data)
                            if file_path:
                                saved_files.append(file_path)
                            
                            # Aggregate the changes
                            changes = result['changes_summary']
                            consolidated_changes['new_brands'] += changes['new_brands']
                            consolidated_changes['updated_brands'] += changes['updated_brands']
                            consolidated_changes['failed_brands'] += changes['failed_brands']
                            consolidated_changes['new_phones'] += changes['new_phones']
                            consolidated_changes['updated_phones'] += changes['updated_phones']
                            consolidated_changes['failed_phones'] += changes['failed_phones']
                            consolidated_changes['changes_details'].extend(changes['changes_details'])
                        
                        return True
                    else:
                        print(f"Failed to scrape brand: {brand_input}")
                        with results_lock:
                            consolidated_changes['failed_brands'] += 1
                        return False
                except Exception as e:
                    error_msg = f"Error processing brand {brand_input}: {str(e)}"
                    print(error_msg)
                    self.log_error(error_msg)
                    with results_lock:
                        consolidated_changes['failed_brands'] += 1
                    return False
            
            # Use ThreadPoolExecutor for concurrent processing
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks
                future_to_brand = {
                    executor.submit(process_brand_with_lock, brand_input, i): (brand_input, i)
                    for i, brand_input in enumerate(brand_inputs, 1)
                }
                
                # Wait for completion
                for future in as_completed(future_to_brand):
                    brand_input, brand_index = future_to_brand[future]
                    try:
                        success = future.result()
                        if success:
                            print(f"✓ Completed brand {brand_index}/{total_brands}: {brand_input}")
                        else:
                            print(f"✗ Failed brand {brand_index}/{total_brands}: {brand_input}")
                    except Exception as e:
                        print(f"✗ Exception for brand {brand_index}/{total_brands}: {brand_input} - {str(e)}")
        else:
            # Sequential processing (original behavior)
            for i, brand_input in enumerate(brand_inputs, 1):
                print(f"\n--- Processing brand {i}/{total_brands}: {brand_input} ---")
                result = self.scrape_single_brand_without_changelog(brand_input, max_pages, max_products, separate_files_mode=True)
                if result:
                    brand_data = result['brand_data']
                    results.append(brand_data)
                    
                    # Save as separate file
                    file_path = self.save_brand_as_separate_file(brand_data)
                    if file_path:
                        saved_files.append(file_path)
                    
                    # Aggregate the changes
                    changes = result['changes_summary']
                    consolidated_changes['new_brands'] += changes['new_brands']
                    consolidated_changes['updated_brands'] += changes['updated_brands']
                    consolidated_changes['failed_brands'] += changes['failed_brands']
                    consolidated_changes['new_phones'] += changes['new_phones']
                    consolidated_changes['updated_phones'] += changes['updated_phones']
                    consolidated_changes['failed_phones'] += changes['failed_phones']
                    consolidated_changes['changes_details'].extend(changes['changes_details'])
                else:
                    print(f"Failed to scrape brand: {brand_input}")
                    consolidated_changes['failed_brands'] += 1
        
        # Create a single consolidated changelog entry
        if consolidated_changes['changes_details']:
            print(f"\n=== Creating consolidated changelog for {len(results)} brands ===")
            self.update_and_save_changelog(consolidated_changes)
        
        print(f"\n=== Completed scraping {len(results)}/{total_brands} brands successfully ===")
        print(f"=== Saved {len(saved_files)} separate brand files ===")
        
        return {
            'results': results,
            'saved_files': saved_files,
            'changes_summary': consolidated_changes
        }
    
    def cleanup(self):
        """Remove temporary files and clean up old logs"""
        try:
            # Clean up old logs
            self.cleanup_old_logs()
            
            # Remove progress file
            if os.path.exists(self.progress_path):
                os.remove(self.progress_path)
                print(f"\nRemoved progress file: {self.progress_path}")
                self.log_success(f"Removed progress file: {self.progress_path}")
        except Exception as e:
            error_msg = f"Error during cleanup: {str(e)}"
            print(error_msg)
            self.log_error(error_msg)

# Run the scraper
if __name__ == "__main__":
    scraper = MobiInfoScraper()
    
    # Choose scraping mode:
    
    # Mode 1: Scrape a single brand by name or URL
    # scraper.scrape_single_brand("bengal")  # By brand name
    # scraper.scrape_single_brand("https://www.mobiledokan.com/mobile-brand/apple")  # By brand URL
    
    # Mode 2: Scrape multiple specific brands by names or URLs (saves to single allbrands.json)
    # brand_list = ["mycell", "oscal", "tcl", "geo", "thuraya", "sonim", "proton", "sharp"]  # List of brand names
    # scraper.scrape_multiple_brands(
    #     brand_inputs=brand_list,
    #     # max_pages=2,  # Limit to 2 pages per brand
    #     # max_products=10  # Limit to 10 products per brand
    # )
    
    # Mode 2B: Scrape multiple brands and save each as separate JSON files (NEW!)

#     brand_list = [
#     "xiaomi", "realme", "apple", "vivo", "samsung", "infinix", "nokia", "oppo", 
#     "tecno", "oneplus", "google", "walton", "honor", "lava", "itel", "symphony", 
#     "huawei", "nothing", "asus", "helio", "benco", "motorola", "iqoo", "sony", 
#     "meizu", "maximus", "lg", "zte", "htc", "coolpad", "umidigi", "kyocera", 
#     "cat", "blu", "blackview", "leitz", "nio", "microsoft", "micromax", "gionee", 
#     "lenovo", "cubot", "alcatel", "fairphone", "we", "freeyond", "hmd", "blackberry", 
#     "allview", "panasonic", "5star", "maxis", "celkon", "xtra", "hallo", "doogee", 
#     "ulefone", "leica", "acer", "gdl", "proton", "sonim", "thuraya", "sharp", 
#     "geo", "tcl", "oukitel", "oscal", "bengal", "mycell", "wiko", "kingstar", 
#     "energizer", "philips", "okapia"
# ]
    # brand_list = ["mycell", "oscal", "tcl", "geo", "thuraya", "sonim", "proton", "sharp"]  # List of brand names
    # brand_list = ["okapia", "philips", "energizer", "kingster", "wiko", "bengal", "okutel"]  # List of brand names
    # brand_list = ["kingstar", "wiko"]  # List of brand names
    brand_list = ["geo", "tcl"]  # List of brand names
    result = scraper.scrape_multiple_brands_separate_files(
        brand_inputs=brand_list,
        # max_brands=5,  # Limit to first 5 brands from the list
        # max_pages=2,  # Limit to 2 pages per brand
        # max_products=10,  # Limit to 10 products per brand
        max_workers=5  # Use 3 concurrent workers for faster processing
    )
    # print(f"Saved {len(result['saved_files'])} separate brand files:")
    # for file_path in result['saved_files']:
    #     print(f"  - {file_path}")
    
    # Alternative: Mix brand names and URLs
    # mixed_brands = [
    #     "bengal",
    #     "okapia", 
    #     "https://www.mobiledokan.com/mobile-brand/apple",
    #     "https://www.mobiledokan.com/mobile-brand/samsung"
    # ]
    # scraper.scrape_multiple_brands(mixed_brands)
    
    # Mode 3: Scrape all available brands (existing functionality)
    # scraper.scrape_all_brands(
    #     max_brands=5,  # Limit to 5 brands for testing
    #     max_pages_per_brand=2,  # Limit to 2 pages per brand
    #     max_products_per_brand=10,  # Limit to 10 products per brand
    #     max_workers=2  # Number of concurrent workers
    # )
    
    # To scrape all brands without limits:
    # scraper.scrape_all_brands()