import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
import urllib.parse
from requests.exceptions import RequestException
import argparse
from concurrent.futures import ThreadPoolExecutor
import re

class WebScraper:
    def __init__(self, user_agent=None, timeout=30, max_retries=3, delay=1):
        """Initialize the web scraper with configurable parameters."""
        self.session = requests.Session()
        self.timeout = timeout
        self.max_retries = max_retries
        self.delay = delay
        
        # Use a realistic user agent if none provided
        default_ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        self.session.headers.update({'User-Agent': user_agent or default_ua})
    
    def fetch_html(self, url):
        """Fetch HTML content from a URL with retry logic."""
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, timeout=self.timeout)
                
                # Handle redirect
                if response.history:
                    print(f"Redirected from {url} to {response.url}")
                
                if response.status_code == 200:
                    return response.text, response.url
                elif response.status_code == 404:
                    print(f"Page not found (404): {url}")
                    return None, url
                elif 500 <= response.status_code < 600:
                    print(f"Server error ({response.status_code}). Retrying {url}...")
                    time.sleep(self.delay * (attempt + 1))  # Exponential backoff
                    continue
                else:
                    print(f"Failed to fetch page. Status Code: {response.status_code}")
                    return None, url
            
            except RequestException as e:
                print(f"Request error on attempt {attempt+1}/{self.max_retries}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.delay * (attempt + 1))
                    continue
                return None, url
        
        return None, url
    
    def get_element_xpath(self, element, soup):
        """Generate XPath for a given element."""
        components = []
        current = element
        
        # Traverse up to root, building XPath components
        while current and current != soup:
            # Get index among siblings of same type
            siblings = current.find_previous_siblings(current.name) if current.name else []
            index = len(siblings) + 1  # XPath indices start at 1
            
            # Build the XPath component for this element
            if current.name:
                if current.get('id'):
                    # If element has ID, use it (it's unique and shorter)
                    components.insert(0, f"//{current.name}[@id='{current.get('id')}']")
                    # Once we hit an ID, we can stop - it's a unique identifier
                    break
                else:
                    # Otherwise use the tag name and position
                    xpath_part = f"/{current.name}"
                    if index > 1 or current.find_next_siblings(current.name):
                        xpath_part += f"[{index}]"
                    components.insert(0, xpath_part)
            
            # Move up to parent
            current = current.parent
        
        # If we ended up with something starting with //, return as is
        if components and components[0].startswith('//'):
            return components[0]
        
        # Otherwise, join all components
        return ''.join(components) if components else '/'
    
    def parse_element(self, element, url, soup, parent=None, level=0, path=None):
        """Recursively parse an HTML element and return rows for a DataFrame with enhanced information."""
        if element.name is None:
            return []
        
        # Create path to current element
        current_path = path or []
        if element.name:
            path_component = element.name
            if 'id' in element.attrs:
                path_component += f"#{element.attrs['id']}"
            elif 'class' in element.attrs:
                path_component += f".{'.'.join(element.attrs['class'])}"
            current_path = current_path + [path_component]
        
        css_path = " > ".join(current_path)
        xpath = self.get_element_xpath(element, soup)
        
        # Extract specific attributes for different tag types
        special_attrs = {}
        
        # Handle images - extract src, alt, dimensions
        if element.name == 'img':
            special_attrs['src'] = element.get('src', '')
            special_attrs['alt'] = element.get('alt', '')
            special_attrs['width'] = element.get('width', '')
            special_attrs['height'] = element.get('height', '')
            
            # Convert relative URLs to absolute
            if special_attrs['src'] and not special_attrs['src'].startswith(('http://', 'https://', 'data:')):
                special_attrs['src'] = urllib.parse.urljoin(url, special_attrs['src'])
        
        # Handle links - extract href, rel, target
        elif element.name == 'a':
            special_attrs['href'] = element.get('href', '')
            special_attrs['rel'] = element.get('rel', '')
            special_attrs['target'] = element.get('target', '')
            
            # Convert relative URLs to absolute
            if special_attrs['href'] and not special_attrs['href'].startswith(('http://', 'https://', 'mailto:', 'tel:', '#', 'javascript:')):
                special_attrs['href'] = urllib.parse.urljoin(url, special_attrs['href'])
        
        # Handle videos
        elif element.name in ('video', 'iframe'):
            special_attrs['src'] = element.get('src', '')
            special_attrs['width'] = element.get('width', '')
            special_attrs['height'] = element.get('height', '')
        
        # Handle forms
        elif element.name == 'form':
            special_attrs['action'] = element.get('action', '')
            special_attrs['method'] = element.get('method', '')
        
        # Handle input fields
        elif element.name == 'input':
            special_attrs['type'] = element.get('type', '')
            special_attrs['name'] = element.get('name', '')
            special_attrs['value'] = element.get('value', '')
            special_attrs['placeholder'] = element.get('placeholder', '')
        
        # Extract text content, clean and truncate if too long
        text_content = element.get_text(strip=True)
        if len(text_content) > 1000:  # Truncate very long text
            text_content = text_content[:997] + "..."
        
        # Get HTML content
        html_content = str(element)
        if len(html_content) > 1000:  # Truncate very long HTML
            html_content = html_content[:997] + "..."
        
        # Count child elements
        child_count = len([child for child in element.find_all(recursive=False) if child.name])
        
        row = {
            "Tag": element.name,
            "Attributes": str(element.attrs),
            "Special Attributes": str(special_attrs) if special_attrs else "",
            "Text Content": text_content,
            "Parent Tag": parent,
            "Level": level,
            "XPath": xpath,
            "CSS Path": css_path,
            "Child Count": child_count,
            "Has Class": 'class' in element.attrs,
            "Has ID": 'id' in element.attrs,
            "Class": ' '.join(element.get('class', [])) if isinstance(element.get('class'), list) else element.get('class', ''),
            "ID": element.get('id', '')
        }
        
        rows = [row]
        for child in element.children:
            if child.name:  # Only process elements (skip raw text nodes)
                rows.extend(self.parse_element(child, url, soup, parent=element.name, level=level+1, path=current_path))
        
        return rows
    
    def filter_elements(self, data, tag_filter=None, min_level=None, max_level=None, has_text=False, has_class=False, has_id=False):
        """Filter the collected data based on various criteria."""
        filtered_data = data.copy()
        
        if tag_filter:
            filtered_data = filtered_data[filtered_data['Tag'].isin(tag_filter)]
        
        if min_level is not None:
            filtered_data = filtered_data[filtered_data['Level'] >= min_level]
            
        if max_level is not None:
            filtered_data = filtered_data[filtered_data['Level'] <= max_level]
            
        if has_text:
            filtered_data = filtered_data[filtered_data['Text Content'] != ""]
            
        if has_class:
            filtered_data = filtered_data[filtered_data['Has Class'] == True]
            
        if has_id:
            filtered_data = filtered_data[filtered_data['Has ID'] == True]
            
        return filtered_data
    
    def extract_images(self, data):
        """Extract only image data for a focused analysis."""
        return self.filter_elements(data, tag_filter=['img'])
        
    def extract_links(self, data):
        """Extract only link data for a focused analysis."""
        return self.filter_elements(data, tag_filter=['a'])
    
    def html_to_excel(self, url, output_file="output.xlsx", tag_filter=None, extract_images=False, extract_links=False):
        """Process HTML and save to Excel with optional filters and separate sheets for images and links."""
        html, final_url = self.fetch_html(url)
        if html:
            soup = BeautifulSoup(html, 'html.parser')
            data = self.parse_element(soup, final_url, soup)
            
            # Create DataFrame
            df = pd.DataFrame(data)
            
            # Create Excel writer
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Apply filters if specified
                if tag_filter or extract_images or extract_links:
                    # Main filtered data
                    filtered_df = self.filter_elements(df, tag_filter=tag_filter)
                    filtered_df.to_excel(writer, sheet_name="Filtered Data", index=False)
                    
                    # Images data if requested
                    if extract_images:
                        images_df = self.extract_images(df)
                        if not images_df.empty:
                            images_df.to_excel(writer, sheet_name="Images", index=False)
                    
                    # Links data if requested
                    if extract_links:
                        links_df = self.extract_links(df)
                        if not links_df.empty:
                            links_df.to_excel(writer, sheet_name="Links", index=False)
                    
                    # All data
                    df.to_excel(writer, sheet_name="All Data", index=False)
                else:
                    # Just save all data
                    df.to_excel(writer, sheet_name="All Data", index=False)
            
            print(f"Excel file saved: {output_file}")
            return True
        else:
            print("Could not fetch HTML from the URL.")
            return False


def main():
    parser = argparse.ArgumentParser(description='Scrape webpage data into Excel with advanced options')
    parser.add_argument('url', help='URL to scrape')
    parser.add_argument('-o', '--output', help='Output Excel file', default='webpage_data.xlsx')
    parser.add_argument('-t', '--tags', help='Filter by tags (comma-separated)', default=None)
    parser.add_argument('-i', '--images', action='store_true', help='Extract images to separate sheet')
    parser.add_argument('-l', '--links', action='store_true', help='Extract links to separate sheet')
    parser.add_argument('-u', '--user-agent', help='Custom User-Agent', default=None)
    parser.add_argument('-r', '--retries', type=int, help='Max retries', default=3)
    parser.add_argument('-d', '--delay', type=float, help='Delay between retries in seconds', default=1.0)
    
    args = parser.parse_args()
    
    # Process tag filter if provided
    tag_filter = args.tags.split(',') if args.tags else None
    
    # Initialize and run scraper
    scraper = WebScraper(user_agent=args.user_agent, max_retries=args.retries, delay=args.delay)
    scraper.html_to_excel(
        args.url, 
        output_file=args.output,
        tag_filter=tag_filter,
        extract_images=args.images,
        extract_links=args.links
    )

if __name__ == "__main__":
    main()
