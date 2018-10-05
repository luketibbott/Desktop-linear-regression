# This is a utility file to support the scraping of Newegg
import pandas as pd
import numpy as np
import requests
import bs4
import re
from collections import OrderedDict
import time
import random

def get_product_html(url, soup=False):
    # Returns raw HTML of passed url
    r = requests.get(url)
    if soup == True:
        return bs4.BeautifulSoup(r.text, 'lxml')
    else:
        return r.text

def get_components(html):
    '''Grabs all available PC components from Newegg webpage. 
       ====Parameters====
       soup: BeautifulSoup object (use get_soup() function)
       ====Returns====
       this_computer: OrderedDict mapping this computer's variables to values
    '''
    soup = BeautifulSoup(html, 'lxml')
    # Narrow down to specifications part of page
    specs = soup.find('div', class_='plinks')
    # All categories of technical specifications in dt tags
    categories = specs.find_all('dt')
    # All details of technical specifications in dd tags
    details = specs.find_all('dd')
    
    this_computer = OrderedDict()
    
    for cat, det in zip(categories, details):
        # raw detail is always located here
        raw_det = det.contents[0]
        
        # in some cases, have to go one tag deeper to get category name
        if type(cat.contents[0]) == bs4.element.Tag:
            raw_cat = cat.contents[0].contents[0]
        else:
            raw_cat = cat.contents[0]
            
        this_computer[raw_cat] = raw_det
    return this_computer

def get_prices(html):
    # Grabs all 96 prices from the product array page
    soup = BeautifulSoup(html, 'lxml')
    price_list = []
    
    price_spans = soup.find_all('span', class_='price-current-label')
    for span in price_spans:
        price_list.append(span.findNextSibling().contents[0])
    return price_list

def get_links(product_array):    
    # Returns a list of 96 product links on a product list page
    prod_links = []

    for prod in product_array:
        prod_links.append(prod.find('a', href=True)['href'])
        
    return prod_links

def one_page_scrape(url):
    '''
       Scrapes one product array page as well as each product on that page. 96 products plus
       one set of prices for each of these products is returned in the HTML of dataframes.
       ====Parameters====
       url: url of product array page to be scraped
    '''
    prices_df = pd.DataFrame(columns=['price_html'])
    products_df = pd.DataFrame(columns=['component_html'])
    
    r = requests.get(url)
    
    array_page_html = pd.DataFrame(data = [r.text], columns=['price_html'])
    prices_df = prices_df.append(array_page_html)
    
    soup = BeautifulSoup(r.text, 'lxml')
    
    # Grab array holding all 96 products
    product_array = soup.find_all('div', class_='item-container')
    # Get links for each of those products
    prod_links = get_links(product_array)
    
    # Scrape the html of each of those products
    counter = 0
    for prod_url in prod_links:
        counter += 1
        print(f'About to scrape page {counter}')
        time.sleep(5 + 5*random.random())
        html = get_product_html(prod_url)
        current_page_html = pd.DataFrame(data = [html], columns=['component_html'])
        products_df = products_df.append(current_page_html)
        
    return prices_df, products_df

def scrape_all(url_list, master_prices, master_components, up_to):
    # Scrapes all pages in url_list
    for url in url_list[:up_to]:
        products, prices = one_page_scrape(url)
        
        master_prices.append(prices)
        master_components.append(products)
        
        master_prices.to_csv('master_price_html.csv')
        master_components.to_csv('master_component_html.csv')
        
    return master_prices, master_components

# ============Cleaning functions===========

def processor_brand(processor):
    # Return the first word in the processor description (usually the brand)
    return processor.split(' ')[0]

def ram_cap(memory):
    # Returns RAM capacity
    return memory.upper().split('GB')[0]

def ram_type(memory):
    # Returns type of RAM (DDR2, DDR3, or DDR4)
    memory = memory.upper()
    if 'DDR2' in memory:
        return 'ddr2'
    if 'DDR3' in memory:
        return 'ddr3'
    # DDR4 is industry standard these days, we default to it
    else:
        return 'ddr4'

def disk_cap(storage):
    # Returns the disk capacity of a hard disk (will also grab first letter of storage unit GB or TB)
    storage = storage.upper()
    stor_split = storage.split('B')
    return stor_split[0]

def ssd_or_hdd(storage):
    # Check contents of storage for keywords indicating disk type
    storage = storage.upper()
    # Some systems have both SSD and HDD in 
    if ('+' in storage) or ('plus' in storage):
        return 'both'
    elif 'SSD' in storage:
        return 'ssd'
    elif 'RPM' in storage or 'HDD' in storage:
        return 'hdd'
    else:
        return 'hdd'
