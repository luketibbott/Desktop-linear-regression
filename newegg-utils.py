# This is a utility file to support the scraping of Newegg
import pandas as pd
import numpy as np
import requests
import bs4
import re
from collections import OrderedDict
import time
import random

def get_soup(url):
    r = requests.get(url)
    soup = bs4.BeautifulSoup(r.text, 'lxml')
    return soup

