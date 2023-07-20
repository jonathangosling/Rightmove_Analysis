from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC  
import time
import credentials
import requests
import pandas as pd

def accept_cookies_if_asked(driver):
    try:
        driver.find_element(By.ID, "onetrust-accept-btn-handler").click()
    except:
        pass

class Data:
    def __init__(self):
        self.ids = []
        self.prices = []
        self.address = []
        self.postcodes = []
        self.latitudes = []
        self.longitudes = []

    def add_page_data(self, driver):
        properties = driver.find_elements(By.CLASS_NAME, "l-searchResult")
        for property in properties:
            self.ids.append(property.find_element(By.CLASS_NAME, "propertyCard-anchor").get_attribute('id'))
            self.prices.append(property.find_element(By.CLASS_NAME, "propertyCard-priceValue").text)
            self.address.append(property.find_element(By.CLASS_NAME, "propertyCard-address").get_attribute('title'))

    def transform_location_data_google(self, logger):
        self.postcodes = []
        self.latitudes = []
        self.longitudes = []
        logger.info("Accessing the google maps api and getting location data ...")
        for addy in self.address:
            addy = addy + ", London, UK"
            addy = addy.replace(' ', '%20')
            r = requests.get(rf'https://maps.googleapis.com/maps/api/geocode/json?address={addy}&key={credentials.googlemaps_api_key}')
            if r.status_code >= 300:
                logger.error(f"Unable to access google maps api. Status code {r.status_code}.")
                raise Exception(r.reason)
            result = r.json()['results']
            postcode_obtained = False
            for comp in result[0]['address_components']:
                if "postal_code" in comp['types'][0]:
                    self.postcodes.append(comp["long_name"])
                    postcode_obtained = True
            if not postcode_obtained:
                self.postcodes.append(None)
            self.latitudes.append(result[0]['geometry']['location']['lat'])
            self.longitudes.append(result[0]['geometry']['location']['lng'])

    def transform_location_data_bing(self, logger):
        self.postcodes = []
        self.latitudes = []
        self.longitudes = []
        logger.info("Accessing the bing maps api and getting location data ...")
        for addy in self.address:
            addy = addy + ", London, UK"
            addy = addy.replace(' ', '%20')
            r = requests.get(rf'https://dev.virtualearth.net/REST/v1/Locations?q={addy}&key={credentials.bingmaps_api_key}')
            if r.status_code >= 300:
                logger.error(f"Unable to access bing maps api. Status code {r.status_code}.")
                raise Exception(r.reason)
            try:
                self.postcodes.append(r.json()['resourceSets'][0]['resources'][0]['address']['postalCode'])
            except KeyError:
                self.postcodes.append(None)
            self.latitudes.append(r.json()['resourceSets'][0]['resources'][0]['geocodePoints'][0]['coordinates'][0])
            self.longitudes.append(r.json()['resourceSets'][0]['resources'][0]['geocodePoints'][0]['coordinates'][1])


    def transform_prices(self):
        self.prices = [int(price[1:-4].replace(',','')) for price in self.prices]

    def make_df(self):
        return pd.DataFrame({'id': self.ids, 'price': self.prices, 'address': self.address, 
                             'postcode': self.postcodes, 'latitude': self.latitudes, 'longitude': self.longitudes})

    def zip_property(self):
        ''''This method zips up all of the attributes needed to 
        insert into the property table in the database'''
        return list(zip([int(id.replace('prop','')) for id in self.ids], 
                        self.address, 
                        self.postcodes, 
                        [round(lat, 8) for lat in self.latitudes], 
                        [round(lon, 8) for lon in self.longitudes]))
    
    def zip_price(self, date_today):
        ''''This method zips up all of the attributes needed to 
        insert into the price table in the database'''
        return [(int(id.replace('prop','')), str(date_today), self.prices[i]) for i, id in enumerate(self.ids)]
    
def scrape(url, logger):
    driver = webdriver.Chrome()

    logger.info("Going to url using Chrome Driver ...")
    driver.get(url)
    logger.info("Website accessed.")

    time.sleep(2)
    accept_cookies_if_asked(driver)

    data = Data()
    data.add_page_data(driver)
    pages_scanned = 1

    button = driver.find_element(By.XPATH, "//button[@title='Next page']")

    logger.info("Looping through pages ...")
    while button.is_enabled():
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable(button))
        button.click()
        accept_cookies_if_asked(driver)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, 'propertyCard-anchor')))
        data.add_page_data(driver)
        pages_scanned += 1
        button = driver.find_element(By.XPATH, "//button[@title='Next page']")

    driver.close()
    logger.info(f"Scraping complete. {pages_scanned} pages scraped.")
    return data

def transform(data, logger):

    logger.info("Transforming prices ...")
    data.transform_prices()
    logger.info("Transforming prices complete. Transforming locations ...")
    data.transform_location_data_google(logger)
    logger.info("Transforming location complete.")
            