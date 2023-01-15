# -*- coding: utf-8 -*-

'''
    implementation of scraper for scraping hotel prices, coordinates
    off makemytrip website for rendering onto QGIS platform
'''

__author__ = 'arka'

__license__ = "MIT"
__version__ = "1.1.0"
__maintainer__ = "Arkaprava Ghosh"
__email__ = "arkaprava.mail@gmail.com"
__status__ = "Development"

import selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver import DesiredCapabilities

from seleniumwire.webdriver import Firefox
from seleniumwire.webdriver import FirefoxOptions
from seleniumwire.utils import decode

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

import os
import requests
import socket
import json
from time import sleep
from urllib.parse import urlencode
from datetime import datetime, timedelta

import pandas as pd

from db import DBManager

import logging

logging.basicConfig(
    filename=os.path.join(os.path.dirname(__file__), ".log"),
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a',
    level=logging.INFO
)

selenium_logger = logging.getLogger('seleniumwire')
selenium_logger.setLevel(logging.ERROR)

DRIVER_VERSION = 109


BROWSER = 'firefox'

if BROWSER == 'chrome':
    op = webdriver.ChromeOptions()
    # TODO: uncomment in prod
    # op.add_argument('--headless')
    # op.add_argument('--window-size=1920,1080')
    # op.add_argument('--user-data-dir=C:\\Users\\arka\\AppData\\Local\\Google\\Chrome\\User Data')
    # op.add_argument("--profile-directory=Default")
    op.add_argument('--disable-web-security')
    op.add_argument('--ignore-certificate-errors-spki-list')
    op.add_argument('--ignore-ssl-errors')
    op.add_argument('--log-level=3')
    op.add_experimental_option("excludeSwitches", ["enable-logging"])
    # op.add_argument('--disable-gpu')
    # op.add_argument('--no-sandbox')
    op.add_argument("--disable-extensions")
    op.add_experimental_option("useAutomationExtension", False)
    # op.add_argument("--proxy-server='direct://'")
    # op.add_argument("--proxy-bypass-list=*")
    op.add_argument("--start-maximized")

    capabilities = DesiredCapabilities.CHROME
    capabilities["goog:loggingPrefs"] = {"performance": "ALL"}  # newer: goog:loggingPrefs
elif BROWSER == 'firefox':
    op = FirefoxOptions()
    op.add_argument('--headless')
    # op.add_experimental_option("excludeSwitches", ["enable-logging"])
    pass


class MMTHotelsAPI:
    __BASE_URL = 'https://www.makemytrip.com/hotels/hotel-listing/'
    __AUTOSUGGEST_URL = 'https://mapi.makemytrip.com/autosuggest/v5/search'
    __INFO_URL = "https://mapi.makemytrip.com/clientbackend/cg/search-hotels/DESKTOP/2"
    __HOTEL_URL = "https://www.makemytrip.com/hotels/hotel-details/"
    __REVIEW_URL = "https://mapi.makemytrip.com/clientbackend/entity/api/hotel/{hotel_id}/flyfishReviews"

    __LOAD_PAUSE_TIME = 10
    __SCROLL_PAUSE_TIME = 5
    __SCROLL_LIMIT = 2
    __REVIEW_LOAD_TIME=5
    __CLICKABLE_WAIT_TIME = 2

    __REVIEW_MAX_PAGES = 2

    def __init__(self, city, dbName, tableName) -> None:
        self.city = city
        self.dbName = dbName
        self.tableName = tableName

        # select best version for selenium webdriver
        self.selenium_version = self.__get_selenium_version()
        self.driver_version = DRIVER_VERSION

        if BROWSER == 'chrome':
            try:
                if self.selenium_version == 3:
                    self.driver = webdriver.Chrome(
                        desired_capabilities=capabilities, 
                        executable_path=os.path.join(os.path.dirname(__file__), 'exe', f"chromedriver_v{self.driver_version}_mod.exe"), 
                        options=op
                    )
                elif self.selenium_version == 4:
                    chromeService = Service(os.path.join(os.path.dirname(__file__), 'exe', f"chromedriver_v{self.driver_version}_mod.exe"))
                    self.driver = webdriver.Chrome(desired_capabilities=capabilities, service=chromeService, options=op)
                browser_version = self.__get_browser_version()

                if browser_version > self.driver_version:
                    self.driver.close()
                    self.driver.quit()

                    self.driver_version = browser_version
                    # op.arguments.remove('--headless')
                    
                    if self.selenium_version == 3:
                        self.driver = webdriver.Chrome(
                            desired_capabilities=capabilities, 
                            executable_path=os.path.join(os.path.dirname(__file__), 'exe', f"chromedriver_v{self.driver_version}_mod.exe"), 
                            options=op
                        )
                    elif self.selenium_version == 4:
                        chromeService = Service(os.path.join(os.path.dirname(__file__), 'exe', f"chromedriver_v{self.driver_version}_mod.exe"))
                        self.driver = webdriver.Chrome(desired_capabilities=capabilities, service=chromeService, options=op)
            except:
                self.__cleanup__()
                logging.error("error loading chromedriver", exc_info=True)
            else:
                logging.info(f"loaded chromedriver version {self.driver_version}")
        else:
            try:
                self.driver = Firefox(
                    firefox_binary="C:\\Program Files\Mozilla Firefox\\firefox.exe", 
                    executable_path=os.path.join(os.path.dirname(__file__), 'exe', f"geckodriver.exe"),
                    options=op
                )
            except:
                self.__cleanup__()
                logging.error("error loading geckodriver", exc_info=True)
            else:
                logging.info(f"loaded geckodriver")

        try:
            self.dbm = DBManager(self.dbName, self.tableName, logging=logging)
        except:
            logging.error("mongodb error", exc_info=True)

        # Step 1: get city code from city name
        self.city_code = self.__get_city_code(self.city)

        # Step 2: form url and start selenium session
        self.driver.get(self.__form_url(self.city_code))

        self.driver.delete_all_cookies()

        # Step 3: scroll and scrape data from network requests
        self.__get_hotel_data()

        input("CODE FINISHED EXECUTING...")


    def __get_network_response(self, log_raw):
        msg = json.loads(log_raw["message"])["message"]
        if 'requestId' in msg['params']:
            try:
                return self.driver.execute_cdp_cmd('Network.getResponseBody', {'requestId': msg["params"]["requestId"]})
            except:
                return None
        else:
            return None

    def __get_hotel_data(self):
        sleep(self.__LOAD_PAUSE_TIME)

        self.__scroll_to_end(self.__SCROLL_LIMIT)

        responses = []

        if BROWSER == 'chrome':
            logs_raw = self.driver.get_log("performance")

            for log_raw in logs_raw:
                msg = json.loads(log_raw["message"])["message"]
                if 'requestId' in msg['params'] and 'request' in msg['params']:
                    request = msg['params']['request']
                    if "method" in request and "url" in request:
                        # print(request['url'], request["method"])
                        if request['url'].startswith(self.__INFO_URL) and request["method"] == "POST":
                            try:
                                response = self.driver.execute_cdp_cmd('Network.getResponseBody', {'requestId': msg["params"]["requestId"]})
                                print(response)
                                responses.append(response) 
                            except:
                                pass
        elif BROWSER == 'firefox':
            # responses = self.driver.execute_script(
            #     "var performance = window.performance || window.mozPerformance || window.msPerformance || window.webkitPerformance || {}; \
            #     var network = performance.getEntries() || {}; \
            #     return network;"
            # )
            for request in self.driver.requests:
                if request.url.startswith(self.__INFO_URL) and request.method == "POST":
                    response = request.response
                    if response and response.status_code == 200:
                        data = json.loads(decode(response.body, response.headers.get('Content-Encoding', 'identity')))
                        for sections in data["response"]["personalizedSections"]:
                            if "hotels" in sections:
                                responses.extend(sections["hotels"])

        # logging.info(json.dumps({'log': responses}, indent=4))
        self.hotel_responses = responses

        docs = []

        for hotel in self.hotel_responses[:1]:
            hotel_id = hotel["id"]
            lat = hotel["geoLocation"]["latitude"]
            lng = hotel["geoLocation"]["longitude"]

            reviews_df = self.__get_reviews(hotel_id, lat, lng)

            doc = dict()

            doc['id'] = hotel_id
            doc['name'] = hotel['name']
            doc['prices'] = [
                {
                    'price': int(hotel['priceDetail']['price']),
                    'bookingDate': (datetime.now() + timedelta(days=1)).strftime("%m%d%Y"),
                    'snapshotDate': datetime.now().strftime("%m%d%Y"),
                }
            ]
            doc['coordinates'] = {
                'lat': hotel['geoLocation']['latitude'],
                'lng': hotel['geoLocation']['longitude']
            }
            doc['reviews'] = [self.__review_to_dict(review) for review in reviews_df.to_dict('records')]

            docs.append(doc)

        self.dbm.insert(docs)
        

    def __review_to_dict(self, review):
        return {
            'id': review['id'],
            'metadata': {
                'title': review['title'],
                'upvote': review['upvote'],
                'reviewText': review['reviewText']
            },
            'images': review['images']
        }

    def __cleanup__(self):
        # quit driver
        self.driver.close()
        self.driver.quit()
        del self.driver

        # kill any stray chromedriver instances forcefully
        os.system(f"taskkill /IM chromedriver_v{self.driver_version}_mod.exe /F")

    def __get_selenium_version(self):
        version = selenium.__version__
        return int(version.split('.', maxsplit=1)[0])

    def __check_internet(self):
        try:
            # see if host name can be resolved => a DNS is listening
            host = socket.gethostbyname(self.REMOTE_SERVER)
            # connect to host
            s = socket.create_connection((host, 80), 2)
            s.close()
        except:
            logging.debug("no internet connection. retrying...")
            return False
        else:
            return True

    def __get_browser_version(self):
        if 'browserVersion' in self.driver.capabilities:
            version = self.driver.capabilities['browserVersion']
        else:
            version = self.driver.capabilities['version']

        return int(version.split('.')[0])
    
    def __get_city_code(self, city):
        pathParams = {
            'q': city,
            'sf': 'true',
            'sgr': 't',
            'language': 'eng',
            'region': 'in',
            'currency': 'INR',
            'idContext': 'B2C',
            'countryCode': 'IN'
        }

        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json;charset=utf-8',
            'currency': 'INR',
            'language': 'eng',
            'origin': 'https://www.makemytrip.com',
            'referer': 'https://www.makemytrip.com/',
            'region': 'in',
            'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'server': 'b2c',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'
        }

        logging.info('sending request for city code')

        try:
            r = requests.get(self.__AUTOSUGGEST_URL, params=pathParams, headers=headers)
            if r.status_code == 200:
                data = r.json()
                if len(data) > 0:
                    for suggestion in data:
                        if 'id' in suggestion and 'countryCode' in suggestion and suggestion['countryCode'] == 'IN':
                            return suggestion['id']
                else:
                    # TODO: throw exception
                    pass
            else:
                # TODO: throw exception
                pass
        except:
            # TODO: throw exception
            pass

    def __form_url(self, city_code):
        pathParams = {
            'checkin': (datetime.now() + timedelta(days=1)).strftime("%m%d%Y"),
            'city': city_code,
            'checkout': (datetime.now() + timedelta(days=2)).strftime("%m%d%Y"),
            'roomStayQualifier': '2e0e',
            'locusId': city_code,
            'country': 'IN',
            'locusType': 'city'
        }
        return self.__BASE_URL + '?' + urlencode(pathParams, safe='/')

    def __scroll_to_end(self, scroll_limit=1):
        # Get scroll height
        last_height = self.driver.execute_script("return document.body.scrollHeight")

        count = 0
        while True and count < scroll_limit:
            # Scroll down to bottom
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # Wait to load page
            sleep(self.__SCROLL_PAUSE_TIME)

            # Calculate new scroll height and compare with last scroll height
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

            count += 1

    def __form_hotel_url(self, hotel_id, lat, lng):
        pathParams = {
            'hotelId': hotel_id,
            '_uCurrency': 'INR',
            'checkin': (datetime.now() + timedelta(days=1)).strftime("%m%d%Y"),
            'checkout': (datetime.now() + timedelta(days=2)).strftime("%m%d%Y"),
            'city': self.city_code,
            'country': 'IN',
            'lat': lat,
            'lng': lng,
            'locusId': self.city_code,
            'roomStayQualifier': '2e0e',
            'locusType': 'city'
        }
        return self.__HOTEL_URL + "?" + urlencode(pathParams, safe='/')

    def __get_reviews(self, hotel_id, lat, lng):
        self.driver.get(self.__form_hotel_url(hotel_id, lat, lng))
        sleep(self.__LOAD_PAUSE_TIME)
        self.__scroll_to_end()


        try:
            pagination = WebDriverWait(self.driver, self.__LOAD_PAUSE_TIME).until(EC.presence_of_element_located((By.XPATH, "//ul[contains(@class, 'pagination')]")))
        except TimeoutException as ex:
            pagination = None
            print("pagination not found")

        review_pages_count = 0

        while review_pages_count < self.__REVIEW_MAX_PAGES:
            try:
                if pagination is None:
                    break

                next_page_btn = WebDriverWait(pagination, self.__CLICKABLE_WAIT_TIME).until(EC.element_to_be_clickable((By.XPATH, "//li/a[contains(text(), '⟩')]")))
                # next_page_btn = pagination.find_element(by=By.XPATH, value="//li/a[contains(text(), '⟩')]")
                if next_page_btn.is_enabled():
                    self.driver.execute_script(
                        "arguments[0].click();",
                        next_page_btn
                    )
                    sleep(self.__REVIEW_LOAD_TIME)
            except:
                break
            review_pages_count += 1

        reviews = []

        if BROWSER == 'firefox':
            for request in self.driver.requests:
                if request.url.startswith(self.__REVIEW_URL.format(hotel_id=hotel_id)) and request.method == "POST":
                    response = request.response
                    if response and response.status_code == 200:
                        try:
                            body = json.loads(decode(response.body, response.headers.get('Content-Encoding', 'identity')))
                            reviews.extend(body["payload"]["response"]["MMT"])
                        except:
                            pass

        reviews_df = pd.DataFrame(reviews)
        print(reviews_df.columns)
        reviews_df.drop_duplicates(subset=['id'], inplace=True)
        reviews_df.drop(list(set(reviews_df.columns) & set(['travellerName', 'responseToReview', 'crawledData'])), axis=1, inplace=True)

        if 'images' in reviews_df.columns:
            reviews_df['images'] = reviews_df['images'].apply(lambda l: [li['imgUrl'] for li in l] if type(l) == list else [])

        return reviews_df


api = MMTHotelsAPI('Darjeeling', dbName='mmt', tableName='hotels')
