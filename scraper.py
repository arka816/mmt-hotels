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
from selenium.webdriver.firefox.service import Service as FirefoxService
from seleniumwire.webdriver import FirefoxOptions

from seleniumwire.utils import decode

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.keys import Keys

from qgis.PyQt.QtCore import QObject, pyqtSignal

import os
import requests
import socket
import json
from time import sleep
from urllib.parse import urlencode, urlparse
from datetime import datetime, timedelta

import pandas as pd

from db import DBManager

from chrome_version import get_chrome_version

import logging

class SignalLogger(logging.Logger):
    def __init__(self, name, level=logging.INFO):
        super().__init__(name, level)
        self.addMessage = None
        self.addError = None

    def set_signal(self, addMessage, addError):
        self.addMessage = addMessage
        self.addError = addError

    def info(self, msg, *args, **kwargs):
        super().info(msg, *args, **kwargs)
        self.addMessage.emit(str(msg))

    def warning(self, msg, *args, **kwargs):
        super().warning(msg, *args, **kwargs)
        self.addMessage.emit(str(msg))

    def error(self, msg, *args, **kwargs):
        super().error(msg, *args, **kwargs)
        self.addError.emit(str(msg)) 

logging.setLoggerClass(SignalLogger)

logging.basicConfig(
    filename=os.path.join(os.path.dirname(os.path.realpath(__file__)), ".log"),
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a',
    level=logging.INFO,
    force=True
)


# selenium_logger = logging.getLogger('seleniumwire')
# selenium_logger.setLevel(logging.DEBUG)
# handler = logging.FileHandler(os.path.join(os.path.dirname(os.path.realpath(__file__)), "seleniumwire.log"))
# selenium_logger.addHandler(handler)

MIN_CHROMEDRIVER_VERSION = 105

CHUNK_SIZE = 4096

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
    op.binary_location = "C:\\Program Files\Mozilla Firefox\\firefox.exe"
    # op.add_experimental_option("excludeSwitches", ["enable-logging"])


class MMTWorker(QObject):
    __BASE_URL = 'https://www.makemytrip.com/hotels/'
    __AUTOSUGGEST_URL = 'https://mapi.makemytrip.com/autosuggest/v5/search'
    __INFO_URL = "https://mapi.makemytrip.com/clientbackend/cg/search-hotels/DESKTOP/2"
    __HOTEL_URL = "https://www.makemytrip.com/hotels/hotel-details/"
    __REVIEW_URL = "https://mapi.makemytrip.com/clientbackend/entity/api/hotel/{hotel_id}/flyfishReviews"

    __LOAD_PAUSE_TIME = 10
    __SCROLL_PAUSE_TIME = 5
    __SCROLL_LIMIT = 2
    __REVIEW_LOAD_TIME=5
    __CLICKABLE_WAIT_TIME = 2
    __HANDLER_LAG = 2
    __REQUEST_LAG = 10

    __REVIEW_MAX_PAGES = 1


    finished = pyqtSignal(list)
    progress = pyqtSignal(int)
    addMessage = pyqtSignal(str)
    addError = pyqtSignal(str)
    total = pyqtSignal(int)


    def __init__(self, city, db_name, table_name, image_dir, csv_path, max_hotels, max_reviews, save_images) -> None:
        QObject.__init__(self)
        
        self.city = city
        self.db_name = db_name
        self.table_name = table_name
        self.image_dir = image_dir
        self.csv_path = csv_path
        self.max_hotels = max_hotels
        self.max_reviews = max_reviews
        self.save_images = save_images

        self.logger = logging.getLogger(__name__)
        self.logger.set_signal(self.addMessage, self.addError)

        # select best version for selenium webdriver
        self.selenium_version = self.__get_selenium_version()

        self.logger.info("selenium version: %s", self.selenium_version)
        
        if BROWSER == 'chrome':
            self.browser_version = get_chrome_version()
            self.logger.info("browser version: %s", self.browser_version)

            try:
                '''
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
                '''
                self.browser_version_major = int(self.browser_version.split('.', maxsplit=1)[0])

                self.logger.info(self.browser_version_major)

                if self.browser_version_major <= MIN_CHROMEDRIVER_VERSION:
                    self.logger.error(f"existing chrome version {self.browser_version} is not supported.")
                else:
                    if self.selenium_version == 3:
                        self.driver = webdriver.Chrome(
                            desired_capabilities=capabilities, 
                            executable_path=os.path.join(os.path.dirname(__file__), 'exe', f"chromedriver_v{self.browser_version_major}_mod.exe"), 
                            options=op)
                    elif self.selenium_version == 4:
                        chromeService = Service(os.path.join(os.path.dirname(__file__), 'exe', f"chromedriver_v{self.browser_version_major}_mod.exe"))
                        self.driver = webdriver.Chrome(desired_capabilities=capabilities, service=chromeService, options=op)
            except:
                self.__cleanup__()
                self.logger.error("error loading chromedriver", exc_info=True)
            else:
                self.logger.info(f"loaded chromedriver version {self.browser_version_major}")
        else:
            try:
                firefoxService = FirefoxService(
                    executable_path=os.path.join(os.path.dirname(__file__), 'exe', f"geckodriver.exe")
                )

                self.driver = Firefox(
                    # firefox_binary="C:\\Program Files\Mozilla Firefox\\firefox.exe", 
                    # executable_path=os.path.join(os.path.dirname(__file__), 'exe', f"geckodriver.exe"),
                    service=firefoxService,
                    options=op,
                    seleniumwire_options={
                        'mitm_http2': False
                    }
                )
            except:
                self.__cleanup__()
                self.logger.error("error loading geckodriver", exc_info=True)
            else:
                self.logger.info(f"loaded geckodriver")

        try:
            self.dbm = DBManager(self.db_name, self.table_name, logging=self.logger)
        except:
            self.logger.error("mongodb error", exc_info=True)

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
            # self.logger.info(logs_raw)

            for index, log_raw in enumerate(logs_raw):
                if not self.running:
                    self.halt_error()

                self.logger.info(f"processed raw log: {index}")

                msg = json.loads(log_raw["message"])["message"]
                if 'requestId' in msg['params'] and 'request' in msg['params']:
                    request = msg['params']['request']
                    if "method" in request and "url" in request:
                        # print(request['url'], request["method"])
                        if request['url'].startswith(self.__INFO_URL) and request["method"] == "POST":
                            try:
                                response = self.driver.execute_cdp_cmd('Network.getResponseBody', {'requestId': msg["params"]["requestId"]})
                                responses.append(response) 
                            except:
                                pass

                if len(responses) >= self.max_hotels:
                    break
        elif BROWSER == 'firefox':
            for request in self.driver.requests:
                if not self.running:
                    self.halt_error()

                if request.url.startswith(self.__INFO_URL) and request.method == "POST":
                    response = request.response
                    if response and response.status_code == 200:
                        data = json.loads(decode(response.body, response.headers.get('Content-Encoding', 'identity')))
                        for sections in data["response"]["personalizedSections"]:
                            if "hotels" in sections:
                                responses.extend(sections["hotels"])

                if len(responses) >= self.max_hotels:
                    break

        if len(responses) == 0:
            # get backup data from window.__INITIAL_STATE__ 
            intial_state = self.driver.execute_script("return window.__INITIAL_STATE__;")

            for sections in intial_state['hotelListing']['personalizedSections']:
                if "hotels" in sections:
                    responses.extend(sections["hotels"])
        
        self.hotel_responses = responses[:self.max_hotels]

        # let the UI know the total number of hotels in worker list
        self.total.emit(len(self.hotel_responses))

        self.logger.info(f"loading reviews for {len(self.hotel_responses)} hotels")

        docs = []

        self.image_req_session = requests.Session()

        for index, hotel in enumerate(self.hotel_responses):
            # iterate through each hotel and download review metadata and images
            if not self.running:
                self.halt_error()

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
                    'bookingDate': (datetime.now() + timedelta(days=1)).strftime("%d-%m-%Y"),
                    'snapshotDate': datetime.now().strftime("%d-%m-%Y"),
                }
            ]
            doc['coordinates'] = {
                'lat': hotel['geoLocation']['latitude'],
                'lng': hotel['geoLocation']['longitude']
            }
            doc['reviews'] = [self.__review_to_dict(review) for review in reviews_df.to_dict('records')]

            docs.append(doc)

            self.progress.emit(index + 1)

        return docs
        
    def __review_to_dict(self, review):
        review_obj = {
            'id': review['id'],
            'metadata': {
                'title': review['title'],
                'upvote': review['upvote'],
                'reviewText': review['reviewText']
            },
        }

        if 'images' in review:
            review_obj['images'] = review['images']

        if 'image_paths' in review:
            review_obj['image_paths'] = review['image_paths']

        return review_obj

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
            self.logger.info("no internet connection. retrying...")
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
        try:
            city_label = WebDriverWait(self.driver, self.__LOAD_PAUSE_TIME).until(EC.element_to_be_clickable((By.XPATH, "//label[@for='city']")))
        except TimeoutException as ex:
            self.logger.warning("input for city not found")
        else:
            self.driver.execute_script(
                f"arguments[0].click();",
                city_label
            )

            sleep(self.__HANDLER_LAG)

            city_input = WebDriverWait(self.driver, self.__LOAD_PAUSE_TIME).until(EC.element_to_be_clickable((By.XPATH, "//input[contains(@class, 'react-autosuggest__input')]")))

            # city_input.send_keys(city)
            self.driver.execute_script(
                f"arguments[0].click();",
                city_input
            )

            city_input.send_keys(city)

            self.driver.execute_script(
                f"arguments[0].click();",
                city_input
            )
        
            city_input.send_keys(Keys.ENTER)

            sleep(self.__HANDLER_LAG + self.__REQUEST_LAG)

            suggestions = []

            if BROWSER == 'firefox':
                for request in self.driver.requests:
                    if request.url.startswith(self.__AUTOSUGGEST_URL) and request.method == "GET":
                        params = request.params
                        if 'q' in params and params['q'] == city:
                            response = request.response
                            if response and response.status_code == 200:
                                try:
                                    body = json.loads(decode(response.body, response.headers.get('Content-Encoding', 'identity')))
                                    suggestions.extend(body)
                                except Exception as ex:
                                    self.logger.info(ex)
                                else:
                                    break


            # filter suggestions for first entry with type = 'city'
            for suggestion in suggestions:
                if suggestion['type'] == 'city':
                    return suggestion['cityCode'], suggestion.get('countryCode', 'IN')
        
    def __get_city_code_direct(self, city):
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

        self.logger.info('sending request for city code')

        try:
            r = requests.get(self.__AUTOSUGGEST_URL, params=pathParams, headers=headers)
            self.logger.info(r.status_code)
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

    def __form_url(self):
        pathParams = {
            'checkin': (datetime.now() + timedelta(days=1)).strftime("%m%d%Y"),
            'city': self._city_code,
            'checkout': (datetime.now() + timedelta(days=2)).strftime("%m%d%Y"),
            'roomStayQualifier': '2e0e',
            'locusId': self._city_code,
            'country': self._country_code,
            'locusType': 'city',
            'searchText': self.city
        }
        return self.__BASE_URL + 'hotel-listing' + '?' + urlencode(pathParams, safe='/')

    def __scroll_to_end(self, scroll_limit=1):
        # Get scroll height
        last_height = self.driver.execute_script("return document.body.scrollHeight")

        count = 0
        while count < scroll_limit:
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
        
        self.logger.info('scrolled to bottom')

    def __form_hotel_url(self, hotel_id, lat, lng):
        pathParams = {
            'hotelId': hotel_id,
            '_uCurrency': 'INR',
            'checkin': (datetime.now() + timedelta(days=1)).strftime("%m%d%Y"),
            'checkout': (datetime.now() + timedelta(days=2)).strftime("%m%d%Y"),
            'city': self._city_code,
            'country': 'IN',
            'lat': lat,
            'lng': lng,
            'locusId': self._city_code,
            'roomStayQualifier': '2e0e',
            'locusType': 'city'
        }
        return self.__HOTEL_URL + "?" + urlencode(pathParams, safe='/')

    def __get_reviews(self, hotel_id, lat, lng):
        hotel_url = self.__form_hotel_url(hotel_id, lat, lng)
        self.logger.info(f"loading hotel url: {hotel_url}")

        self.driver.get(hotel_url)
        sleep(self.__LOAD_PAUSE_TIME)
        self.__scroll_to_end()


        try:
            pagination = WebDriverWait(self.driver, self.__LOAD_PAUSE_TIME).until(EC.presence_of_element_located((By.XPATH, "//ul[contains(@class, 'pagination')]")))
        except TimeoutException as ex:
            pagination = None
            self.logger.warning("pagination not found")

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
        reviews_df.drop_duplicates(subset=['id'], inplace=True)
        reviews_df.drop(list(set(reviews_df.columns) & set(['travellerName', 'responseToReview', 'crawledData'])), axis=1, inplace=True)

        if 'images' in reviews_df.columns:
            reviews_df['images'] = reviews_df['images'].apply(lambda l: [li['imgUrl'] for li in l] if type(l) == list else [])

            if self.save_images:
                reviews_df['image_paths'] = reviews_df['images'].apply(lambda l: [self.__download_images(li) for li in l])

        return reviews_df

    def __download_images(self, url: str) -> str:
        '''
            download the image and return the path
        '''
        if not url.startswith('http:') or not url.startswith('https:'):
            url = 'https:' + url

        url_components = urlparse(url)
        path_param = url_components.path.strip('/')

        image_path = os.path.join(self.image_dir, path_param)

        os.makedirs(os.path.dirname(image_path), exist_ok=True)

        r = self.image_req_session.get(url, stream=True)
        if r.status_code == 200:
            try:
                with open(image_path, 'wb') as f:
                    for chunk in r.iter_content(CHUNK_SIZE):
                        if not self.running:
                            self._halt_error()
                            return
                        f.write(chunk)
            except:
                self.logger.warning(f"could not write image file")
            else:
                self.logger.info(f"saved image file at {image_path}")
                return image_path
        else:
            self.addMessage.emit(f"could not write image file")

    def halt_error(self):
        self.addMessage.emit("worker halted forcefully")
        self.finished.emit([])

    def __serialize_csv(self, docs):
        '''
            only stores first price for each observed hotel in case of multiple prices
        '''
        dfs = []

        for doc in docs:
            titles = [review['metadata']['title'] for review in doc['reviews']]
            upvotes = [review['metadata']['upvote'] for review in doc['reviews']]
            review_texts = [review['metadata']['reviewText'] for review in doc['reviews']]
            image_urls = [review['images'] if 'images' in review else '' for review in doc['reviews']]
            image_paths = [review['image_paths'] if 'image_paths' in review else '' for review in doc['reviews']]

            df = pd.DataFrame({
                'title': titles, 
                'upvotes': upvotes, 
                'text': review_texts,
                'image_urls': image_urls,
                'image_paths': image_paths
            })

            df['hotel_name'] = doc['name']
            df['lat'] = doc['coordinates']['lat']
            df['lng'] = doc['coordinates']['lng']
            df['price'] = doc['prices'][0]['price']
            df['booking_date'] = doc['prices'][0]['bookingDate']
            df['snapshot_date'] = doc['prices'][0]['snapshotDate']

            dfs.append(df)

        df = pd.concat(dfs)

        return df


    def run(self):
        self.running = True

        # Step 1: get city code from city name
        try:
            self.driver.get(self.__BASE_URL)
        except:
            self.logger.error(f"error fetching URL: {self.__BASE_URL}", exc_info=True)

        self.driver.delete_all_cookies()
        self.logger.info("all cookies deleted")

        self._city_code, self._country_code = self.__get_city_code(self.city)
        if self._city_code is None:
            self.logger.error("city code not found")
        else:
            self.logger.info(f"City Code: {self._city_code}")

        # Step 2: form url and start selenium session
        self._url = self.__form_url()
        self.logger.info(f"URL: {self._url}")
        
        try:
            self.driver.get(self._url)
        except:
            self.logger.error(f"error fetching URL: {self._url}", exc_info=True)

        # Step 3: scroll and scrape data from network requests
        docs = self.__get_hotel_data()

        # Step 4: insert into database
        self.dbm.insert(docs)

        # Step 5: save as csv file
        data = self.__serialize_csv(docs)
        data.to_csv(self.csv_path)

        self.finished.emit(docs)

    def stop(self):
        self.running = False

if __name__ == "__main__":
    worker = MMTWorker('darjeeling', 'mmt', 'hotels', 1, 1)
    worker.run()
