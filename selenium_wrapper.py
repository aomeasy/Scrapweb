"""
Selenium wrapper to replace Playwright functionality
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
import os
import time
import logging

logger = logging.getLogger(__name__)

class SeleniumBrowser:
    """Selenium browser wrapper to mimic Playwright API"""
    
    def __init__(self, headless=True):
        self.driver = None
        self.headless = headless
        
    def __enter__(self):
        self.driver = self._create_driver()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.driver:
            self.driver.quit()
    
    def _create_driver(self):
        """Create Chrome driver with proper configuration"""
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument('--headless')
        
        # Essential options for cloud environment
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-web-security')
        chrome_options.add_argument('--disable-features=VizDisplayCompositor')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        try:
            # Try to use system Chrome first
            chrome_path = '/usr/bin/google-chrome-stable'
            if os.path.exists(chrome_path):
                chrome_options.binary_location = chrome_path
                logger.info(f"Using system Chrome at: {chrome_path}")
            
            # Create driver
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Remove automation indicators
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            # Set timeouts
            driver.set_page_load_timeout(30)
            driver.implicitly_wait(10)
            
            logger.info("Chrome driver created successfully")
            return driver
            
        except Exception as e:
            logger.error(f"Error creating Chrome driver: {e}")
            raise

    def new_page(self):
        """Create a new page (tab) - returns SeleniumPage object"""
        return SeleniumPage(self.driver)

class SeleniumPage:
    """Selenium page wrapper to mimic Playwright Page API"""
    
    def __init__(self, driver):
        self.driver = driver
        self.wait = WebDriverWait(driver, 10)
    
    def goto(self, url, timeout=30):
        """Navigate to URL"""
        self.driver.get(url)
        return True
    
    def fill(self, selector, text):
        """Fill input field"""
        element = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
        element.clear()
        element.send_keys(text)
    
    def click(self, selector, timeout=10):
        """Click element"""
        element = WebDriverWait(self.driver, timeout).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
        )
        element.click()
    
    def wait_for_selector(self, selector, timeout=10):
        """Wait for element to be present"""
        return self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
    
    def wait_for_load_state(self, state='load', timeout=30):
        """Wait for page load state"""
        if state == 'load':
            self.wait.until(lambda driver: driver.execute_script("return document.readyState") == "complete")
    
    def locator(self, selector):
        """Get locator for element"""
        return SeleniumLocator(self.driver, selector)
    
    def query_selector(self, selector):
        """Find element by selector"""
        try:
            return self.driver.find_element(By.CSS_SELECTOR, selector)
        except:
            return None
    
    def query_selector_all(self, selector):
        """Find all elements by selector"""
        return self.driver.find_elements(By.CSS_SELECTOR, selector)
    
    def content(self):
        """Get page HTML content"""
        return self.driver.page_source
    
    def screenshot(self, path=None, full_page=False):
        """Take screenshot"""
        if full_page:
            # Set window size to capture full page
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
        
        if path:
            return self.driver.save_screenshot(path)
        else:
            return self.driver.get_screenshot_as_png()
    
    def evaluate(self, script):
        """Execute JavaScript"""
        return self.driver.execute_script(script)

class SeleniumLocator:
    """Selenium locator wrapper"""
    
    def __init__(self, driver, selector):
        self.driver = driver
        self.selector = selector
        self.wait = WebDriverWait(driver, 10)
    
    def click(self, timeout=10):
        """Click element"""
        element = WebDriverWait(self.driver, timeout).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, self.selector))
        )
        element.click()
    
    def fill(self, text):
        """Fill input field"""
        element = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, self.selector)))
        element.clear()
        element.send_keys(text)
    
    def text_content(self):
        """Get text content"""
        element = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, self.selector)))
        return element.text
    
    def is_visible(self):
        """Check if element is visible"""
        try:
            element = self.driver.find_element(By.CSS_SELECTOR, self.selector)
            return element.is_displayed()
        except:
            return False

# Context manager for browser
def sync_selenium():
    """Context manager similar to sync_playwright()"""
    class SeleniumContext:
        @property
        def chromium(self):
            return SeleniumBrowserLauncher()
    
    return SeleniumContext()

class SeleniumBrowserLauncher:
    """Browser launcher similar to playwright.chromium"""
    
    def launch(self, headless=True, **kwargs):
        """Launch browser"""
        return SeleniumBrowser(headless=headless)
