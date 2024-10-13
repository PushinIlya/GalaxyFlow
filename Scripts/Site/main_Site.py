#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# импортируем библиотеки
import subprocess
import threading
import datetime
import pathlib
import py7zr
import pyzipper
import time
import sys
import os
from openpyxl import load_workbook
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


# In[ ]:


login_pbi = ''
pass_pbi = ""
unzip_pwd = ''
id_dataflow = 'NodeContainer5Dataflow_tab_48d7e8ee1-ae26-41d1-af0e-e1f27c4f84fd' # id потока данных в рабочей области
id_dataset = 'NodeContainer3Dataset_tab_3155187' # id набора данных в рабочей области


# In[ ]:


# запишем путь к папке с исходными данными в переменную
source = pathlib.Path(os.getcwd().replace('Fin_Flow', os.path.join('YandexDisk', 'Fin_Flow')).replace('Scripts', 'Source data'))


# In[ ]:


# создадим новый сервис Chrome
service = ChromeService(ChromeDriverManager().install())

# создадим объект для опций Chrome драйвера
options = webdriver.ChromeOptions()


# In[ ]:


# получим user-agent
driver_temp = webdriver.Chrome(service=service, options=options)
user_agent = driver_temp.execute_script("return navigator.userAgent")

# добавим user-agent
options.add_argument(f'user-agent={user_agent}')

driver_temp.close()


# In[ ]:


# создадим новый драйвер Chrome
driver = webdriver.Chrome(service=service, options=options)


# In[ ]:


# откроем окно браузера и залогинимся в Power BI
driver.get('https://app.powerbi.com')
time.sleep(5)
driver.switch_to.window(driver.window_handles[-1]) # переключимся на последнее открытое окно
driver.maximize_window() # развернём окно браузера

if 'https://login.microsoftonline.com' not in driver.current_url:
    login_input = driver.find_element(By.CSS_SELECTOR, 'input[type="text"]')
    login_input.clear()
    login_input.send_keys(login_pbi)
    
    time.sleep(1)
    button = driver.find_element(By.ID, 'submitBtn')
    button.click()
else: 
    login_input = driver.find_element(By.CSS_SELECTOR, 'input[type="email"]')
    login_input.clear()
    login_input.send_keys(login_pbi)
    
    time.sleep(1)
    button = driver.find_element(By.ID, 'idSIButton9')
    button.click()
    
time.sleep(5)
pass_input = driver.find_element(By.CSS_SELECTOR, 'input[type="password"]')
pass_input.clear()
pass_input.send_keys(pass_pbi)
    
time.sleep(1)
button = driver.find_element(By.ID, 'idSIButton9')
button.click()
    
# ожидание подтверждения двухфакторной аутентификации
time.sleep(5)
wait = WebDriverWait(driver, 30)
wait.until(ec.presence_of_element_located((By.ID, "idRichContext_DisplaySign")))
print(f'Код аутентификатора: {driver.find_element(By.ID, "idRichContext_DisplaySign").text}')
wait = WebDriverWait(driver, 30)
wait.until(ec.url_to_be('https://login.microsoftonline.com/common/SAS/ProcessAuth'))

time.sleep(5)
button = driver.find_element(By.ID, 'idSIButton9')
button.click()

# откроем страницу с рабочей областью
time.sleep(5)
driver.get('https://app.powerbi.com/groups/me/lineage?experience=power-bi')
driver.minimize_window() # свернём окно браузера


# In[ ]:


'''Класс-обработчик событий, наследующийся от класса "FileSystemEventHandler"'''
class MyEventHandler(FileSystemEventHandler):
    # при создании файлов запускается таймер, чтобы все необходимые файлы были созданы и код исполнился 1 раз
    def __init__(self):
        self.is_running = False 

    def on_created(self, event):
        if not self.is_running:
            self.is_running = True
            timer = threading.Timer(30, self.execute_code)
            timer.start()

    def execute_code(self):
        # код, который нужно выполнить при создании файла
        if __name__ == '__main__':
            # распакуем архивы с исходными данными
            for archive in os.listdir(source):
                if archive.endswith('.zip'):
                    # путь к файлу
                    archive_path = os.path.join(source, archive)
                    # откроем файл в режиме чтения
                    with pyzipper.AESZipFile(archive_path, 'r', compression=pyzipper.ZIP_STORED, encryption=pyzipper.WZ_AES) as file:
                        for member in file.infolist():
                            # извлечём файлы
                            file.extract(member, path=source, pwd=unzip_pwd.encode())
                if archive.endswith('.7z'):
                    # путь к файлу
                    archive_path = os.path.join(source, archive)
                    # откроем файл в режиме чтения
                    with py7zr.SevenZipFile(archive_path, mode='r', password=unzip_pwd) as file:
                        for member in file.getnames():
                            # извлечём файлы
                            file.extract(path=source, targets=member)     
            # удалим все файлы с расширением zip и 7z после распаковки
            for archive in os.listdir(source):
                if archive.endswith('.zip') or archive.endswith('.7z'):
                    os.remove(os.path.join(source, archive))
                    
            # запустим скрипты для обработки исходных данных
            subprocess.run(['python', 'cash_flow.py'])
            if subprocess.run(['python', 'cash_flow.py']).returncode != 0:
                print(f'Ошибка при выполнении скрипта cash_flow.py: {datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")}')
                sys.exit(1)
            subprocess.run(['python', 'osv_50.py'])
            if subprocess.run(['python', 'osv_50.py']).returncode != 0:
                print(f'Ошибка при выполнении скрипта osv_50.py: {datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")}')
                sys.exit(1)
            subprocess.run(['python', 'osv_51.py'])
            if subprocess.run(['python', 'osv_51.py']).returncode != 0:
                print(f'Ошибка при выполнении скрипта osv_51.py: {datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")}')
                sys.exit(1)
            subprocess.run(['python', 'osv_66-67.py'])
            if subprocess.run(['python', 'osv_66-67.py']).returncode != 0:
                print(f'Ошибка при выполнении скрипта osv_66-67.py: {datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")}')
                sys.exit(1)
            subprocess.run(['python', 'ofr.py'])
            if subprocess.run(['python', 'ofr.py']).returncode != 0:
                print(f'Ошибка при выполнении скрипта ofr.py: {datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")}')
                sys.exit(1)
            print(f'{source.name} - Данные обновлены: {datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")}')

            # обновим поток данных
            time.sleep(5) # дождёмся полной загрузки файлов после выполнения скриптов
            button = driver.find_element(By.XPATH, f'//*[@id="{id_dataflow}"]/action-panel/div/button')
            driver.execute_script("arguments[0].scrollIntoView(true);", button) # скроллинг к кнопке
            driver.execute_script("arguments[0].click();", button) # нажмём на кнопку даже если она не видна на экране
            
            # ожидаем обновления потока данных
            wait = WebDriverWait(driver, 300) # каждые 5 минут обновляем страницу, так как анимация обновления может зависнуть
            wait_anim = WebDriverWait(driver, 30)
            # ожидаем появления анимации обновления
            try:
                wait_anim.until(ec.presence_of_element_located((By.XPATH, f'//*[@id="{id_dataflow}"]/section/div[1]/section[2]/status-icon/dataflow-icon-container/span/spinner')))
            except TimeoutException:
                driver.refresh() # обновим страницу, чтобы анимация обновления появилась 
                time.sleep(5)
            # ожидаем пока не пропадёт анимация обновления, чтобы начать обновление набора данных
            while True:
                # проверяем исчезла ли анимация обновления
                try:
                    # дожидаемся исчезновения анимации, в некоторых случаях она может исчезнуть на время и снова появиться
                    # для этого повторно проверяем есть ли анимация обновления
                    element_locator = (By.XPATH, f'//*[@id="{id_dataflow}"]/section/div[1]/section[2]/status-icon/dataflow-icon-container/span/spinner')
                    wait.until_not(ec.presence_of_element_located(element_locator))
                    time.sleep(5)
                    if wait.until_not(ec.presence_of_element_located(element_locator)) == True:
                        break
                    else:
                        continue
                except TimeoutException:
                    driver.refresh() # обновим страницу, так как анимация обновления может зависнуть
                    time.sleep(5)
                    continue
            
            # обновим набор данных
            time.sleep(5)
            button = driver.find_element(By.XPATH, f'//*[@id="{id_dataset}"]/action-panel/div/button')
            driver.execute_script("arguments[0].scrollIntoView(true);", button)
            driver.execute_script("arguments[0].click();", button)
            
            # ожидаем обновления набора данных
            try:
                wait_anim.until(ec.presence_of_element_located((By.XPATH, f'//*[@id="{id_dataset}"]/section/div[1]/section[2]/status-icon/dataset-icon-container-modern/span/spinner')))
            except TimeoutException:
                driver.refresh()
                time.sleep(5)
            while True:
                try:
                    element_locator = (By.XPATH, f'//*[@id="{id_dataset}"]/section/div[1]/section[2]/status-icon/dataset-icon-container-modern/span/spinner')
                    wait.until_not(ec.presence_of_element_located(element_locator))
                    time.sleep(5)
                    if wait.until_not(ec.presence_of_element_located(element_locator)) == True:
                        break
                    else:
                        continue
                except TimeoutException:
                    driver.refresh()
                    time.sleep(5)
                    continue     
            print(f'{source.name} - Отчёт обновлён: {datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")}')
            print()
            
            self.is_running = False
            
# создадим объект класса "Observer" и передадим ему экземпляр обработчика событий
event_handler = MyEventHandler()
observer = Observer()
observer.schedule(event_handler, path=source, recursive=False)

while True:
    try:
        observer.start() # запустим наблюдение за папкой
        observer.join() # ждем, пока наблюдение не будет остановлено
    except Exception:
        observer.stop() # если возникнет ошибка, остановим наблюдение
        print(f'Ошибка при наблюдении за папкой: {datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")}')
        sys.exit(1)

