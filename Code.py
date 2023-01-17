
#Una tecnica utilizada para acelerar el proceso de descarga cuando se ejecutan tecnicas de web scraping es la paralizacion de nucleos o ejecucion
#de multiprocesos. Este documento se divide en dos partes, las cuales permiten obtener el ingreso publico a nivel municipal por separado. La diferencia
#se encuentra en que en la primera parte no se realiza una paralelizacion de núcleos, mientras que, en la segunda, sí. 

#==============================================================
#Sin paralelización de núcleos
#==============================================================
import os
import time
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By

options = webdriver.ChromeOptions()
options.add_argument('--start-maximized')
options.add_experimental_option('prefs', {
        "download.prompt_for_download": False,
        "download.default_directory" : r"C:\Users\Cesar\Desktop\mef_ingreso_canon_2019",
        "savefile.default_directory": r"C:\Users\Cesar\Desktop\mef_ingreso_canon_2019"})
chromedriver =  r'C:\Program Files\Chromedriver\chromedriver.exe'
os.environ["webdriver.chrome.driver"] = chromedriver

driver = webdriver.Chrome(chromedriver, chrome_options=options)

for year in range(2019,2021):
    base_dato = []
    driver.get('https://apps5.mineco.gob.pe/transparenciaingresos/Navegador/default.aspx?y=' + str(year))
    #Solo hay un frame a diferencia del documento "Web-Scraping-SIAF-1"
    driver.switch_to.frame(0)

    #Tipo de gobierno
    driver.find_element('name','ctl00$CPH1$BtnTipoGobierno').click()

    #Gobiernos locales
    driver.find_element('xpath',"//*[contains(text(), 'LOCALES')]").click()

    if year>=2012:
        driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[1]/table/tbody/tr[2]/td[1]/input[2]').click()

        driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[3]/div/table[2]/tbody/tr[1]').click()

        #Departamentos
        driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[1]/table/tbody/tr[2]/td[1]/input[1]').click()

    else:
        #Departamentos
        driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[1]/table/tbody/tr[2]/td[1]/input[2]').click()

    tabla_dpto = driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[3]/div/table[2]')
    departamentos = tabla_dpto.find_elements(By.TAG_NAME,'tr')

    for dpto in range(len(departamentos)):
        driver.find_element('id','tr' + str(dpto)).click()

        #Provincias
        driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[1]/table/tbody/tr[2]/td[1]/input[1]').click()

        tabla_prov = driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[3]/div/table[2]')
        provincias = tabla_prov.find_elements(By.TAG_NAME,'tr')
        for prov in range(len(provincias)):
            driver.find_element('id','tr' + str(prov)).click()

            #Distritos
            driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[1]/table/tbody/tr[2]/td[1]/input').click()

            #Descargar datos
            dpto_valor = driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[2]/table/tbody/tr[4]/td[2]').text
            prov_valor = driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[2]/table/tbody/tr[5]/td[2]').text

            tabla_muni = driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[3]/div/table[2]')
            municipalidades = tabla_muni.find_elements(By.TAG_NAME,'tr')
            for muni in range(1,len(municipalidades)):
                muni_valor = driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[3]/div/table[2]/tbody/tr[' + str(muni) + ']/td[2]').text
                pia = driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[3]/div/table[2]/tbody/tr[' + str(muni) + ']/td[3]').text
                pim = driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[3]/div/table[2]/tbody/tr[' + str(muni) + ']/td[4]').text
                recaudo = driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[3]/div/table[2]/tbody/tr[' + str(muni) + ']/td[5]').text

                base_dato.append([year,dpto_valor,prov_valor,muni_valor,pia,pim,recaudo])
                df = pd.DataFrame(base_dato,columns=['year','dpto_valor','prov_valor','muni_valor','pia','pim','recaudo'])
                df.to_csv('A_' + str(year) + '.csv',encoding='utf-8-sig',index=False)
                time.sleep(1)

            #Cambio de provincias
            if year>=2012:
                driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[2]/table/tbody/tr[5]/td[1]/img').click()
            else:
                driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[2]/table/tbody/tr[4]/td[1]/img').click()

        #Cambio de departamentos
        if year>=2012:
            driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[2]/table/tbody/tr[4]/td[1]/img').click()
        else:
            driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[2]/table/tbody/tr[3]/td[1]/img').click()

#==============================================================
#Con paralelización de núcleos
#==============================================================
#Se recomienda leer README previamente. La paralelizacion de nucleos consiste en asignar una tarea en particular a cada nucleo de la computadora.
#En este caso se esta asignando a cada nucleo un año en especifico en el rango de 2012-2021 mediante la funcion SIAF. Si la computadora no soporta
#tantos nucleos, es necesario reducir el rango. Asimismo, no se recomienda poner la totalidad de nucleos porque la computadora puede colapsar. Es 
#recomendable asignar un nucleo menos a la totalidad de nucleos que dispone la computadora.

import os
import time
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from multiprocessing import Pool

def(siaf):
    options = webdriver.ChromeOptions()
    options.add_argument('--start-maximized')
    options.add_experimental_option('prefs', {
            "download.prompt_for_download": False,
            "download.default_directory" : r"C:\Users\Cesar\Desktop\mef_ingreso_canon_2019",
            "savefile.default_directory": r"C:\Users\Cesar\Desktop\mef_ingreso_canon_2019"})
    chromedriver =  r'C:\Program Files\Chromedriver\chromedriver.exe'
    os.environ["webdriver.chrome.driver"] = chromedriver

    driver = webdriver.Chrome(chromedriver, chrome_options=options)

    base_dato = []
    driver.get('https://apps5.mineco.gob.pe/transparenciaingresos/Navegador/default.aspx?y=' + str(year))
    #Solo hay un frame a diferencia del documento "Web-Scraping-SIAF-1"
    driver.switch_to.frame(0)

    #Tipo de gobierno
    driver.find_element('name','ctl00$CPH1$BtnTipoGobierno').click()

    #Gobiernos locales
    driver.find_element('xpath',"//*[contains(text(), 'LOCALES')]").click()

    if year>=2012:
        driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[1]/table/tbody/tr[2]/td[1]/input[2]').click()

        driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[3]/div/table[2]/tbody/tr[1]').click()

        #Departamentos
        driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[1]/table/tbody/tr[2]/td[1]/input[1]').click()

    else:
        #Departamentos
        driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[1]/table/tbody/tr[2]/td[1]/input[2]').click()

    tabla_dpto = driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[3]/div/table[2]')
    departamentos = tabla_dpto.find_elements(By.TAG_NAME,'tr')

    for dpto in range(len(departamentos)):
        driver.find_element('id','tr' + str(dpto)).click()

        #Provincias
        driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[1]/table/tbody/tr[2]/td[1]/input[1]').click()

        tabla_prov = driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[3]/div/table[2]')
        provincias = tabla_prov.find_elements(By.TAG_NAME,'tr')
        for prov in range(len(provincias)):
            driver.find_element('id','tr' + str(prov)).click()

            #Municipalidad
            driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[1]/table/tbody/tr[2]/td[1]/input').click()

            #Descargar datos
            dpto_valor = driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[2]/table/tbody/tr[4]/td[2]').text
            prov_valor = driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[2]/table/tbody/tr[5]/td[2]').text

            tabla_muni = driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[3]/div/table[2]')
            municipalidades = tabla_muni.find_elements(By.TAG_NAME,'tr')
            for muni in range(1,len(municipalidades)):
                muni_valor = driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[3]/div/table[2]/tbody/tr[' + str(muni) + ']/td[2]').text
                pia = driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[3]/div/table[2]/tbody/tr[' + str(muni) + ']/td[3]').text
                pim = driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[3]/div/table[2]/tbody/tr[' + str(muni) + ']/td[4]').text
                recaudo = driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[3]/div/table[2]/tbody/tr[' + str(muni) + ']/td[5]').text

                base_dato.append([year,dpto_valor,prov_valor,muni_valor,pia,pim,recaudo])
                df = pd.DataFrame(base_dato,columns=['year','dpto_valor','prov_valor','muni_valor','pia','pim','recaudo'])
                df.to_csv('A_' + str(year) + '.csv',encoding='utf-8-sig',index=False)
                time.sleep(1)

            #Cambio de provincias
            if year>=2012:
                driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[2]/table/tbody/tr[5]/td[1]/img').click()
            else:
                driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[2]/table/tbody/tr[4]/td[1]/img').click()

        #Cambio de departamentos
        if year>=2012:
            driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[2]/table/tbody/tr[4]/td[1]/img').click()
        else:
            driver.find_element('xpath','/html/body/form/div[4]/div[3]/div[2]/table/tbody/tr[3]/td[1]/img').click()

if __name__ == "__main__":
        print(Pool().map(siaf,range(2012,2021)))