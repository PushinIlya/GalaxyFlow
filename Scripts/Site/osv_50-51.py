#!/usr/bin/env python
# coding: utf-8

# In[ ]:


'''
Скрипт для преобразования исходных данных из 1C
'''
# импортируем библиотеки
import pandas as pd
import os
import shutil
import pathlib


# In[ ]:


from osv_50 import osv_50_merged
from osv_51 import osv_51_merged


# In[ ]:


# запишем путь к папке с исходными данными в переменную
source = pathlib.Path(os.getcwd().replace('Fin_Flow', os.path.join('YandexDisk', 'Fin_Flow')).replace('Scripts', 'Source data'))


# In[ ]:


# создадим списки с названиями всех необходимых файлов в папке с исходными данными
source_osv_name = [raw_data for raw_data in os.listdir(source) if 'ОСВ 50-51' in raw_data and
                   raw_data.endswith('.xlsx')]

print(source_osv_name)


# In[ ]:


# создадим новый словарь с объединёнными таблицами из osv_50 и osv_51
osv_50_51_merged = {i[0].replace('50', '50_51'): pd.concat([i[1], osv_51_merged[i[0].replace('50', '51')]],
                                                           axis=0).reset_index(inplace=False, drop=True)
                    for i in osv_50_merged.items()}


# In[ ]:


for i in osv_50_51_merged.values():
    i['Источник'] = i.apply(lambda row: 'Касса'
                            if pd.notna(row['Статья движения денежных средств'])
                            else 'Банк',
                            axis=1)


# In[ ]:


for i in osv_50_51_merged.items():
    print(f'{i[0]}:')
    print(i[1].info(show_counts=True))
    print()


# In[ ]:


'''
Цикл, который перезаписывает каждый файл из 'osv_50_51_merged', чтобы сохранить внесённые ранее изменения в его структуру
'''
for i in osv_50_51_merged.items():
    i[1].to_csv(os.path.join(source, i[0].replace('xlsx', 'csv')),
                header=True,
                index=False)


# In[ ]:


# путь для сохранения исходных данных
path_to_save = pathlib.Path(os.getcwd().replace('Scripts', 'Data'))


# In[ ]:


# создадим словарь с расположением новых папок по группам компаний
folder_location = {i: os.path.join(path_to_save) for i in osv_50_51_merged.keys()}


# In[ ]:


# создаются только те папки, которые до этого не существовали
for i in folder_location.items():
    if not os.path.exists(i[1]):
        os.makedirs(i[1])


# In[ ]:


# цикл, который копирует каждый файл из 'osv_50_51_merged' в новую папку
for i in folder_location.items():
    shutil.copy(os.path.join(source, i[0].replace('xlsx', 'csv')), i[1])
    # удалим исходные данные с расширением .csv из первоначальной папки
    os.remove(os.path.join(source, i[0].replace('xlsx', 'csv')))


# In[ ]:


# удалим исходные данные с расширением .xlsx из первоначальной папки
for i in source_osv_name:
    os.remove(os.path.join(source, i))

