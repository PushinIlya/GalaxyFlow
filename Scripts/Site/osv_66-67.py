#!/usr/bin/env python
# coding: utf-8

# ## Обработка ОСВ по счетам 66 и 67 "Кредиты и займы"

# In[ ]:


'''
Скрипт для преобразования исходных данных из 1C
'''
# импортируем библиотеки
import pandas as pd
import numpy as np
import os
import re
import shutil
import pathlib
from openpyxl import load_workbook
from openpyxl.worksheet.table import Table, TableStyleInfo
from datetime import datetime, timedelta


# In[ ]:


date_formats = ['%d.%m.%Y %H:%M:%S', '%m/%d/%Y %I:%M:%S %p'] # форматы дат
org_formats = ['ИП', 'ООО', 'ОДО', 'АО', 'ЗАО', 'ПАО', 'НПАО',
               'ХТ', 'ХО', 'ПК', 'ПТ ', 'КТ', 'СПК', 'ТОО',
               'Колхоз', 'Коопхоз', 'ФГП', 'ФГУП', 'ОГУП',
               'КГУП', 'РГУП', 'МУП'] # организационно-правовые формы

# функция для проверки формата строки
def check_format(row):
    for f in date_formats:
        try:
            pd.to_datetime(row, format=f)
            return row
        except ValueError:
            pass
    return np.nan


# In[ ]:


# инвертированная функция для проверки формата строки
def check_format_inv(row):
    for f in date_formats:
        try:
            pd.to_datetime(row, format=f)
            return np.nan
        except ValueError:
            pass
    return row


# In[ ]:


# функция для определения организации
def check_format_org(row):
    try:
        for i in org_formats:
            if i in row:
                return row
        return np.nan
    except TypeError:
        return np.nan


# In[ ]:


# запишем путь к папке с исходными данными в переменную
source = pathlib.Path(os.getcwd().replace('Fin_Flow', os.path.join('YandexDisk', 'Fin_Flow')).replace('Scripts', 'Source data'))


# In[ ]:


# создадим списки с названиями всех необходимых файлов в папке с исходными данными
source_osv_name = [raw_data for raw_data in os.listdir(source) if 'ОСВ 66-67' in raw_data and
                   raw_data.endswith('.xlsx')]

print(source_osv_name)


# In[ ]:


osv_66_67 = {raw_data: pd.read_excel(os.path.join(source, f'{raw_data}')) for raw_data in source_osv_name}


# In[ ]:


osv_66_67 = {i[0]: i[1].drop([0,1,2,3,4,5]) for i in osv_66_67.items()}


# In[ ]:


osv_66_67 = {i[0]: i[1].drop(columns=['Unnamed: 3', 'Unnamed: 7']) for i in osv_66_67.items()}


# In[ ]:


for i in osv_66_67.values():
    i['Дата'] = i['Счет'].apply(check_format)


# In[ ]:


for i in osv_66_67.values():
    i['Организация (доп)'] = i['Счет'].apply(check_format_inv)


# In[ ]:


for i in osv_66_67.values():
    i['Счет'] = i['Организация (доп)']


# In[ ]:


for i in osv_66_67.values():
    i['Организация (доп)'] = i['Счет'].apply(check_format_org)


# In[ ]:


for i in osv_66_67.values():
    i.columns = ['Счет',
                 'Кредитор',
                 'Начальный остаток',
                 'Списание',
                 'Поступление',
                 'Конечный остаток',
                 'Дата',
                 'Организация']


# In[ ]:


# словарь с именами столбцов и типами данных
type_of_columns = {'Счет': 'object',
                   'Кредитор': 'object',
                   'Начальный остаток': 'float64', 
                   'Списание': 'float64',
                   'Поступление': 'float64',
                   'Конечный остаток': 'float64',
                   'Организация': 'object'}


# In[ ]:


# подготовим строки для преобразования их в числовые значения
for i in osv_66_67.values():
    i['Начальный остаток'] = i['Начальный остаток'].apply(lambda row: str(row).replace(',', '.').replace(' ', '')
                                                          if pd.notna(row)
                                                          else row)
    i['Поступление'] = i['Поступление'].apply(lambda row: str(row).replace(',', '.').replace(' ', '')
                                              if pd.notna(row)
                                              else row)
    i['Списание'] = i['Списание'].apply(lambda row: str(row).replace(',', '.').replace(' ', '')
                                        if pd.notna(row)
                                        else row)
    i['Конечный остаток'] = i['Конечный остаток'].apply(lambda row: str(row).replace(',', '.').replace(' ', '')
                                                        if pd.notna(row)
                                                        else row)


# In[ ]:


# изменим тип данных в каждом датафрейме в соответствии со словарём 'type_of_columns'
osv_66_67 = {i[0]: i[1].astype(type_of_columns) for i in osv_66_67.items()}


# In[ ]:


# добавим ИНН для кредиторов
for i in osv_66_67.values():
    i['Кредитор'] = i.apply(lambda row: f'{row["Кредитор"]} (ИНН: {row["Счет"]})'
                            if re.match(r"^\d{10}$|^\d{12}$", str(row['Счет']))
                            else row['Кредитор'], axis=1) 


# In[ ]:


# добавим договоры для кредиторов
for i in osv_66_67.values():
    i['Договор'] = i.apply(lambda row: row['Счет']
                           if re.match(r"^(?!^\d{10}$|^\d{12}$)", str(row['Счет'])) and re.match(r"^(?!^[0-9]{2}\.[0-9]{2}$)", str(row['Счет']))
                           else np.nan, axis=1) 


# In[ ]:


# удалим ИНН и договоры из столбца "Счет"
for i in osv_66_67.values():
    i['Счет'] = i['Счет'].apply(lambda row: row
                                if re.match(r"^[0-9]{2}\.[0-9]{2}$", str(row))
                                else np.nan) 


# In[ ]:


# заполним пропущенные значения в столбцах значениями предыдущей заполненной ячейки по строкам
for i in osv_66_67.values():
    i['Счет'] = i['Счет'].fillna(method='ffill', axis=0)
    i['Организация'] = i.groupby('Счет')['Организация'].fillna(method='ffill', axis=0)
    i['Кредитор'] = i.groupby('Счет')['Кредитор'].fillna(method='ffill', axis=0)
    i['Договор'] = i.groupby(['Счет', 'Кредитор'])['Договор'].fillna(method='ffill', axis=0)


# In[ ]:


for i in osv_66_67.values():
    i.dropna(subset=['Дата'], inplace=True)


# In[ ]:


# преобразуем строки в даты, используя один из форматов
for i in osv_66_67.values():
    i['Дата'] = i['Дата'].apply(lambda row: pd.to_datetime(row, format=next((f for f in date_formats if pd.to_datetime(row, format=f, errors='coerce') is not pd.NaT), None)))


# In[ ]:


# исключим пустые датафреймы
osv_66_67 = {i[0]: i[1] for i in osv_66_67.items() if not i[1].empty}


# In[ ]:


for i in osv_66_67.values():
    i['Кредитор'] = i['Кредитор'].fillna('-')


# In[ ]:


for i in osv_66_67.values():
    i['Тип задолженности'] = i.apply(lambda row: 'Кредиты'
                                     if '.01' in str(row['Счет']) or '.02' in str(row['Счет'])
                                     else ('Займы' if '.03' in str(row['Счет']) or '.04' in str(row['Счет']) else 'Долговые ценные бумаги'),
                                     axis=1)


# In[ ]:


for i in osv_66_67.values():
    i['Структура задолженности'] = i.apply(lambda row: 'Основной долг'
                                           if '.01' in str(row['Счет']) or '.03' in str(row['Счет']) or '.05' in str(row['Счет'])
                                           else 'Проценты',
                                           axis=1)


# In[ ]:


for i in osv_66_67.values():
    i['Срочность задолженности'] = i.apply(lambda row: 'Долгосрочная задолженность'
                                           if '67.' in str(row['Счет'])
                                           else 'Краткосрочная задолженность',
                                           axis=1)


# In[ ]:


# сгруппируем счета
osv_66_67 = {i[0]: i[1].groupby(['Организация',
                                 'Кредитор',
                                 'Договор',
                                 'Дата',
                                 'Тип задолженности',
                                 'Структура задолженности',
                                 'Срочность задолженности'], as_index=False)[['Начальный остаток',
                                                                              'Поступление',
                                                                              'Списание',
                                                                              'Конечный остаток']].sum() for i in osv_66_67.items()}


# In[ ]:


for i in osv_66_67.values():
    i['Начальный остаток'] = i['Начальный остаток'].fillna(0)
    i['Поступление'] = i['Поступление'].fillna(0)
    i['Списание'] = i['Списание'].fillna(0)
    i['Конечный остаток'] = i['Конечный остаток'].fillna(0)


# In[ ]:


# создадим словарь, где ключ - название группы компаний, а значение - список таблиц по всем компаниям группы
osv_66_67_groups = {}

for key, value in osv_66_67.items():
    group = key.split('(')[1].split(')')[0]
    if group in osv_66_67_groups:
        osv_66_67_groups[group].append(value)
    else:
        osv_66_67_groups[group] = [value]


# In[ ]:


# создадим новый словарь с объединёнными таблицами
osv_66_67_merged = {f'ОСВ 66_67_{i[0]}.xlsx': pd.concat(i[1], axis=0).reset_index(inplace=False, drop=True)
                    for i in osv_66_67_groups.items()}


# In[ ]:


# определим последнюю дату по всем датафреймам группы компаний, чтобы продлить датафреймы с более ранними последними датами до самой поздней даты
last_date = {i[0]: i[1].sort_values(by='Дата')['Дата'].iloc[-1] for i in osv_66_67_merged.items()}


# In[ ]:


# создадим словарь со строками на последнюю дату из датафреймов, которые нужно продлить
osv_66_67_new_rows = {i[0]: i[1][i[1]['Дата'] == i[1]['Дата'].max()].copy() for i in osv_66_67.items()}


# In[ ]:


# очистим датафреймы, которые не нужно продлять
osv_66_67_new_rows = {i[0]: i[1].iloc[0:0] if i[1]['Дата'].max() == last_date[f'ОСВ 66_67_{i[0].split("(")[1].split(")")[0]}.xlsx'] else i[1] for i in osv_66_67_new_rows.items()}


# In[ ]:


# изменим добавляемые строки
for i in osv_66_67_new_rows.items():
    i[1]['Поступление'] = 0
    i[1]['Списание'] = 0
    i[1]['Дата'] = last_date[f'ОСВ 66_67_{i[0].split("(")[1].split(")")[0]}.xlsx']


# In[ ]:


# добавим новые строки в датафреймы
osv_66_67 = {i[0]: pd.concat([i[1], osv_66_67_new_rows[i[0]]]) for i in osv_66_67.items()}


# In[ ]:


# создадим столбцы для добавления новых данных по остаткам задолженности в договоре
for i in osv_66_67.values():
    i['Разница'] = i.groupby(['Организация',
                              'Кредитор',
                              'Договор',
                              'Тип задолженности',
                              'Структура задолженности',
                              'Срочность задолженности'])['Дата'].diff()


# In[ ]:


# добавим промежуточные значения по остаткам задолженности в договоре, используя диапазоны дат
for i in osv_66_67.values():
    i['Начальная дата'] = i['Дата'] - i['Разница'] + timedelta(days=1)
    i['Конечная дата'] = i['Дата']
    i['Дата'] = i.apply(lambda row: pd.date_range(start=row['Начальная дата'],
                                                  end=row['Конечная дата']).tolist()
                        if pd.notna(row['Начальная дата'])
                        else row['Дата'],
                        axis=1)


# In[ ]:


# добавим новые строки с датами
osv_66_67 = {i[0]: i[1].explode('Дата') for i in osv_66_67.items()} 


# In[ ]:


for i in osv_66_67.values():
    i['Поступление'] = i.apply(lambda row: 0 if row['Дата'] != row['Конечная дата'] else row['Поступление'],
                               axis=1)
    i['Списание'] = i.apply(lambda row: 0 if row['Дата'] != row['Конечная дата'] else row['Списание'],
                            axis=1)


# In[ ]:


osv_66_67 = {i[0]: i[1].drop(columns=['Разница',
                                      'Начальная дата',
                                      'Конечная дата'])
             for i in osv_66_67.items()}


# In[ ]:


# создадим уникальный идентификатор для каждого договора
for i in osv_66_67.values():
    i['Уникальный номер'] = i.apply(lambda row: f"{row['Кредитор']} [-+-] {row['Договор']} [-+-] {row['Тип задолженности']} [-+-] {row['Структура задолженности']} [-+-] {row['Срочность задолженности']}",
                                    axis=1)


# In[ ]:


# создадим словарь для добавления промежуточных значений по остаткам у кредиторов, используя данные по остаткам на дату накопительным итогом
# сгруппируем до уровня дат и получим уникальные значения уникальных номеров для каждой даты
osv_66_67_values = {i[0]: i[1].groupby(['Организация',
                                        'Дата'], as_index=False)['Уникальный номер'].unique() for i in osv_66_67.items()} 


# In[ ]:


# отсортируем датафреймы по дате
osv_66_67_values = {i[0]: i[1].groupby(['Организация'], as_index=False).apply(lambda row: row.sort_values('Дата')) for i in osv_66_67_values.items()} 


# In[ ]:


# присвоим индексы
osv_66_67_values = {i[0]: i[1].groupby(['Организация'], as_index=False).apply(lambda row: row.reset_index(drop=True)) for i in osv_66_67_values.items()}


# In[ ]:


# добавим на каждую дату список из списков уникальных значений уникальных номеров накопительным итогом
grouped_dict = {}
for key, value in osv_66_67_values.items():
    group_list = []
    for name, group in value.groupby(level=0):
        group['Уникальный номер (доп)'] = group.index.map(lambda row: group.loc[:row, 'Уникальный номер'].tolist())
        group['Уникальный номер (доп)'] = group['Уникальный номер (доп)'].apply(lambda row: set([x for sublist in row for x in sublist]))
        group_list.append(group)
    table = pd.concat(group_list, axis=0)
    grouped_dict[key] = table


# In[ ]:


osv_66_67_values = grouped_dict


# In[ ]:


# добавим новые строки с уникальными номерами
osv_66_67_values = {i[0]: i[1].explode('Уникальный номер (доп)') for i in osv_66_67_values.items()} 


# In[ ]:


for i in osv_66_67_values.values():
    i['Уникальный номер'] = i['Уникальный номер (доп)']
    i['Начальный остаток'] = 0.0
    i['Поступление'] = 0.0
    i['Списание'] = 0.0
    i['Конечный остаток'] = 0.0


# In[ ]:


# расшифруем уникальный номер 
for i in osv_66_67_values.values():
    i['Кредитор'] = i['Уникальный номер'].apply(lambda row: row.split(' [-+-] ')[0])
    i['Договор'] = i['Уникальный номер'].apply(lambda row: row.split(' [-+-] ')[1])
    i['Тип задолженности'] = i['Уникальный номер'].apply(lambda row: row.split(' [-+-] ')[2])
    i['Структура задолженности'] = i['Уникальный номер'].apply(lambda row: row.split(' [-+-] ')[3])
    i['Срочность задолженности'] = i['Уникальный номер'].apply(lambda row: row.split(' [-+-] ')[4])


# In[ ]:


osv_66_67_values = {i[0]: i[1].drop(columns=['Уникальный номер', 'Уникальный номер (доп)'])
                    for i in osv_66_67_values.items()}


# In[ ]:


osv_66_67 = {i[0]: pd.concat([i[1], osv_66_67_values[i[0]]]) for i in osv_66_67.items()}


# In[ ]:


osv_66_67 = {i[0]: i[1].sort_values(by='Дата').groupby(['Организация',
                                                        'Кредитор',
                                                        'Договор',
                                                        'Дата',
                                                        'Тип задолженности',
                                                        'Структура задолженности',
                                                        'Срочность задолженности'],
                                                       as_index=False)[['Начальный остаток',
                                                                        'Поступление',
                                                                        'Списание',
                                                                        'Конечный остаток']].sum() for i in osv_66_67.items()}


# In[ ]:


for i in osv_66_67.values():
    i['Начальный остаток'] = 0.0
    i['Конечный остаток'] = 0.0
    i['Изменение'] = i['Поступление'] - i['Списание']


# In[ ]:


# создадим новые таблицы для расчёта остатков
osv_66_67_grouped = {i[0]: i[1].sort_values(by='Дата').groupby(['Организация',
                                                                'Кредитор',
                                                                'Договор',
                                                                'Дата',
                                                                'Тип задолженности',
                                                                'Структура задолженности',
                                                                'Срочность задолженности'],
                                                               as_index=False)['Изменение'].sum() for i in osv_66_67.items()}


# In[ ]:


grouped_dict = {}
for key, value in osv_66_67_grouped.items():
    # применение операций shift и cumsum к каждой организации отдельно
    group_list = []
    for name, group in value.groupby(['Организация',
                                      'Кредитор',
                                      'Договор',
                                      'Тип задолженности',
                                      'Структура задолженности',
                                      'Срочность задолженности']):
        group['Начальный остаток'] = group['Изменение'].shift(fill_value=0).cumsum()
        group['Конечный остаток'] = group['Начальный остаток'] + group['Изменение']
        group_list.append(group)
    table = pd.concat(group_list, axis=0)
    grouped_dict[key] = table


# In[ ]:


osv_66_67_grouped = grouped_dict


# In[ ]:


merged_dict = {}
# объединим словарь с таблицами для расчёта начального и конечного остатков
for key, table1 in osv_66_67.items():
    table2 = osv_66_67_grouped.get(key)
    merged_table = pd.merge(table1, table2, on=['Организация',
                                                'Кредитор',
                                                'Договор',
                                                'Дата',
                                                'Тип задолженности',
                                                'Структура задолженности',
                                                'Срочность задолженности'],
                            how='left',
                            suffixes=('', '_new'))
    merged_dict[key] = merged_table


# In[ ]:


osv_66_67 = merged_dict


# In[ ]:


for i in osv_66_67.values():
    i['Начальный остаток'] = i['Начальный остаток_new']
    i['Конечный остаток'] = i['Конечный остаток_new']


# In[ ]:


# удалим лишние столбцы
osv_66_67 = {i[0]: i[1].iloc[:, :-3] for i in osv_66_67.items()}


# In[ ]:


osv_66_67 = {i[0]: i[1].reset_index(drop=True) for i in osv_66_67.items()}


# In[ ]:


# создадим словарь, где ключ - название группы компаний, а значение - список таблиц по всем компаниям группы
osv_66_67_groups = {}

for key, value in osv_66_67.items():
    group = key.split('(')[1].split(')')[0]
    if group in osv_66_67_groups:
        osv_66_67_groups[group].append(value)
    else:
        osv_66_67_groups[group] = [value]


# In[ ]:


# создадим новый словарь с объединёнными таблицами
osv_66_67_merged = {f'ОСВ 66_67_{i[0]}.xlsx': pd.concat(i[1], axis=0).reset_index(inplace=False, drop=True)
                    for i in osv_66_67_groups.items()}


# In[ ]:


for i in osv_66_67_merged.items():
    print(f'{i[0]}:')
    print(i[1].info(show_counts=True))
    print()


# In[ ]:


'''
Цикл, который перезаписывает каждый файл из 'osv_66_67_merged', чтобы сохранить внесённые ранее изменения в его структуру
'''
for i in osv_66_67_merged.items():
    i[1].to_csv(os.path.join(source, i[0].replace('xlsx', 'csv')),
                header=True,
                index=False)


# In[ ]:


# путь для сохранения исходных данных
path_to_save = pathlib.Path(os.getcwd().replace('Scripts', 'Data'))


# In[ ]:


# создадим словарь с расположением новых папок по группам компаний
folder_location = {i: os.path.join(path_to_save) for i in osv_66_67_merged.keys()}


# In[ ]:


# создаются только те папки, которые до этого не существовали
for i in folder_location.items():
    if not os.path.exists(i[1]):
        os.makedirs(i[1])


# In[ ]:


# цикл, который копирует каждый файл из 'osv_66_67_merged' в новую папку
for i in folder_location.items():
    shutil.copy(os.path.join(source, i[0].replace('xlsx', 'csv')), i[1])
    # удалим исходные данные с расширением .csv из первоначальной папки
    os.remove(os.path.join(source, i[0].replace('xlsx', 'csv')))


# In[ ]:


# удалим исходные данные с расширением .xlsx из первоначальной папки
for i in source_osv_name:
    os.remove(os.path.join(source, i))

