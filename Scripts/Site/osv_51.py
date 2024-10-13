#!/usr/bin/env python
# coding: utf-8

# ## Обработка ОСВ по счету 51 "Расчетные счета"

# In[ ]:


'''
Скрипт для преобразования исходных данных из 1C
'''
# импортируем библиотеки
import pandas as pd
import numpy as np
import os
import shutil
import pathlib
from openpyxl import load_workbook
from openpyxl.worksheet.table import Table, TableStyleInfo
from datetime import datetime, timedelta


# In[ ]:


date_formats = ['%d.%m.%Y %H:%M:%S', '%m/%d/%Y %I:%M:%S %p'] # форматы дат

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


# функция для определения счёта
def check_format_digit(row):
    try:
        if row[0].isdigit() is True:
            return row
        else:
            return np.nan
    except TypeError:
        return np.nan


# In[ ]:


# функция для определения организации
def check_format_digit_inv(row):
    try:
        if row[0].isdigit() is False:
            return row
        else:
            return np.nan
    except TypeError:
        return np.nan


# In[ ]:


# запишем путь к папке с исходными данными в переменную
source = pathlib.Path(os.getcwd().replace('Fin_Flow', os.path.join('YandexDisk', 'Fin_Flow')).replace('Scripts', 'Source data'))


# In[ ]:


# создадим списки с названиями всех необходимых файлов в папке с исходными данными
source_osv_name = [raw_data for raw_data in os.listdir(source) if 'ОСВ 50-51' in raw_data and
                   raw_data.endswith('.xlsx')]

print(source_osv_name)


# In[ ]:


osv_51 = {raw_data: pd.read_excel(os.path.join(source, f'{raw_data}')) for raw_data in source_osv_name}


# In[ ]:


osv_51 = {i[0]: i[1].drop([0,1,2,3]) for i in osv_51.items()}


# In[ ]:


osv_51 = {i[0]: i[1].drop(columns=['Unnamed: 3', 'Unnamed: 7']) for i in osv_51.items()}


# In[ ]:


for i in osv_51.values():
    i['Дата'] = i['Счет'].apply(check_format)


# In[ ]:


for i in osv_51.values():
    i['Организация (доп)'] = i['Счет'].apply(check_format_inv)


# In[ ]:


for i in osv_51.values():
    i['Счет'] = i['Организация (доп)']


# In[ ]:


for i in osv_51.values():
    i['Организация (доп)'] = i['Счет'].apply(check_format_digit_inv)


# In[ ]:


for i in osv_51.values():
    i['Счет'] = i['Счет'].apply(check_format_digit)


# In[ ]:


for i in osv_51.values():
    i.columns = ['Счет',
                 'Банковский счет',
                 'Начальный остаток',
                 'Поступление',
                 'Списание',
                 'Конечный остаток',
                 'Дата',
                 'Организация']


# In[ ]:


# словарь с именами столбцов и типами данных
type_of_columns = {'Счет': 'object',
                   'Банковский счет': 'object',
                   'Начальный остаток': 'float64', 
                   'Поступление': 'float64',
                   'Списание': 'float64',
                   'Конечный остаток': 'float64',
                   'Организация': 'object'}


# In[ ]:


# подготовим строки для преобразования их в числовые значения
for i in osv_51.values():
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
osv_51 = {i[0]: i[1].astype(type_of_columns) for i in osv_51.items()}


# In[ ]:


# заполним пропущенные значения в столбцах значениями предыдущей заполненной ячейки по строкам
for i in osv_51.values():
    i['Счет'] = i['Счет'].fillna(method='ffill', axis=0)
    i['Организация'] = i.groupby('Счет')['Организация'].fillna(method='ffill', axis=0)
    i['Банковский счет'] = i.groupby('Счет')['Банковский счет'].fillna(method='ffill', axis=0)


# In[ ]:


for i in osv_51.values():
    i.dropna(subset=['Дата'], inplace=True)


# In[ ]:


# преобразуем строки в даты, используя один из форматов
for i in osv_51.values():
    i['Дата'] = i['Дата'].apply(lambda row: pd.to_datetime(row, format=next((f for f in date_formats if pd.to_datetime(row, format=f, errors='coerce') is not pd.NaT), None)))


# In[ ]:


# оставим только строки со счётом 51
osv_51 = {i[0]: i[1][i[1]['Счет'] == '51'] for i in osv_51.items()}


# In[ ]:


# исключим пустые датафреймы
osv_51 = {i[0]: i[1] for i in osv_51.items() if not i[1].empty}


# In[ ]:


for i in osv_51.values():
    i['Банковский счет'] = i['Банковский счет'].fillna('-')


# In[ ]:


# сгруппируем счета
osv_51 = {i[0]: i[1].groupby(['Организация',
                              'Банковский счет',
                              'Дата'], as_index=False)[['Начальный остаток',
                                                        'Поступление',
                                                        'Списание',
                                                        'Конечный остаток']].sum() for i in osv_51.items()}


# In[ ]:


for i in osv_51.values():
    i['Начальный остаток'] = i['Начальный остаток'].fillna(0)
    i['Поступление'] = i['Поступление'].fillna(0)
    i['Списание'] = i['Списание'].fillna(0)
    i['Конечный остаток'] = i['Конечный остаток'].fillna(0)


# In[ ]:


# создадим словарь, где ключ - название группы компаний, а значение - список таблиц по всем компаниям группы
osv_51_groups = {}

for key, value in osv_51.items():
    group = key.split('(')[1].split(')')[0]
    if group in osv_51_groups:
        osv_51_groups[group].append(value)
    else:
        osv_51_groups[group] = [value]


# In[ ]:


# создадим новый словарь с объединёнными таблицами
osv_51_merged = {f'ОСВ 51_{i[0]}.xlsx': pd.concat(i[1], axis=0).reset_index(inplace=False, drop=True)
                 for i in osv_51_groups.items()}


# In[ ]:


# определим последнюю дату по всем датафреймам группы компаний, чтобы продлить датафреймы с более ранними последними датами до самой поздней даты
last_date = {i[0]: i[1].sort_values(by='Дата')['Дата'].iloc[-1] for i in osv_51_merged.items()}


# In[ ]:


# создадим словарь со строками на последнюю дату из датафреймов, которые нужно продлить
osv_51_new_rows = {i[0]: i[1][i[1]['Дата'] == i[1]['Дата'].max()].copy() for i in osv_51.items()}


# In[ ]:


# очистим датафреймы, которые не нужно продлять
osv_51_new_rows = {i[0]: i[1].iloc[0:0] if i[1]['Дата'].max() == last_date[f'ОСВ 51_{i[0].split("(")[1].split(")")[0]}.xlsx'] else i[1] for i in osv_51_new_rows.items()}


# In[ ]:


# изменим добавляемые строки
for i in osv_51_new_rows.items():
    i[1]['Поступление'] = 0
    i[1]['Списание'] = 0
    i[1]['Дата'] = last_date[f'ОСВ 51_{i[0].split("(")[1].split(")")[0]}.xlsx']


# In[ ]:


# добавим новые строки в датафреймы
osv_51 = {i[0]: pd.concat([i[1], osv_51_new_rows[i[0]]]) for i in osv_51.items()}


# In[ ]:


# создадим столбцы для добавления новых данных по остаткам на расчётных счетах
for i in osv_51.values():
    i['Разница'] = i.groupby(['Организация', 'Банковский счет'])['Дата'].diff()


# In[ ]:


# добавим промежуточные значения по остаткам на банковских счетах, используя диапазоны дат
for i in osv_51.values():
    i['Начальная дата'] = i['Дата'] - i['Разница'] + timedelta(days=1)
    i['Конечная дата'] = i['Дата']
    i['Дата'] = i.apply(lambda row: pd.date_range(start=row['Начальная дата'],
                                                  end=row['Конечная дата']).tolist()
                        if pd.notna(row['Начальная дата'])
                        else row['Дата'],
                        axis=1)


# In[ ]:


# добавим новые строки с датами
osv_51 = {i[0]: i[1].explode('Дата') for i in osv_51.items()} 


# In[ ]:


for i in osv_51.values():
    i['Поступление'] = i.apply(lambda row: 0 if row['Дата'] != row['Конечная дата'] else row['Поступление'],
                               axis=1)
    i['Списание'] = i.apply(lambda row: 0 if row['Дата'] != row['Конечная дата'] else row['Списание'],
                            axis=1)


# In[ ]:


osv_51 = {i[0]: i[1].drop(columns=['Разница',
                                   'Начальная дата',
                                   'Конечная дата'])
          for i in osv_51.items()}


# In[ ]:


# создадим словарь для добавления промежуточных значений по остаткам на банковских счетах, используя данные по остаткам на дату накопительным итогом
# сгруппируем до уровня дат и получим уникальные значения банковских счетов для каждой даты
osv_51_values = {i[0]: i[1].groupby(['Организация', 'Дата'], as_index=False)['Банковский счет'].unique() for i in osv_51.items()} 


# In[ ]:


# отсортируем датафреймы по дате
osv_51_values = {i[0]: i[1].groupby(['Организация'], as_index=False).apply(lambda row: row.sort_values('Дата')) for i in osv_51_values.items()} 


# In[ ]:


# присвоим индексы
osv_51_values = {i[0]: i[1].groupby(['Организация'], as_index=False).apply(lambda row: row.reset_index(drop=True)) for i in osv_51_values.items()}


# In[ ]:


# добавим на каждую дату список из списков уникальных банковских счетов накопительным итогом
grouped_dict = {}
for key, value in osv_51_values.items():
    group_list = []
    for name, group in value.groupby(level=0):
        group['Банковский счет (доп)'] = group.index.map(lambda row: group.loc[:row, 'Банковский счет'].tolist())
        group['Банковский счет (доп)'] = group['Банковский счет (доп)'].apply(lambda row: set([x for sublist in row for x in sublist]))
        group_list.append(group)
    table = pd.concat(group_list, axis=0)
    grouped_dict[key] = table


# In[ ]:


osv_51_values = grouped_dict


# In[ ]:


# добавим новые строки с банковскими счетами
osv_51_values = {i[0]: i[1].explode('Банковский счет (доп)') for i in osv_51_values.items()} 


# In[ ]:


for i in osv_51_values.values():
    i['Банковский счет'] = i['Банковский счет (доп)']
    i['Начальный остаток'] = 0.0
    i['Поступление'] = 0.0
    i['Списание'] = 0.0
    i['Конечный остаток'] = 0.0


# In[ ]:


osv_51_values = {i[0]: i[1].drop(columns=['Банковский счет (доп)'])
                 for i in osv_51_values.items()}


# In[ ]:


osv_51 = {i[0]: pd.concat([i[1], osv_51_values[i[0]]]) for i in osv_51.items()}


# In[ ]:


osv_51 = {i[0]: i[1].sort_values(by='Дата').groupby(['Организация',
                                                     'Банковский счет',
                                                     'Дата'],
                                                    as_index=False)[['Начальный остаток',
                                                                     'Поступление',
                                                                     'Списание',
                                                                     'Конечный остаток']].sum() for i in osv_51.items()}


# In[ ]:


for i in osv_51.values():
    i['Начальный остаток'] = 0.0
    i['Конечный остаток'] = 0.0
    i['Изменение'] = i['Поступление'] - i['Списание']


# In[ ]:


# создадим новые таблицы для расчёта остатков
osv_51_grouped = {i[0]: i[1].sort_values(by='Дата').groupby(['Организация',
                                                             'Банковский счет',
                                                             'Дата'], as_index=False)['Изменение'].sum() for i in osv_51.items()}


# In[ ]:


grouped_dict = {}
for key, value in osv_51_grouped.items():
    # применение операций shift и cumsum к каждой организации отдельно
    group_list = []
    for name, group in value.groupby(['Организация', 'Банковский счет']):
        group['Начальный остаток'] = group['Изменение'].shift(fill_value=0).cumsum()
        group['Конечный остаток'] = group['Начальный остаток'] + group['Изменение']
        group_list.append(group)
    table = pd.concat(group_list, axis=0)
    grouped_dict[key] = table


# In[ ]:


osv_51_grouped = grouped_dict


# In[ ]:


merged_dict = {}
# объединим словарь с таблицами для расчёта начального и конечного остатков
for key, table1 in osv_51.items():
    table2 = osv_51_grouped.get(key)
    merged_table = pd.merge(table1, table2, on=['Организация',
                                                'Банковский счет',
                                                'Дата'],
                            how='left',
                            suffixes=('', '_new'))
    merged_dict[key] = merged_table


# In[ ]:


osv_51 = merged_dict


# In[ ]:


for i in osv_51.values():
    i['Начальный остаток'] = i['Начальный остаток_new']
    i['Конечный остаток'] = i['Конечный остаток_new']


# In[ ]:


# удалим лишние столбцы
osv_51 = {i[0]: i[1].iloc[:, :-3] for i in osv_51.items()}


# In[ ]:


osv_51 = {i[0]: i[1].reset_index(drop=True) for i in osv_51.items()}


# In[ ]:


# создадим словарь, где ключ - название группы компаний, а значение - список таблиц по всем компаниям группы
osv_51_groups = {}

for key, value in osv_51.items():
    group = key.split('(')[1].split(')')[0]
    if group in osv_51_groups:
        osv_51_groups[group].append(value)
    else:
        osv_51_groups[group] = [value]


# In[ ]:


# создадим новый словарь с объединёнными таблицами
osv_51_merged = {f'ОСВ 51_{i[0]}.xlsx': pd.concat(i[1], axis=0).reset_index(inplace=False, drop=True)
                 for i in osv_51_groups.items()}


# In[ ]:


for i in osv_51_merged.items():
    print(f'{i[0]}:')
    print(i[1].info(show_counts=True))
    print()


# In[ ]:


'''
Цикл, который перезаписывает каждый файл из 'osv_51_merged', чтобы сохранить внесённые ранее изменения в его структуру
'''
for i in osv_51_merged.items():
    i[1].to_csv(os.path.join(source, i[0].replace('xlsx', 'csv')),
                header=True,
                index=False)


# In[ ]:


# путь для сохранения исходных данных
path_to_save = pathlib.Path(os.getcwd().replace('Scripts', 'Data'))


# In[ ]:


# создадим словарь с расположением новых папок по группам компаний
folder_location = {i: os.path.join(path_to_save) for i in osv_51_merged.keys()}


# In[ ]:


# создаются только те папки, которые до этого не существовали
for i in folder_location.items():
    if not os.path.exists(i[1]):
        os.makedirs(i[1])


# In[ ]:


# цикл, который копирует каждый файл из 'osv_51_merged' в новую папку
for i in folder_location.items():
    shutil.copy(os.path.join(source, i[0].replace('xlsx', 'csv')), i[1])
    # удалим исходные данные с расширением .csv из первоначальной папки
    os.remove(os.path.join(source, i[0].replace('xlsx', 'csv')))


# In[ ]:


# удалим исходные данные с расширением .xlsx из первоначальной папки
for i in source_osv_name:
    os.remove(os.path.join(source, i))

