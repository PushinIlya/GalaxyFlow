#!/usr/bin/env python
# coding: utf-8

# ## Обработка банковских выписок и ОСВ по счету 50 "Касса"

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


from osv_50 import osv_50


# In[ ]:


# запишем путь к папке с исходными данными в переменную
source_bank_statements = pathlib.Path(os.getcwd().replace('Fin_Flow', os.path.join('YandexDisk', 'Fin_Flow')).replace('Scripts', 'Source data'))
source_cash_flow_dict = pathlib.Path(os.getcwd().replace('Fin_Flow',os.path.join('YandexDisk', 'Fin_Flow')).replace('Scripts', 'Source data'),
                                     'Cash flow items 1C')


# In[ ]:


# создадим списки с названиями всех необходимых файлов в папке с исходными данными
bank_statements_name = [raw_data for raw_data in os.listdir(source_bank_statements) if 'Банковские выписки' in raw_data and
                        raw_data.endswith('.xlsx')]

cash_flow_dict_name = [raw_data for raw_data in os.listdir(source_cash_flow_dict) if 'Статьи движения денежных средств' in raw_data and
                       raw_data.endswith('.xlsx')]

print(bank_statements_name)
print(cash_flow_dict_name)


# In[ ]:


bank_statements = {raw_data: pd.read_excel(os.path.join(source_bank_statements, f'{raw_data}')) for raw_data in bank_statements_name}
cash_flow_dict = {raw_data: pd.read_excel(os.path.join(source_cash_flow_dict, f'{raw_data}')) for raw_data in cash_flow_dict_name}


# In[ ]:


# словарь с именами столбцов и типами данных
type_of_columns = {'Вид операции': 'object',
                   'Статья движения денежных средств': 'object',
                   'Вид движения': 'object', 
                   'Группа статей': 'object',
                   'Операция': 'object',}


# In[ ]:


# изменим тип данных в каждом датафрейме в соответствии со словарём 'type_of_columns'
cash_flow_dict = {i[0]: i[1].astype(type_of_columns) for i in cash_flow_dict.items()}


# In[ ]:


# словарь с именами столбцов и типами данных
type_of_columns = {'Поступление': 'float64',
                   'Списание': 'float64',
                   'Назначение платежа': 'object', 
                   'Контрагент': 'object',
                   'ИНН': 'object',
                   'Вид операции': 'object',
                   'Организация': 'object',
                   'Банковский счет': 'object'}


# In[ ]:


# подготовим строки для преобразования их в числовые значения
for i in bank_statements.values():
    i['Поступление'] = i['Поступление'].apply(lambda row: str(row).replace(',', '.').replace(' ', '')
                                              if pd.notna(row)
                                              else row)
    i['Списание'] = i['Списание'].apply(lambda row: str(row).replace(',', '.').replace(' ', '')
                                        if pd.notna(row)
                                        else row)
    i['ИНН'] = i['ИНН'].apply(lambda row: str(row).replace('.0', '')
                              if pd.notna(row)
                              else row)


# In[ ]:


# изменим тип данных в каждом датафрейме в соответствии со словарём 'type_of_columns'
bank_statements = {i[0]: i[1].astype(type_of_columns) for i in bank_statements.items()}


# In[ ]:


date_formats = ['%d.%m.%Y', '%m/%d/%Y'] # форматы дат
# преобразуем строки в даты, используя один из форматов
for i in bank_statements.values():
    i['Дата'] = i['Дата'].apply(lambda row: pd.to_datetime(row, format=next((f for f in date_formats if pd.to_datetime(row, format=f, errors='coerce') is not pd.NaT), None)))


# In[ ]:


# исключим пустые датафреймы
bank_statements = {i[0]: i[1] for i in bank_statements.items() if not i[1].empty}


# In[ ]:


for i in bank_statements.values():
    i['Контрагент'] = i.apply(lambda row: f"{row['Контрагент']} (ИНН: {row['ИНН']})"
                              if pd.notna(row['ИНН'])
                              else row['Контрагент'],
                              axis=1)


# In[ ]:


# оставим только нужные столбцы
bank_statements = {i[0]: i[1][['Дата',
                               'Поступление',
                               'Списание',
                               'Назначение платежа',
                               'Контрагент',
                               'Вид операции',
                               'Банковский счет',
                               'Организация']]  
                   for i in bank_statements.items()}


# In[ ]:


for i in bank_statements.values():
    i['Тип операции'] = i['Поступление'].apply(lambda row: 'Списание' if np.isnan(row) else 'Поступление')


# In[ ]:


# объединим банковские выписки со словарями
bank_statements = {i[0]: pd.concat([pd.merge(i[1].query('`Тип операции` == "Поступление"'),
                                             cash_flow_dict[f'({i[0].split("(")[1].split(")")[0]})Статьи движения денежных средств_{i[0].split("_")[-1].split(".")[0]}.xlsx'].query('Операция == "Поступление"'),
                                             on='Вид операции',
                                             how='left'),
                                    pd.merge(i[1].query('`Тип операции` == "Списание"'),
                                             cash_flow_dict[f'({i[0].split("(")[1].split(")")[0]})Статьи движения денежных средств_{i[0].split("_")[-1].split(".")[0]}.xlsx'].query('Операция == "Списание"'),
                                             on='Вид операции',
                                             how='left')])
                   for i in bank_statements.items()}


# In[ ]:


for i in bank_statements.values():
    i['Источник'] = 'Банк'


# In[ ]:


osv_50 = {i[0]: i[1].drop(columns=['Начальный остаток',
                                   'Конечный остаток',
                                   'Изменение'])
          for i in osv_50.items()}


# In[ ]:


# удалим лишние значения в кассе
osv_50 = {i[0]: i[1][(i[1]['Поступление'] != 0) | (i[1]['Списание'] != 0)]
          for i in osv_50.items()}


# In[ ]:


# создадим новый столбец, который будет содержать список из двух значений, в том случае если
# одновременно в столбцах "Поступление" и "Списание" есть значения
for i in osv_50.values():
    i['Поступление/Списание'] = i.apply(lambda row: ['Поступление', 'Списание'] if row['Поступление'] != 0 and row['Списание'] != 0 else '', axis=1)

# разделим строки, где "Тип операции" равен "Поступление/Списание"
osv_50 = {i[0]: i[1].explode('Поступление/Списание') for i in osv_50.items()}

for i in osv_50.values():
    i['Поступление'] = i.apply(lambda row: 0 if row['Поступление/Списание'] == 'Списание' else row['Поступление'], axis=1)
for i in osv_50.values():
    i['Списание'] = i.apply(lambda row: 0 if row['Поступление/Списание'] == 'Поступление' else row['Списание'], axis=1)


# In[ ]:


for i in osv_50.values():
    i['Тип операции'] = i.apply(lambda row: 'Поступление' if row['Поступление'] != 0 else 'Списание', axis=1)


# In[ ]:


# сгруппируем счета
osv_50 = {i[0]: i[1].groupby(['Организация',
                              'Тип операции',
                              'Статья движения денежных средств',
                              'Дата'], as_index=False)[['Поступление',
                                                        'Списание']].sum() for i in osv_50.items()}


# In[ ]:


# объединим кассу со словарями
osv_50 = {i[0]: pd.concat([pd.merge(i[1].query('`Тип операции` == "Поступление"'),
                                    cash_flow_dict[f'({i[0].split("(")[1].split(")")[0]})Статьи движения денежных средств_{i[0].split("_")[-1].split(".")[0]}.xlsx'].query('Операция == "Поступление"').drop_duplicates(subset='Статья движения денежных средств'),
                                    on='Статья движения денежных средств',
                                    how='left'),
                           pd.merge(i[1].query('`Тип операции` == "Списание"'),
                                    cash_flow_dict[f'({i[0].split("(")[1].split(")")[0]})Статьи движения денежных средств_{i[0].split("_")[-1].split(".")[0]}.xlsx'].query('Операция == "Списание"').drop_duplicates(subset='Статья движения денежных средств'),
                                    on='Статья движения денежных средств',
                                    how='left')])
          for i in osv_50.items()}


# In[ ]:


for i in osv_50.values():
    i['Источник'] = 'Касса'


# In[ ]:


# объединим банковские выписки с кассой
cash_flow = {i[0]: pd.concat([bank_statements[i[0]],
                              osv_50[i[0].replace('Банковские выписки', 'ОСВ 50-51')]]).reset_index(drop=True)
             for i in bank_statements.items()}


# In[ ]:


for i in cash_flow.values():
    i['Поступление'] = i['Поступление'].fillna(0)
    i['Списание'] = i['Списание'].fillna(0)


# In[ ]:


for i in cash_flow.values():
    i['Сумма'] = i.apply(lambda row: row['Поступление'] + row['Списание'], axis=1)


# In[ ]:


for i in cash_flow.values():
    i['Денежный поток'] = i.apply(lambda row: row['Сумма'] if row['Тип операции'] == 'Поступление' else - row['Сумма'], axis=1)
    i['Денежный приток'] = i.apply(lambda row: row['Сумма'] if row['Тип операции'] == 'Поступление' else np.nan, axis=1)
    i['Денежный отток'] = i.apply(lambda row: row['Сумма'] if row['Тип операции'] == 'Списание' else np.nan, axis=1)


# In[ ]:


# в кассе отсутствует данные значения
for i in cash_flow.values():
    i['Назначение платежа'] = i.apply(lambda row: 'Касса' if row['Источник'] == 'Касса' else row['Назначение платежа'], axis=1)
    i['Контрагент'] = i.apply(lambda row: 'Касса' if row['Источник'] == 'Касса' else row['Контрагент'], axis=1)
    i['Вид операции'] = i.apply(lambda row: 'Касса' if row['Источник'] == 'Касса' else row['Вид операции'], axis=1)
    i['Банковский счет'] = i.apply(lambda row: 'Касса' if row['Источник'] == 'Касса' else row['Банковский счет'], axis=1)


# In[ ]:


for i in cash_flow.values():
    i['Назначение платежа'] = i['Назначение платежа'].fillna('-')
    i['Контрагент'] = i['Контрагент'].fillna('-')
    i['Вид операции'] = i['Вид операции'].fillna('-')
    i['Банковский счет'] = i['Банковский счет'].fillna('-')
    i['Статья движения денежных средств'] = i['Статья движения денежных средств'].fillna('-')
    i['Вид движения'] = i['Вид движения'].fillna('-')
    # если cash_flow_dict неактуален, то все неуказанные статьи движения денежных средств отнесём
    # к неденежным операциям
    i['Группа статей'] = i['Группа статей'].fillna('Неденежные операции')


# In[ ]:


# добавим столбец, где будет указано, что данный контрагент в конкретную дату встречается впервые
for i in cash_flow.values():
    min_date = i.groupby(['Организация', 'Контрагент'])['Дата'].idxmin()
    i['Новый контрагент'] = i['Дата'].eq(i.loc[min_date, 'Дата']).apply(lambda row: 'Да' if row else 'Нет')


# In[ ]:


for i in cash_flow.values():
    i['Группа статей'] = i['Группа статей'].replace({'Текущие операции': 'Денежные потоки от текущих операций',
                                                     'Инвестиционные операции': 'Денежные потоки от инвестиционных операций',
                                                     'Финансовые операции': 'Денежные потоки от финансовых операций'})


# In[ ]:


cash_flow = {i[0]: i[1].drop(columns=['Поступление',
                                      'Списание',
                                      'Операция',
                                      'Сумма'])
             for i in cash_flow.items()}


# In[ ]:


# создадим словарь, где ключ - название группы компаний, а значение - список таблиц по всем компаниям группы
cash_flow_groups = {}

for key, value in cash_flow.items():
    group = key.split('(')[1].split(')')[0]
    if group in cash_flow_groups:
        cash_flow_groups[group].append(value)
    else:
        cash_flow_groups[group] = [value]


# In[ ]:


# создадим новый словарь с объединёнными таблицами
cash_flow_merged = {f'ОДДС_{i[0]}.xlsx': pd.concat(i[1], axis=0).reset_index(inplace=False, drop=True)
                    for i in cash_flow_groups.items()}


# In[ ]:


for i in cash_flow_merged.items():
    print(f'{i[0]}:')
    print(i[1].info(show_counts=True))
    print()


# In[ ]:


'''
Цикл, который перезаписывает каждый файл из 'cash_flow_merged', чтобы сохранить внесённые ранее изменения в его структуру
'''
for i in cash_flow_merged.items():
    i[1].to_csv(os.path.join(source_bank_statements, i[0].replace('xlsx', 'csv')),
                header=True,
                index=False)


# In[ ]:


# путь для сохранения исходных данных
path_to_save = pathlib.Path(os.getcwd().replace('Scripts', 'Data'))


# In[ ]:


# создадим словарь с расположением новых папок по группам компаний
folder_location = {i: os.path.join(path_to_save) for i in cash_flow_merged.keys()}


# In[ ]:


# создаются только те папки, которые до этого не существовали
for i in folder_location.items():
    if not os.path.exists(i[1]):
        os.makedirs(i[1])


# In[ ]:


# цикл, который копирует каждый файл из 'cash_flow_merged' в новую папку
for i in folder_location.items():
    shutil.copy(os.path.join(source_bank_statements, i[0].replace('xlsx', 'csv')), i[1])
    # удалим исходные данные с расширением .csv из первоначальной папки
    os.remove(os.path.join(source_bank_statements, i[0].replace('xlsx', 'csv')))


# In[ ]:


# удалим исходные данные с расширением .xlsx из первоначальной папки
for i in bank_statements_name:
    os.remove(os.path.join(source_bank_statements, i))

