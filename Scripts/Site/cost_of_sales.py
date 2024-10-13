#!/usr/bin/env python
# coding: utf-8

# ## Формирование отчёта о финансовых результатах

# ### Расчёт "Себестоимость продаж"

# In[ ]:


'''
Скрипт для преобразования исходных данных из 1C
'''
# импортируем библиотеки
import pandas as pd
import numpy as np
import os
import pathlib
from openpyxl import load_workbook
from datetime import datetime, timedelta


# In[ ]:


# запишем путь к папке с исходными данными в переменную
source = pathlib.Path(os.getcwd().replace('Fin_Flow', os.path.join('YandexDisk', 'Fin_Flow')).replace('Scripts', 'Source data'))


# In[ ]:


# создадим списки с названиями всех необходимых файлов в папке с исходными данными
source_costs_name = [raw_data for raw_data in os.listdir(source) if 'Себестоимость продаж' in raw_data and
                     raw_data.endswith('.xlsx')]

print(source_costs_name)


# In[ ]:


# загрузим excel-файлы в pandas
costs = {raw_data: pd.read_excel(os.path.join(source, f'{raw_data}'), header=None) for raw_data in source_costs_name}


# In[ ]:


# загрузим excel-файлы в openpyxl
costs_structure = {raw_data: load_workbook(filename=os.path.join(source, f'{raw_data}')) for raw_data in source_costs_name}


# In[ ]:


# получим первый лист в каждом excel-файле
costs_structure = {i[0]: i[1].worksheets[0] for i in costs_structure.items()}


# In[ ]:


# создадим словарь для каждого файла, где ключ-номер строки, а значение-уровень группировки
costs_structure = {i[0]: {row: i[1].row_dimensions[row].outline_level for row in i[1].row_dimensions} for i in costs_structure.items()}


# In[ ]:


# преобразуем словари в pd.Series
costs_structure = {i[0]: pd.Series(i[1], name='Уровень').reset_index(drop=True) for i in costs_structure.items()}


# In[ ]:


# добавим в основные датафреймы уровни группировки строк
costs = {i[0]: i[1].merge(costs_structure[i[0]], left_index=True, right_index=True, how='inner') for i in costs.items()}


# In[ ]:


costs = {i[0]: i[1].drop(list(range(6))) for i in costs.items()}


# In[ ]:


for i in costs.values():
    i['Организация'] = i.apply(lambda row: row[0] if row['Уровень'] == 0 else np.nan,
                               axis=1)
    i['Счет'] = i.apply(lambda row: row[0] if row['Уровень'] == 1 else np.nan,
                        axis=1)
    i['Кор_счет'] = i.apply(lambda row: row[0] if row['Уровень'] == 2 else np.nan,
                            axis=1)
    i['Вид статьи'] = i.apply(lambda row: row[0] if row['Уровень'] == 3 else np.nan,
                              axis=1)
    i['Статья'] = i.apply(lambda row: row[0] if row['Уровень'] == 4 else np.nan,
                          axis=1)
    i['Дата'] = i.apply(lambda row: row[0] if row['Уровень'] == 5 else np.nan,
                        axis=1)


# In[ ]:


costs = {i[0]: i[1].rename(columns={1: 'Изменение'}) for i in costs.items()}


# In[ ]:


# словари с именами столбцов и типами данных
type_of_columns_costs = {'Изменение': 'float64',
                         'Организация': 'object',
                         'Счет': 'object',
                         'Кор_счет': 'object', 
                         'Вид статьи': 'object',
                         'Статья': 'object'}


# In[ ]:


# подготовим строки для преобразования их в числовые значения
for i in costs.values():
    i['Изменение'] = i['Изменение'].apply(lambda row: str(row).replace(',', '.').replace(' ', '')
                                          if pd.notna(row)
                                          else row)


# In[ ]:


# изменим тип данных в каждом датафрейме в соответствии со словарём 'type_of_columns'
costs = {i[0]: i[1].astype(type_of_columns_costs) for i in costs.items()}


# In[ ]:


# заполним пропущенные значения в столбцах значениями предыдущей заполненной ячейки по строкам
for i in costs.values():
    i['Организация'] = i['Организация'].fillna(method='ffill', axis=0)
    i['Счет'] = i['Счет'].fillna(method='ffill', axis=0)
    i['Кор_счет'] = i['Кор_счет'].fillna(method='ffill', axis=0)
    i['Вид статьи'] = i['Вид статьи'].fillna(method='ffill', axis=0)
    i['Статья'] = i['Статья'].fillna(method='ffill', axis=0)


# In[ ]:


for i in costs.values():
    i.dropna(subset=['Дата'], inplace=True)


# In[ ]:


date_formats = ['%d.%m.%Y', '%m/%d/%Y'] # форматы дат
# преобразуем строки в даты, используя один из форматов
for i in costs.values():
    i['Дата'] = i['Дата'].apply(lambda row: pd.to_datetime(row, format=next((f for f in date_formats if pd.to_datetime(row, format=f, errors='coerce') is not pd.NaT), None)))


# In[ ]:


costs = {i[0]: i[1].drop(columns=[0, 'Кор_счет'])
         for i in costs.items()}


# In[ ]:


# исключим пустые датафреймы
costs = {i[0]: i[1] for i in costs.items() if not i[1].empty}


# In[ ]:


for i in costs.values():
    i['Статья'] = i['Статья'].fillna('-')


# In[ ]:


for i in costs.values():
    i['Статья'] = i['Статья'].apply(lambda row: f"{row.rsplit(',', 1)[0]} (ИНН: {row.rsplit(',', 1)[-1]})"
                                    if row.rsplit(',', 1)[-1] != ' '
                                    else row[:-2])


# In[ ]:


costs = {i[0]: i[1].groupby(['Организация',
                             'Вид статьи',
                             'Статья',
                             'Дата'], as_index=False)['Изменение'].sum() for i in costs.items()}


# In[ ]:


# создадим словарь, где ключ - название группы компаний, а значение - список таблиц по всем компаниям группы
costs_groups = {}

for key, value in costs.items():
    group = key.split('(')[1].split(')')[0]
    if group in costs_groups:
        costs_groups[group].append(value)
    else:
        costs_groups[group] = [value]


# In[ ]:


# создадим новый словарь с объединёнными таблицами
costs_merged = {f'Себестоимость продаж_{i[0]}.xlsx': pd.concat(i[1], axis=0).reset_index(inplace=False, drop=True)
                for i in costs_groups.items()}


# In[ ]:


# определим последнюю дату по всем датафреймам группы компаний, чтобы продлить датафреймы с более ранними последними датами до самой поздней даты
last_date = {i[0]: i[1].sort_values(by='Дата')['Дата'].iloc[-1] for i in costs_merged.items()}


# In[ ]:


# создадим словарь со строками на последнюю дату из статей, которые нужно продлить
costs_new_rows = {i[0]: i[1].groupby(['Организация', 'Вид статьи', 'Статья'], as_index=False)['Дата'].max() for i in costs.items()}


# In[ ]:


# удалим статьи, которые не нужно продлять
for i in costs_new_rows.items():
    i[1].drop(i[1][i[1]['Дата'] == last_date[f'Себестоимость продаж_{i[0].split("(")[1].split(")")[0]}.xlsx']].index, inplace=True)


# In[ ]:


# изменим добавляемые строки
for i in costs_new_rows.items():
    i[1]['Изменение'] = 0
    i[1]['Дата'] = last_date[f'Себестоимость продаж_{i[0].split("(")[1].split(")")[0]}.xlsx']


# In[ ]:


# добавим новые строки в датафреймы
costs = {i[0]: pd.concat([i[1], costs_new_rows[i[0]]]) for i in costs.items()}


# In[ ]:


# создадим столбцы для добавления новых данных по остаткам на счетах
for i in costs.values():
    i['Разница'] = i.groupby(['Организация', 'Вид статьи', 'Статья'])['Дата'].diff()


# In[ ]:


# добавим промежуточные значения по остаткам на счёте, используя диапазоны дат
for i in costs.values():
    i['Начальная дата'] = i['Дата'] - i['Разница'] + timedelta(days=1)
    i['Конечная дата'] = i['Дата']
    i['Дата'] = i.apply(lambda row: pd.date_range(start=row['Начальная дата'],
                                                  end=row['Конечная дата']).tolist()
                        if pd.notna(row['Начальная дата'])
                        else row['Дата'],
                        axis=1)


# In[ ]:


# добавим новые строки с датами
costs = {i[0]: i[1].explode('Дата') for i in costs.items()}


# In[ ]:


for i in costs.values():
    i['Изменение'] = i.apply(lambda row: 0 if row['Дата'] != row['Конечная дата'] else row['Изменение'],
                             axis=1)


# In[ ]:


costs = {i[0]: i[1].drop(columns=['Разница',
                                  'Начальная дата',
                                  'Конечная дата'])
         for i in costs.items()}


# In[ ]:


# создадим словарь для добавления промежуточных значений по остаткам на счетах, используя данные по остаткам на дату накопительным итогом
# сгруппируем до уровня дат и получим уникальные значения статей для каждой даты
costs_values = {i[0]: i[1].groupby(['Организация', 'Вид статьи', 'Дата'], as_index=False)['Статья'].unique() for i in costs.items()} 


# In[ ]:


# отсортируем датафреймы по дате
costs_values = {i[0]: i[1].groupby(['Организация', 'Вид статьи'], as_index=False).apply(lambda row: row.sort_values('Дата')) for i in costs_values.items()} 


# In[ ]:


# присвоим индексы
costs_values = {i[0]: i[1].groupby(['Организация', 'Вид статьи'], as_index=False).apply(lambda row: row.reset_index(drop=True)) for i in costs_values.items()}


# In[ ]:


# добавим на каждую дату список из списков уникальных статей накопительным итогом
grouped_dict = {}
for key, value in costs_values.items():
    group_list = []
    for name, group in value.groupby(level=0):
        group['Статья (доп)'] = group.index.map(lambda row: group.loc[:row, 'Статья'].tolist())
        group['Статья (доп)'] = group['Статья (доп)'].apply(lambda row: set([x for sublist in row for x in sublist]))
        group_list.append(group)
    table = pd.concat(group_list, axis=0)
    grouped_dict[key] = table


# In[ ]:


costs_values = grouped_dict


# In[ ]:


# добавим новые строки со статьями
costs_values = {i[0]: i[1].explode('Статья (доп)') for i in costs_values.items()} 


# In[ ]:


for i in costs_values.values():
    i['Статья'] = i['Статья (доп)']
    i['Изменение'] = 0.0


# In[ ]:


costs_values = {i[0]: i[1].drop(columns=['Статья (доп)'])
                for i in costs_values.items()}


# In[ ]:


costs = {i[0]: pd.concat([i[1], costs_values[i[0]]]) for i in costs.items()}


# In[ ]:


costs = {i[0]: i[1].sort_values(by='Дата').groupby(['Организация',
                                                    'Вид статьи',
                                                    'Статья',
                                                    'Дата'],
                                                   as_index=False)['Изменение'].sum() for i in costs.items()}


# In[ ]:


for i in costs.values():
    i['Начальный остаток'] = 0.0
    i['Конечный остаток'] = 0.0


# In[ ]:


# создадим новые таблицы для расчёта остатков
costs_grouped = {i[0]: i[1].sort_values(by='Дата').groupby(['Организация',
                                                            'Вид статьи',
                                                            'Статья',
                                                            'Дата'],
                                                           as_index=False)['Изменение'].sum() for i in costs.items()}


# In[ ]:


grouped_dict = {}
for key, value in costs_grouped.items():
    # применение операций shift и cumsum к каждому счёту, организации и виду статьи отдельно
    group_list = []
    for name, group in value.groupby(['Организация',
                                      'Вид статьи',
                                      'Статья']):
        group['Начальный остаток'] = group['Изменение'].shift(fill_value=0).cumsum()
        group['Конечный остаток'] = group['Начальный остаток'] + group['Изменение']
        group_list.append(group)
    table = pd.concat(group_list, axis=0)
    grouped_dict[key] = table


# In[ ]:


costs_grouped = grouped_dict


# In[ ]:


merged_dict = {}
# объединим словарь с таблицами для расчёта остатков
for key, table1 in costs.items():
    table2 = costs_grouped.get(key)
    merged_table = pd.merge(table1, table2, on=['Организация',
                                                'Вид статьи',
                                                'Статья',
                                                'Дата'],
                            how='left',
                            suffixes=('', '_new'))
    merged_dict[key] = merged_table


# In[ ]:


costs = merged_dict


# In[ ]:


for i in costs.values():
    i['Начальный остаток'] = i['Начальный остаток_new']
    i['Конечный остаток'] = i['Конечный остаток_new']


# In[ ]:


# удалим лишние столбцы
costs = {i[0]: i[1].iloc[:, :-3] for i in costs.items()}


# In[ ]:


for i in costs.values():
    i['Изменение'] = -i['Изменение'] 
    i['Начальный остаток'] = -i['Начальный остаток'] 
    i['Конечный остаток'] = -i['Конечный остаток']
    i['Показатель'] = 'Себестоимость продаж'


# In[ ]:


costs = {i[0]: i[1].reset_index(drop=True) for i in costs.items()}


# In[ ]:


# создадим словарь, где ключ - название группы компаний, а значение - список таблиц по всем компаниям группы
costs_groups = {}

for key, value in costs.items():
    group = key.split('(')[1].split(')')[0]
    if group in costs_groups:
        costs_groups[group].append(value)
    else:
        costs_groups[group] = [value]


# In[ ]:


# создадим новый словарь с объединёнными таблицами
costs_merged = {f'Себестоимость продаж_{i[0]}.xlsx': pd.concat(i[1], axis=0).reset_index(inplace=False, drop=True)
                for i in costs_groups.items()}


# In[ ]:


for i in costs_merged.items():
    print(f'{i[0]}:')
    print(i[1].info(show_counts=True))
    print()


# In[ ]:


# удалим исходные данные с расширением .xlsx из первоначальной папки
for i in source_costs_name:
    os.remove(os.path.join(source, i))

