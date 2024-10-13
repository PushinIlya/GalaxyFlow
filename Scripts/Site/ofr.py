#!/usr/bin/env python
# coding: utf-8

# ## Обработка отчёта о финансовых результатах

# In[ ]:


import pandas as pd
import pathlib
import shutil
import os


# In[ ]:


# запишем путь к папке с исходными данными в переменную
source = pathlib.Path(os.getcwd().replace('Fin_Flow', os.path.join('YandexDisk', 'Fin_Flow')).replace('Scripts', 'Source data'))


# In[ ]:


print('Расчёт "Выручка":')
import revenue
from revenue import rev_merged
print()
print('Расчёт "Себестоимость продаж":')
import cost_of_sales
from cost_of_sales import costs_merged
print()
print('Расчёт "Коммерческие расходы":')
import commercial_costs
from commercial_costs import com_costs_merged
print()
print('Расчёт "Управленческие расходы":')
import administrative_costs
from administrative_costs import adm_costs_merged
print()
print('Расчёт "Прочие доходы":')
import other_income
from other_income import oth_incom_merged
print()
print('Расчёт "Прочие расходы":')
import other_costs
from other_costs import oth_costs_merged
print()
print('Расчёт "Налог на прибыль":')
import income_tax
from income_tax import tax_merged


# In[ ]:


ofr = {i[0].replace('Выручка', 'ОФР'): pd.concat([rev_merged[i[0]],
                                                  costs_merged[i[0].replace('Выручка', 'Себестоимость продаж')],
                                                  com_costs_merged[i[0].replace('Выручка', 'Коммерческие расходы')],
                                                  adm_costs_merged[i[0].replace('Выручка', 'Управленческие расходы')],
                                                  oth_incom_merged[i[0].replace('Выручка', 'Прочие доходы')],
                                                  oth_costs_merged[i[0].replace('Выручка', 'Прочие расходы')],
                                                  tax_merged[i[0].replace('Выручка', 'Налог на прибыль')]]).reset_index(drop=True)
       for i in rev_merged.items()}


# In[ ]:


# определим последнюю дату по всем показателям группы компаний, чтобы продлить показатели с более ранними последними датами до самой поздней даты
last_date = {i[0]: i[1].sort_values(by='Дата')['Дата'].iloc[-1] for i in ofr.items()}


# In[ ]:


# создадим словарь со строками на последнюю дату из показателей, которые нужно продлить
ofr_new_rows = {i[0]: i[1].groupby(['Организация', 'Показатель'], as_index=False)['Дата'].max() for i in ofr.items()}


# In[ ]:


# оставим только те показатели, которые нужно продлить
ofr_new_rows = {i[0]: i[1][i[1]['Дата'] != i[1]['Дата'].max()] for i in ofr_new_rows.items()}


# In[ ]:


# получим строки, которые нужно продлить
ofr_new_rows = {i[0]: pd.merge(i[1], ofr[i[0]],
                               on=['Организация',
                                   'Показатель',
                                   'Дата'],
                               how='left') for i in ofr_new_rows.items()}


# In[ ]:


# добавим промежуточные значения по остаткам на счёте, используя диапазоны дат
for i in ofr_new_rows.items():
    i[1]['Начальная дата'] = i[1]['Дата']
    i[1]['Конечная дата'] = last_date[i[0]]
    i[1]['Дата'] = i[1].apply(lambda row: pd.date_range(start=row['Начальная дата'],
                                                        end=row['Конечная дата']).tolist()
                              if pd.notna(row['Начальная дата'])
                              else row['Дата'],
                              axis=1)


# In[ ]:


# добавим новые строки с датами
ofr_new_rows = {i[0]: i[1].explode('Дата') for i in ofr_new_rows.items()}


# In[ ]:


# удалим первоначальные строки, которые уже есть в ofr, чтобы избежать дубликатов
ofr_new_rows = {i[0]: i[1][i[1]['Дата'] != i[1]['Начальная дата']] for i in ofr_new_rows.items()}


# In[ ]:


ofr_new_rows = {i[0]: i[1].drop(columns=['Начальная дата',
                                         'Конечная дата'])
                for i in ofr_new_rows.items()}


# In[ ]:


for i in ofr_new_rows.values():
    i['Изменение'] = 0.0
    i['Начальный остаток'] = i['Конечный остаток']


# In[ ]:


# добавим новые строки
ofr = {i[0]: pd.concat([i[1],
                        ofr_new_rows[i[0]]]).reset_index(drop=True)
       for i in ofr.items()}


# In[ ]:


# рассчитаем прибыль
profit = {i[0]: i[1].groupby(['Показатель',
                              'Организация',
                              'Дата'], as_index=False)['Изменение'].sum() for i in ofr.items()}


# In[ ]:


profit = {i[0]: i[1].pivot_table(index='Дата', columns=['Показатель', 'Организация'], values='Изменение').reset_index() for i in profit.items()}


# In[ ]:


for i in profit.values():
    i['Выручка'] = i['Выручка'].fillna(0)
    i['Себестоимость продаж'] = i['Себестоимость продаж'].fillna(0)
    i['Коммерческие расходы'] = i['Коммерческие расходы'].fillna(0)
    i['Управленческие расходы'] = i['Управленческие расходы'].fillna(0)
    i['Прочие доходы'] = i['Прочие доходы'].fillna(0)
    i['Прочие расходы'] = i['Прочие расходы'].fillna(0)
    i['Налог на прибыль'] = i['Налог на прибыль'].fillna(0)


# In[ ]:


for i in profit.values():
    for column in i.columns.levels[1].drop(''):
        for index, row in i.iterrows():
            i.at[index, ('Валовая прибыль (убыток)', column)] = row['Выручка'][column] + row['Себестоимость продаж'][column]


# In[ ]:


for i in profit.values():
    for column in i.columns.levels[1].drop(''):
        for index, row in i.iterrows():
            i.at[index, ('Прибыль (убыток) от продаж', column)] = row['Валовая прибыль (убыток)'][column] + row['Коммерческие расходы'][column] + row['Управленческие расходы'][column]


# In[ ]:


for i in profit.values():
    for column in i.columns.levels[1].drop(''):
        for index, row in i.iterrows():
            i.at[index, ('Прибыль (убыток) до налогообложения', column)] = row['Прибыль (убыток) от продаж'][column] + row['Прочие доходы'][column] + row['Прочие расходы'][column]   


# In[ ]:


for i in profit.values():
    for column in i.columns.levels[1].drop(''):
        for index, row in i.iterrows():
            i.at[index, ('Чистая прибыль (убыток)', column)] = row['Прибыль (убыток) до налогообложения'][column] + row['Налог на прибыль'][column]


# In[ ]:


profit = {i[0]: i[1].melt(id_vars='Дата', var_name=['Показатель', 'Организация'], value_name='Изменение') for i in profit.items()}


# In[ ]:


gross_profit = {i[0]: i[1].query('Показатель == "Валовая прибыль (убыток)"').reset_index(drop=True) for i in profit.items()}


# In[ ]:


grouped_dict = {}
for key, value in gross_profit.items():
    # применение операций shift и cumsum к каждой организации отдельно
    group_list = []
    for name, group in value.groupby(['Организация']):
        group['Начальный остаток'] = group['Изменение'].shift(fill_value=0).cumsum()
        group['Конечный остаток'] = group['Начальный остаток'] + group['Изменение']
        group_list.append(group)
    table = pd.concat(group_list, axis=0)
    grouped_dict[key] = table

gross_profit = grouped_dict


# In[ ]:


sales_profit = {i[0]: i[1].query('Показатель == "Прибыль (убыток) от продаж"').reset_index(drop=True) for i in profit.items()}


# In[ ]:


grouped_dict = {}
for key, value in sales_profit.items():
    # применение операций shift и cumsum к каждой организации отдельно
    group_list = []
    for name, group in value.groupby(['Организация']):
        group['Начальный остаток'] = group['Изменение'].shift(fill_value=0).cumsum()
        group['Конечный остаток'] = group['Начальный остаток'] + group['Изменение']
        group_list.append(group)
    table = pd.concat(group_list, axis=0)
    grouped_dict[key] = table

sales_profit = grouped_dict


# In[ ]:


profit_before_tax = {i[0]: i[1].query('Показатель == "Прибыль (убыток) до налогообложения"').reset_index(drop=True) for i in profit.items()}


# In[ ]:


grouped_dict = {}
for key, value in profit_before_tax.items():
    # применение операций shift и cumsum к каждой организации отдельно
    group_list = []
    for name, group in value.groupby(['Организация']):
        group['Начальный остаток'] = group['Изменение'].shift(fill_value=0).cumsum()
        group['Конечный остаток'] = group['Начальный остаток'] + group['Изменение']
        group_list.append(group)
    table = pd.concat(group_list, axis=0)
    grouped_dict[key] = table

profit_before_tax = grouped_dict


# In[ ]:


net_profit = {i[0]: i[1].query('Показатель == "Чистая прибыль (убыток)"').reset_index(drop=True) for i in profit.items()}


# In[ ]:


grouped_dict = {}
for key, value in net_profit.items():
    # применение операций shift и cumsum к каждой организации отдельно
    group_list = []
    for name, group in value.groupby(['Организация']):
        group['Начальный остаток'] = group['Изменение'].shift(fill_value=0).cumsum()
        group['Конечный остаток'] = group['Начальный остаток'] + group['Изменение']
        group_list.append(group)
    table = pd.concat(group_list, axis=0)
    grouped_dict[key] = table

net_profit = grouped_dict


# In[ ]:


ofr = {i[0]: pd.concat([i[1],
                        gross_profit[i[0]],
                        sales_profit[i[0]],
                        profit_before_tax[i[0]],
                        net_profit[i[0]]]).reset_index(drop=True)
       for i in ofr.items()}


# In[ ]:


for i in ofr.values():
    i['Вид статьи'] = i['Вид статьи'].fillna('-')
    i['Статья'] = i['Статья'].fillna('-')


# In[ ]:


for i in ofr.items():
    print(f'{i[0]}:')
    print(i[1].info(show_counts=True))
    print()


# In[ ]:


'''
Цикл, который перезаписывает каждый файл из 'ofr', чтобы сохранить внесённые ранее изменения в его структуру
'''
for i in ofr.items():
    i[1].to_csv(os.path.join(source, i[0].replace('xlsx', 'csv')),
                header=True,
                index=False)


# In[ ]:


# путь для сохранения исходных данных
path_to_save = pathlib.Path(os.getcwd().replace('Scripts', 'Data'))


# In[ ]:


# создадим словарь с расположением новых папок по группам компаний
folder_location = {i: os.path.join(path_to_save) for i in ofr.keys()}


# In[ ]:


# создаются только те папки, которые до этого не существовали
for i in folder_location.items():
    if not os.path.exists(i[1]):
        os.makedirs(i[1])


# In[ ]:


# цикл, который копирует каждый файл из 'ofr' в новую папку
for i in folder_location.items():
    shutil.copy(os.path.join(source, i[0].replace('xlsx', 'csv')), i[1])
    # удалим исходные данные с расширением .csv из первоначальной папки
    os.remove(os.path.join(source, i[0].replace('xlsx', 'csv')))

