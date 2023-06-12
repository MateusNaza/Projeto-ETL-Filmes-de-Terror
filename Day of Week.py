# %%
from datetime import datetime

today = datetime.now()
today_week = input('Digite as três primeiras letras do dia de hoje:').upper()
typed_day = input('Digite a data desejada: (DD/MM/YYYY) ')
calc_date = datetime.strptime(typed_day, '%d/%m/%Y')


days_of_week_dic = {'SEG': 'Segunda-Feira',
                    'TER': 'Terça-Feira',
                    'QUA': 'Quarta-Feira',
                    'QUI': 'Quinta-Feira',
                    'SEX': 'Sexta-Feira',
                    'SAB': 'Sábado',
                    'DOM': 'Domingo'}


factor = ((today - calc_date).days) % 7


def calc_day_of_week(day, factor):
    days_of_week_list = ['SEG', 'TER', 'QUA', 'QUI', 'SEX', 'SAB', 'DOM']
    index = days_of_week_list.index(day)
    result = days_of_week_dic[days_of_week_list[index - factor]]
    return result


print(f'Hoje é {days_of_week_dic[today_week]}.')
print(f'O dia {typed_day} cairá numa {calc_day_of_week(today_week, factor)}!')