# %%
from locale import setlocale, LC_ALL
from calendar import mdays, month_name
from functools import reduce


# Minha solução usando somente o Filter()

def month_with_31_days():
    setlocale(LC_ALL, 'pt-br')

    month_days_dict = dict(zip(month_name, mdays))
    result = filter(lambda m: month_days_dict[m] == 31, month_days_dict)

    print('Meses com 31 dias: ')
    return [f'-> {m}' for m in result]


month_with_31_days()

# %%
from locale import setlocale, LC_ALL
from calendar import mdays, month_name
from functools import reduce


# Solução apresentada no curso

setlocale(LC_ALL, 'pt-br')

months = filter(lambda m: mdays[m] == 31, range(1, 13))
month_names = map(lambda m: month_name[m], months)
print(reduce(lambda title, m_name: f"{title}\n- {m_name}", month_names, 'Meses com 31 dias: '))