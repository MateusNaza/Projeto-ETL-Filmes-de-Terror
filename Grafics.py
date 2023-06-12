# %%
import matplotlib.pyplot as plt
import seaborn as sns

x = [20, 42, 36, 54, 55, 8, 23, 30, 50, 44]
y = ['N1', 'N2','N3','N4','N5','N6','N7','N8','N9','N10',]
z = ['Variável 1','Variável 2','Variável 3','Variável 4','Variável 5','Variável 6',
     'Variável 7','Variável 8','Variável 9','Variável 10',]

# Barra Vertical
plt.bar(y, x)

# Barra Horizontal
plt.barh(z, x, color='g')

# Barra Horizontal com Seaborn
sns.barplot(x=x, y=z)

# %%
import matplotlib.pyplot as plt
import numpy as np

x = np.random.randn(1000)

plt.hist(x, bins = 100)
plt.xlabel('Eixo X')
plt.ylabel('Frequencias')