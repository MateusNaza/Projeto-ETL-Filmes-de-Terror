# %%
from random import random

T = 10000

I = 0

for i in range(T):
    x = random()
    y = random()

    if ((x ** 2) + (y ** 2)) < 1:
        I = I + 1

print(I)

pi = 4 * I / T

print(pi)




