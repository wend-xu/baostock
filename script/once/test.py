import pandas as pd

key = 'c'
df = pd.DataFrame({
    'a': [1, 2, 3],
    'b': [4, 5, 6]
})
length = df.shape[0]
column = pd.Series([None] * length)
df[key] = column

column2 = df.get(key)
column.iloc[1] = 7
df.at[0,key] = 8
print("")
