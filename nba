## Bibliotecas
from sqlalchemy import create_engine
import pandas as pd
from datetime import date

## Configurações gerais
path_nba_payroll = '/Users/ellen/Aula_How/Aula004/NBA Payroll(1990-2023).csv'
path_nba_player_box_score_stats = '/Users/ellen/Aula_How/Aula004/NBA Player Box Score Stats(1950 - 2022).csv'
path_nba_player_stats = '/Users/ellen/Aula_How/Aula004/NBA Player Stats(1950 - 2022).csv'
path_nba_salaries = '/Users/ellen/Aula_How/Aula004/NBA Salaries(1990-2023).csv'

## Declarando variável data atual
today = date.today()

## Conexão com o banco de dados
host = 'localhost'
dbname = 'db_test'
user = 'root'
password = 'root'

## Leitura dos dados em csv
df1 = pd.read_csv(path_nba_payroll)
df2 = pd.read_csv(path_nba_player_box_score_stats)
df3 = pd.read_csv(path_nba_player_stats)
df4 = pd.read_csv(path_nba_salaries)


# Tratamento Colunas do dataframe df1:


df1.insert(1, 'date', today)  ## inserindo o campo data com data de criação do registro

df1['payroll'] = df1['payroll'].str.replace('$', '')
df1['payroll'] = df1['payroll'].str.replace(',', '')
df1['payroll'] = df1['payroll'].astype(float).round(2)

df1['inflationAdjPayroll'] = df1['inflationAdjPayroll'].str.replace('$', '')
df1['inflationAdjPayroll'] = df1['inflationAdjPayroll'].str.replace(',', '')
df1['inflationAdjPayroll'] = df1['inflationAdjPayroll'].astype(float).round(2)

df1 = df1.rename(columns = {'Unnamed: 0' : 'id'})


# Tratamento Colunas do dataframe df2:

df2.insert(1, 'date', today)

df2['GAME_DATE'] = pd.to_datetime(df2['GAME_DATE'], format='%b %d, %Y')
df2['MATCHUP'] = df2['MATCHUP'].str.replace('@', 'vs.')

df2 = df2.rename(columns = {'Unnamed: 0' : 'id'})
df2['id'] = df2['id']+1

# Tratamento Colunas do dataframe df3:

df3.insert(1, 'date', today) ## inserindo o campo data com data de criação do registro

df3 = df3.fillna(value=0)

df3['Age'] = df3['Age'].astype(int)
df3['G'] = df3['G'].astype(int)
df3['MP'] = df3['MP'].astype(int)
df3['FGA'] = df3['FGA'].astype(int)
df3['3P'] = df3['3P'].astype(int)
df3['3PA'] = df3['3PA'].astype(int)
df3['2P'] = df3['2P'].astype(int)
df3['2PA'] = df3['2PA'].astype(int)
df3['FT'] = df3['FT'].astype(int)
df3['FTA'] = df3['FTA'].astype(int)
df3['ORB'] = df3['ORB'].astype(int)
df3['DRB'] = df3['DRB'].astype(int)
df3['TRB'] = df3['TRB'].astype(int)
df3['AST'] = df3['AST'].astype(int)
df3['STL'] = df3['STL'].astype(int)
df3['BLK'] = df3['BLK'].astype(int)
df3['TOV'] = df3['TOV'].astype(int)
df3['PF'] = df3['PF'].astype(int)
df3['PTS'] = df3['PTS'].astype(int)


df3 = df3.rename(columns={'Unnamed: 0': 'CountYear','Unnamed: 0.1' : 'id'})
df3['id'] = df3['id']+1

# Tratamento Colunas do dataframe df4: 

df4.insert(1, 'date', today) ## inserindo o campo data com data de criação do registro

df4['salary'] = df4['salary'].str.replace('$', '')
df4['salary'] = df4['salary'].str.replace(',', '')
df4['salary'] = df4['salary'].astype(float).round(2)

df4['inflationAdjSalary'] = df4['inflationAdjSalary'].str.replace('$', '')
df4['inflationAdjSalary'] = df4['inflationAdjSalary'].str.replace(',', '')
df4['inflationAdjSalary'] = df4['inflationAdjSalary'].astype(float).round(2)

df4 = df4.rename(columns={'Unnamed: 0' : 'id'})
df4['id'] = df4['id']+1

# conectando com o banco de dados postgree

conn_string = f'postgresql://{user}:{password}@{host}:5432/{dbname}'
con = create_engine(conn_string)
conn = con.connect()

# carregando as tabelas

df1.to_sql('Payroll', schema='NBA', con=conn, if_exists='replace', index=False)
df2.to_sql('Player_Box_Score_Stats', schema='NBA', con=conn, if_exists='replace', index=False)
df3.to_sql('Play_Stats', schema='NBA', con=conn, if_exists='replace', index=False)
df4.to_sql('Salaries', schema='NBA', con=conn, if_exists='replace', index=False)

conn.close()
