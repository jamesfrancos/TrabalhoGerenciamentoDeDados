import streamlit as st
from pyspark.sql import SparkSession 
import pandas as pd
import matplotlib.pyplot as plt

sp = SparkSession.builder.appName("Brasileirao").getOrCreate()

st.title("Análises do Brasileirão")

@st.cache_resource
def load_data():
    df = (
        sp.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("DatasetBrasileirao2003.csv")
    )
    return df

@st.cache_data
def toPandas():
    return df_spark.toPandas()

df_spark = load_data()

df = toPandas()
dfToFilter = df

# sidebar - Filtros
anos = ["Todos"] + sorted(df["ano_campeonato"].unique())
ano = st.sidebar.selectbox("Selecione o Ano",anos)

if ano == "Todos":
   dfToFilter = df

else:
    dfToFilter = dfToFilter[dfToFilter["ano_campeonato"]==ano]

# Mandante ou visistante
opcao = st.sidebar.selectbox("Escolha o tipo de time:", ("Mandante", "Visitante"))

# Caixa de seleção dos times
if opcao == "Mandante":
    tipo = ("time_mandante", "gols_mandante")
    
else:
    tipo = ("time_visitante", "gols_visitante")

times = ["Todos"] + sorted(df[tipo[0]].unique())
time = st.sidebar.selectbox("Selecione o time:", times)

# Cálculo da média de gols

media_gols = (
    dfToFilter.groupby(tipo[0])[tipo[1]]
    .mean()
    .sort_values()
)


# Escolhendo o Ano








# 1) Se NÃO escolher time
if time == "Todos":
    # media de gols de todos os times
    st.subheader(f"Média de gols por time {opcao}")

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(media_gols.index, media_gols.values)
    ax.set_xticklabels(media_gols.index, rotation=90)
    ax.set_title(f"Média de gols por time {opcao}")
    ax.set_xlabel("Time")
    ax.set_ylabel("Média de gols")

    st.pyplot(fig)

    # Média de publico
    if ano == "Todos":
        publico_medio = dfToFilter.groupby(tipo[0])["publico"].mean().sort_values()
        fig1, ax1 = plt.subplots(figsize=(12, 6))
        st.subheader(f"Média de publico por time {opcao}")
        ax1.bar(publico_medio.index, publico_medio.values)
        ax1.set_xticklabels(publico_medio.index, rotation=90)
        ax1.set_title(f"Público médio por time {opcao}")
        ax1.set_xlabel("Time")
        ax1.set_ylabel("Público médio")
    
        st.pyplot(fig1)
# 2) Se escolher um time
else:
    st.subheader(f"Média de gols do {time} como {opcao} por ano")

    # Média apenas do time selecionado
    media_ano = (
        df[df[tipo[0]] == time]
        .groupby("ano_campeonato")[tipo[1]]
        .mean()
        .sort_index()
    )

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(media_ano.index, media_ano.values, marker="o")
    ax.set_title(f"Média de gols por ano — {time}")
    ax.set_xlabel("Ano")
    ax.set_ylabel("Média de gols")

    st.pyplot(fig)

    if ano == "Todos":
        # Média de publico por ano
        publico_medio_ano = (
        dfToFilter[dfToFilter[tipo[0]] == time]
        .groupby("ano_campeonato")["publico"]
        .mean()
        .sort_index()
    )
        texto = f"Média de publico por ano {opcao}"
    else:
        # Média de publico por rodada
        publico_medio_ano = (
            dfToFilter[dfToFilter[tipo[0]] == time]
            .groupby("rodada")["publico"]
            .mean()
            .sort_index()
        )   
        texto = f"Média de publico por rodada {opcao}"

    print(publico_medio_ano)

    fig1, ax1 = plt.subplots(figsize=(12, 6))
    # fazer um if pra trocar por rodada tambem
    st.subheader(texto)

    ax1.plot(publico_medio_ano.index.astype(int), publico_medio_ano.values, marker="o")
    #ax1.set_xticklabels(publico_medio_ano.index, rotation=90)
    ax1.set_xticks(publico_medio_ano.index.astype(int))
    ax1.set_xticklabels(publico_medio_ano.index.astype(int), rotation=45)

    ax1.set_title(f"ano")
    ax1.set_xlabel("Time")
    ax1.set_ylabel("Público médio")
    st.pyplot(fig1)