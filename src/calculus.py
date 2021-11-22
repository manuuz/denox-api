
from math import *
import datetime
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

from .database import connection_database

## função para cálculo de distância no globo
def distanceCalculus(lon1, lat1, lon2, lat2):
    ## conversão para radianos
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    
    ## aplicação da fórmula de variação
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    ## aplicação da fórmula de haversine
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6373.0 #raio da Terra 

    distance = r * c #em km

    return distance

## função para cálculos pedidos: distância percorrida, tempo em movimento, tempo parado e centroides
def metricsCalculus(data):
    total_distance = 0
    moving_time = 0
    downtime = 0

    # serial e criação de dataframe para manipulação
    serial = int(data[1]['serial'])
    df = pd.DataFrame.from_records(data)

    # a partir de um loop calcula tanto a distancia total (com a variação das linhas a partir do valor da coluna)
    # quanto analisa se há situação de movimento, acrescentando então o tempo de maneira prática
    for i, r in df.iterrows():
        if i != 0:
            total_distance += distanceCalculus(r['latitude'], r['longitude'], df.iloc[i - 1]['latitude'], df.iloc[i - 1]['longitude'])

            if r['situacao_movimento'] == True:
                moving_time += 60 #60 segundos (= 1 minuto)
            else:
                downtime += 60 #60 segundos (= 1 minuto)

    # retorno
    payload = {
        'distancia_percorrida': total_distance,
		'tempo_em_movimento': moving_time, #em segundos 
		'tempo_parado': downtime, #em segundos
		'centroides_paradas':[[-19.985399, -43.948095],[-19.974550, -43.948438]],
		'serial': serial
    }

    return payload


## função para coleta de dados
def returnMetrics(serial, datahora_inicio, datahora_fim):
    # pega o serial e converte para int, e as datahora para datetime no formato correto
    serial = int(serial)
    datahora_inicio = int(datetime(datahora_inicio, "%d/%m/%Y %H:%M:%S").timestamp())
    datahora_fim = int(datetime(datahora_fim, "%d/%m/%Y %H:%M:%S").timestamp())

    # conexão com o banco de dados
    db = connection_database()
    data = db.dados_rastreamento.find({"serial": serial, "datahora":{"$gte": datahora_inicio,"$lte": datahora_fim}}, {'_id': False})

    return data
