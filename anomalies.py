import dask.dataframe as dd
import matplotlib.pyplot as plt
import aux as aux
from dask.distributed import SSHCluster,Client
import math
import numpy as np
import pandas as pd
import os

def fill_missing_anomaly(anomalies, values, minutes):

    tmax=int(max(minutes))
    lastvalue=0
    fmin=minutes.copy()
    fvals=values.copy()
    fanom=anomalies.copy()

    for i in range(1,tmax):
        if i not in minutes:
            fmin.append(i)
            fvals.append(lastvalue)
            fanom.append(0)
        else:
            index=minutes.index(i)
            lastvalue=fvals[index]

    _, filled_values, filled_anomalies = zip(*sorted(zip(fmin, fvals, fanom)))

    return list(filled_values), list(filled_anomalies)


def plot_anom_or_trend(a,b,c, plot, hwid, eng):
    if plot == 'anom':
        colors=['none' if col == 0 else 'r' for col in b]

        plt.scatter(a, c, c='none', edgecolors=colors, marker='o')
        
        plt.title(f"Anomalies in engine {eng} of hardware {hwid}")
        plt.xlabel('Minute')
        plt.ylabel('State')
        plt.grid(True)
        plt.ylim(-0.5, 1.5)
        plt.yticks([0, 1], ['Off', 'On'])
    elif plot == 'trend':
        plt.scatter(a, c, c='none', edgecolors='b', marker='o')

        plt.xlabel('Minute')
        plt.ylabel('State')
        plt.grid(True)
        plt.ylim(-0.5, 1.5)
        plt.yticks([0, 1], ['Off', 'On'])
    elif plot == 'both':
        fig, axs = plt.subplots(2, 1, figsize=(5,10))
        colors=['none' if col == 0 else 'r' for col in b]

        axs[0].scatter(a, c, c='none', edgecolors=colors, marker='o')
        axs[0].set_xlabel('Minute')
        axs[0].set_ylabel('State')
        axs[0].set_ylim(-0.5, 1.5)
        axs[0].set_yticks([0, 1], ['Off', 'On'])
        axs[0].grid(True)


        axs[1].scatter(a, c, c='none', edgecolors='b', marker='o')
        axs[1].set_xlabel('Minute')
        axs[1].set_ylabel('State')
        axs[1].set_ylim(-0.5, 1.5)
        axs[1].set_yticks([0, 1], ['Off', 'On'])
        axs[1].grid(True)

    output_directory = './Motors_graphs'  
    output_file = f'{hwid}_{eng}_plot.png'

    os.makedirs(output_directory, exist_ok=True)

    output_path = os.path.join(output_directory, output_file)
    
    plt.savefig(output_path)
    plt.tight_layout()
    plt.show()


def anomaly_column(df, hwid, metric, aggfunc='mean'):
    #ottieni i periodi

    history=aux.aggregate_up_down(df,hwid,metric)

    #definisci una funzione che serve dopo 

    def is_within_intervals(value, intervals):
        for start, end in intervals:
            if start <= value <= end:
                return 1
        return 0

    #prendi il dataset filtrato che ti serve

    selection=df[df['hwid']==hwid]
    selection=selection[selection['metric']==metric]

    #trasforma i valori in millisecondi

    history_new=[int(x[1]*60000) for x in history]

    #ottieni il valore di tempo iniziale, tutti i tempi sono scalati con questo come inizio per colpa del .diff()

    initial_time=selection['when'].min().compute()

    #calcola, per gli intervalli delle anomalie (x<(60+1)*1000), la coppia (inizio, fine) dei tempi dell'intervallo. 
    #Questo si calcola con il tempo iniziale come base + la somma dei valori precedenti nel vettore per l'inizio,
    #e un ulteriore somma della durata dell'intervallo per la fine

    a=[(initial_time+sum(history_new[:i]),initial_time+sum(history_new[:i+1])) for i,x in enumerate(history_new) if x < (60+1)*1000]
    selection['minute']=selection['when'].map(lambda x: int((x - 1601510422405) / 60000),meta=('when','int64'))

    #crea una nuova colonna che prende when e ritorna true o false in base a se il valore di when è in uno degli intervalli delle anomalie

    selection['anomaly']=selection['when'].apply(lambda x: is_within_intervals(x, a), meta=('when', 'bool'))

    agg_dict = {'value': aggfunc, 'anomaly': 'max'}
    
    g = selection.groupby(['hwid', 'metric', 'minute']).agg(agg_dict).compute().sort_values(by='minute')

    
    minutes=list(g.index.get_level_values('minute'))
    anomaly = list(g['anomaly'])
    value = list(g['value'])

    filled_val, filled_anom = fill_missing_anomaly(anomaly, value, minutes)

    time = np.arange(0,len(filled_anom))

    return time, filled_anom, filled_val



def corr_finder(engine, metric, window):
    corr_coeff = list()

    for i in range(len(engine['Anomaly'])):
        if engine.iloc[i,1]:#['Anomaly']:
            values_engine = engine.iloc[i-window:i+window+1,2]#['Value']
            values_metric = metric.iloc[i-window:i+window+1,1]#['Value']

            df = pd.DataFrame({'values_engine': values_engine, 'values_metric': values_metric})
            corr_coeff.append(df.corr(method='pearson').iloc[0, 1])
    
    return corr_coeff