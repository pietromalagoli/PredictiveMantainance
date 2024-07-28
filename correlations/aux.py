#####IMPORTS###########
import dask.dataframe as dd
import matplotlib.pyplot as plt
import aux as aux
import math




#####CALC FUNCTIONS######

def aggregate_up_down(df, hwid, metric):

    selection=df[df['hwid']==hwid]
    selection=selection[selection['metric']==metric].compute()  #we use compute here because we already filtered and we can manage this much data furthermore the 
                                                                #diff operations cause problems when the dataframe is distributed because of the edges
                                                                #of the data itself where the diff is not trivially solved.
                                                                
                                                                #STATS FOR A CALL OF THIS FUNCTION ON ALL COMBINATIONS OF hw and ['S117','S118','S169','S170'] metric
                                                                #Version with compute call: 9m 26 s
                                                                #Version without compute call: 20m 38s

    selection['switch']=selection['value'].diff().map(abs).map(lambda x: True if x==1 or math.isnan(x) else False)
    selection=selection[selection['switch']]
    selection['times']=selection['when'].diff()/1000/60
    selection['value']=selection['value'].map(lambda x: 0 if x==1 else 1)
    times=list(selection['times'])[1:]
    values=list(selection['value'])[1:]
    history=[[value,time] for value,time in zip(values,times)]

    return history

def divide_up_down(aggregated):
    up_times=[el[1] for el in aggregated if el[0]==1]
    down_times=[el[1] for el in aggregated if el[0]==0]

    return up_times,down_times

def calc_mean_variance_std(aggregated):

    up_times,down_times=aux.divide_up_down(aggregated)

    mean_up_time=sum(up_times)/len(up_times)
    mean_down_time=sum(down_times)/len(down_times)

    variance_up=sum((x - mean_up_time) ** 2 for x in up_times) / (len(up_times) - 1)
    variance_down=sum((x - mean_down_time) ** 2 for x in down_times) / (len(down_times) - 1)

    std_up=variance_up**(1/2)
    std_down=variance_down**(1/2)
    
    #calculate stats without values out of 1 std from mean, proved not to be very useful
    up_times_filter=[time for time in up_times if (time>mean_up_time-std_up and time<mean_up_time+std_up)]
    down_times_filter=[time for time in down_times if (time>mean_down_time-std_down and time<mean_down_time+std_down)]

    mean_up_time_f=sum(up_times_filter)/len(up_times_filter)
    mean_down_time_f=sum(down_times_filter)/len(down_times_filter)

    variance_up_f=sum((x - mean_up_time_f) ** 2 for x in up_times_filter) / (len(up_times_filter) - 1)
    variance_down_f=sum((x - mean_down_time_f) ** 2 for x in down_times_filter) / (len(down_times_filter) - 1)

    std_up_f=variance_up_f**(1/2)
    std_down_f=variance_down_f**(1/2)
    
    return [mean_up_time,variance_up,std_up,mean_up_time_f,variance_up_f,std_up_f],[mean_down_time,variance_down,std_down,mean_down_time_f,variance_down_f,std_down_f]

def out_events(aggregated):

    info_up,info_down=calc_mean_variance_std(aggregated)
    out_up=[el for el in aggregated if ( el[0]==1 and (el[1]>(info_up[0]+3*info_up[2]) or el[1]<(info_up[0]-3*info_up[2])))]
    out_down=[el for el in aggregated if ( el[0]==0 and (el[1]>(info_down[0]+3*info_down[2]) or el[1]<(info_down[0]-3*info_down[2])))]

    return out_up,out_down

def getuniquecouples(df):
    g=df.groupby(['hwid','metric']).mean().compute()
    hwids=list(g.index.get_level_values('hwid'))
    metrics=list(g.index.get_level_values('metric'))
    combos=zip(hwids,metrics)
    combos=sorted(combos,key=lambda x:x[0])
    
    return combos

def average_up_down_analysis(df):

    zipped=getuniquecouples(df)

    for hw,metric in zipped:
            print("##################")
            print(f"analysis for hw {hw} and metric {metric}")
            aggregated=aux.aggregate_up_down(df,hw,metric)
            up_info,down_info=aux.calc_mean_variance_std(aggregated)
            print(f"Mean and standard deviation for uptime: {up_info[0]} +- {up_info[2]}")
            print(f"Mean and standard deviation for downtime: {down_info[0]} +- {down_info[2]}")
            print(f"Mean and standard deviation for uptime after filter: {up_info[3]} +- {up_info[5]}")
            print(f"Mean and standard deviation for downtime after filter: {down_info[3]} +- {down_info[5]}")

def find_anomaly_number(df):

    zipped=getuniquecouples(df)

    limitlow=(30+1)/60
    limithigh=(60+1)/60

    for hw,metric in zipped:
            print(f"hw: {hw} metric: {metric}")
            agg=aux.aggregate_up_down(df,hw,metric)
            times=[el[1] for el in agg]
            zeros=[x for x in times if x==0]
            print(f"The instances of 1 single isolated uptime or downtime reading are: {len(zeros)}")
            between=[x for x in times if (x>limitlow and x<limithigh)]
            print(f"The instances of uptimes or downtimes between 30 sec and 1 min are: {len(between)}")
            low=[x for x in times if (x>0 and x<limitlow)]
            print(f"The instances of <30 seconds uptimes or downtimes are: {len(low)}")

def plot_states(df):

    zipped=getuniquecouples(df)

    for hw,metric in zipped:
            aux.plot_machine_state(df,hwid=hw,metric=metric,\
                                    save_graph=True,show_graph=True)
            
def plot_percentages(df_percentages):
    zipped = aux.getuniquecouples(df_percentages)

    for hw,metric in zipped:
        prova=df_percentages[df_percentages['hwid']==hw]
        vals=list(prova[prova['metric']==metric]['value'].compute())
        print(f"Graph for hw: {hw} and metric {metric}")
        plt.scatter(range(len(vals)),vals)
        plt.show()

def get_norm_data(df,hwid,metric,aggfunc='mean'):
    selection=df[df['hwid']==hwid]
    selection=selection[selection['metric']==metric]
    selection['minute']=selection['when'].map(lambda x: int((x - 1601510422405) / 60000),meta=('when','int64'))

    if aggfunc=='mean':
        g = selection.groupby(['hwid', 'metric', 'minute']).mean().compute().sort_values(by='minute')
    elif aggfunc=='max':
        g = selection.groupby(['hwid', 'metric', 'minute']).max().compute().sort_values(by='minute')
    elif aggfunc=='min':
        g = selection.groupby(['hwid', 'metric', 'minute']).min().compute().sort_values(by='minute')
    elif aggfunc=='sum':
        g = selection.groupby(['hwid', 'metric', 'minute']).sum().compute().sort_values(by='minute')
    else:
        raise ValueError('Non supported aggfunction')

    minutes=list(g.index.get_level_values('minute'))

    return list(g['value']),minutes

def fill_missing_min(values,minutes):

    tmax=int(max(minutes))
    lastvalue=0
    fmin=minutes.copy()
    fvals=values.copy()

    for i in range(1,tmax):
        if i not in minutes:
            fmin.append(i)
            fvals.append(lastvalue)
        else:
            index=minutes.index(i)
            lastvalue=fvals[index]

    _, filled_values = zip(*sorted(zip(fmin, fvals)))

    return list(filled_values)

def check_fault(row,metric):
    return ((row[f'{metric}_6']==1) or (row[f'{metric}_7']==1) or (row[f'{metric}_8']==1))

def getbindigit(value,ind):
    binary = bin(value)[2:].zfill(16)  #convert value to binary, remove '0b' prefix and zero-pad to 16 bits
    return int(binary[16-ind])

def separate_readings(df,hwid,metric):
    selection=df[df['hwid']==hwid]
    selection=selection[selection['metric']==metric]
    for i in range(1,17):
        selection[f'{metric}_{i}']=selection['value'].apply(getbindigit,ind=i,meta=(f'{metric}_{i}','int'))

    selection['minute']=selection['when'].map(lambda x: int((x - 1601510422405) / 60000),meta=('when','int64'))

    agg_dict = {f'{metric}_6': 'max', f'{metric}_7': 'max', f'{metric}_8': 'max'}
    
    g = selection.groupby(['hwid', 'metric', 'minute']).agg(agg_dict).compute().sort_values(by='minute')

    minutes=list(g.index.get_level_values('minute'))
    alarm6 = list(g[f'{metric}_6'])
    alarm7 = list(g[f'{metric}_7'])
    alarm8 = list(g[f'{metric}_8'])

    tmax=int(max(minutes))
    fmin=minutes.copy()
    fvals6=alarm6.copy()
    fvals7=alarm7.copy()
    fvals8=alarm8.copy()

    for i in range(1,tmax):
        if i not in minutes:
            fmin.append(i)
            fvals6.append(0)
            fvals7.append(0)
            fvals8.append(0)

    _, filled_a6, filled_a7, filled_a8 = zip(*sorted(zip(fmin, fvals6, fvals7, fvals8)))

    time = np.arange(0,len(filled_a7))

    new_df = pd.DataFrame({'minute': time, f'{metric}_6': filled_a6, f'{metric}_7': filled_a7, f'{metric}_8': filled_a8})
    
    new_df['fault'] = new_df.apply(check_fault,axis=1, meta=('fault', 'bool'),args=(metric,))
    
    return new_df

#######PLOT FUNCTIONS##########

def plot_machine_state(df,hwid,metric,save_graph=False,show_graph=False,split_graphs=False):

    agg=aux.aggregate_up_down(df,hwid,metric)

    states = [state[0] for state in agg]
    durations = [state[1] for state in agg]

    # Generating the time points
    time_points = [0]
    for duration in durations:
        time_points.append(time_points[-1] + duration)

    colors=['none' if duration > (61/60) else 'r' for duration in durations]

    if split_graphs:
        num_points = len(states)
        quarter = num_points // 4

        # Function to plot each quarter
        def plot_quarter(start, end, ax):
            for i in range(start, end):
                # Plotting the horizontal line for the duration of each state
                ax.step(time_points[i:i+2], [states[i], states[i]], where='post', linestyle='-', marker='o', color=colors[i])
                if i < end - 1:
                    # Plotting the vertical transition line between states
                    ax.plot([time_points[i+1], time_points[i+1]], [states[i], states[i+1]], linestyle='-', color=colors[i])
            ax.set_xlabel('Time')
            ax.set_ylabel('State')
            ax.grid(True)
            ax.set_ylim(-0.5, 1.5)
            ax.set_yticks([0, 1])
            ax.set_yticklabels(['Off', 'On'])

        # Create subplots
        fig, axs = plt.subplots(4, 1, figsize=(10, 20))

        for i in range(4):
            start = i * quarter
            end = (i + 1) * quarter if i < 3 else num_points
            plot_quarter(start, end, axs[i])
            axs[i].set_title(f'Quarter {i+1}')

        fig.suptitle(f'Machine {hwid} metric {metric} State Over Time')

    if not split_graphs:
        # Plotting the states over time
        plt.figure(figsize=(10, 5))

        for i in range(len(states)):
            # Plotting the horizontal line for the duration of each state
            plt.step(time_points[i:i+2], [states[i], states[i]], where='post', linestyle='-', marker='o', color=colors[i])
            if i < len(states) - 1:
                # Plotting the vertical transition line between states
                plt.plot([time_points[i+1], time_points[i+1]], [states[i], states[i+1]], linestyle='-', color=colors[i])

        plt.xlabel('Time')
        plt.ylabel('State')
        plt.title(f'Machine {hwid} metric {metric} State Over Time')
        plt.grid(True)
        plt.ylim(-0.5, 1.5)
        plt.yticks([0, 1], ['Off', 'On'])

    if save_graph:
        plt.savefig(f"Graphs/state_hw{hwid}_metric{metric}")
    if show_graph:
        plt.show()
    plt.close()
