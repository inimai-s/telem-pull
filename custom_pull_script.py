# CUSTOM PULL SCRIPT - W FRACTIONAL EPOCH SUPPORT
# give sxid list, start-end period, epoch in seconds, channels
# draw data for a specific sxid for a specific time period and epoch instead of all the sxid's for the same time period and epoch

from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import telemetron as tron
from scipy import interpolate
import multiprocessing

def get_unixtime(dt64):
    return dt64.astype("datetime64[s]").astype("int")

# pulls data for a specific tron input
def single_asset_pull(tron_inputs):
    try:
        print('Input: ', tron_inputs['sat_name'], ' using: ', multiprocessing.current_process())
        #set up tron
        config = tron.Config(environment="telemetron.satellite.spacex.corp", auth_method="browser")
        client = tron.Client(config)
        structured_telemetry = client.telemetry.search_structured_telemetry(assets=[tron_inputs['sat_name']],
                                                                            channels=tron_inputs['channels'],
                                                                            start_time=tron_inputs['start_time'],
                                                                            end_time=tron_inputs['end_time'],
                                                                            fidelity_interval_nanoseconds=tron_inputs['dt_epoch'],
                                                                            include_min_series=False,
                                                                            include_max_series=True)

        # check to make sure we had a successful pull
        if structured_telemetry.assets == None:
            print(f"List pulled empty for \n{tron_inputs['sat_name']}\n")
            return

        # generate interpolated points
        for asset in structured_telemetry.assets.values():
            # Create a temp data frame for this satellite
            dummy = pd.DataFrame(columns=(tron_inputs['cols']))
            dummy["timestamp"] = tron_inputs['master_time_datetime']

            # run through all channels for this satellite
            for channel in asset.channels.values():
                chan = channel.channel_name
                col = chan.replace(".", "_")

                if type(channel) == tron.models.channel.EnumChannel:
                    time = channel.agg_data.time
                    value = [dat.value for dat in channel.agg_data.data]
                else:
                    time = channel.max_data.time
                    value = channel.max_data.data

                if len(time>=2) and len(value)>=2:

                    f = interpolate.interp1d(
                        time,
                        value,
                        kind="previous",
                        copy=False,
                        bounds_error=False,
                        fill_value=np.nan,
                        assume_sorted=True,
                    )

                    dummy[col] = f(tron_inputs['master_time_epoch'])

            # Store the satellite name in the data frame
            dummy["sat_name"] = np.repeat(asset.asset_name, len(dummy))

            return dummy
    except:
        print('Input: ', tron_inputs['sat_name'], ' failed')


# format tron input for each sat request
def process_sat(sat_name, start_time, end_time, channels, cols, epoch_sec):
    dt_epoch = epoch_sec * 1e9 # epoch_sec in ns

    global_start_epoch = int(start_time.timestamp())
    global_end_epoch = int(end_time.timestamp())
    str_start = start_time.strftime('%Y-%m-%d %H:%M:%S')
    str_end = end_time.strftime('%Y-%m-%d %H:%M:%S')
    
    # setup timeframe of pulls
    master_time_epoch = range(int(global_start_epoch * 1e9),int(global_end_epoch * 1e9),int(dt_epoch))
    master_time_epoch = [float(ep) / 1e9 for ep in master_time_epoch] 
    master_time_datetime = list(map(datetime.fromtimestamp, master_time_epoch))
    
    print(sat_name)

    # pack up inputs
    tron_inputs = {
        'sat_name': sat_name,
        'channels': channels,
        'start_time': str_start,
        'end_time' : str_end,
        'dt_epoch': dt_epoch,
        'cols': cols,
        'master_time_datetime': master_time_datetime,
        'master_time_epoch': master_time_epoch,
                    }
        
    return tron_inputs

        


if __name__ == '__main__':

    # pull_params.csv - csv file with the following columns from left to right:
    # 1. sxid column labeled with heading 'identifier'
    # 2. start time with heading 'start_time' in '%Y-%m-%d %H:%M:%S' format
    # 3. end time with heading 'end_time' in '%Y-%m-%d %H:%M:%S' format
    # 4. epoch between measurement pulls in seconds with heading 'epoch_sec'
    df = pd.read_csv('pull_params.csv')


    # convert string times to datetime objects
    df['start_time'] = pd.to_datetime(df['start_time'], format='%Y-%m-%d %H:%M:%S')
    df['end_time'] = pd.to_datetime(df['end_time'], format='%Y-%m-%d %H:%M:%S')

    # set up multiprocessing
    p = multiprocessing.Pool()
    print('Number of processors in pool: ', p._processes)

    #set up tron
    config = tron.Config(environment="telemetron.satellite.spacex.corp", auth_method="browser")
    client = tron.Client(config)

    # set up input channels - change as appropriate
    channels_new = [
        # 'satfc1x.loads.i_avi_deploy_forward',
        # 'satfc1x.loads.i_kae1_deploy_forward',
        # 'satfc1x.loads.i_kae3_deploy_forward',
        # 'satfc1x.loads.i_kae4_deploy_forward',
        # 'satfc1x.loads.i_lsw_deploy_forward',
        # 'satfc1x.loads.i_propavi1_deploy_forward',
        'satfc1x.gnc.num_burn_attempts_i32'
        ]

    channels = list(set(channels_new))
    cols = [chan.replace(".", "_") for chan in channels]
    output = pd.DataFrame(columns=cols)

    # assert master_channel in channels
    cols.extend(["timestamp", "sat_name"])

    big_tron_inputs = []

    for i in range(len(df['identifier'])):
        tron_input = process_sat(f'satellite{df['identifier'].iloc[i]}', df['start_time'].iloc[i], df['end_time'].iloc[i], channels, cols, float(df['epoch_sec'].iloc[i]))
        big_tron_inputs.append(tron_input)


    dummy = p.map(single_asset_pull, big_tron_inputs)
    p.close()
    p.join()
    output = output._append(dummy)

    output.to_csv('output.csv')

    print('done')
