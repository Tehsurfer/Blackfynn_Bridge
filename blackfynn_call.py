# blackfynn_get aquires data from the three wrapper and uses it to call the blackfynn
# api in a python 2.7 virtual environment

from blackfynn import Collection, Blackfynn, TimeSeries
import numpy as np
from file_pipe import FilePipe

def blackfynn_get():
    # make a blackfynn API call according to threeWrapper parameters

    fpipe = FilePipe()
    params = fpipe.receive()

    api_key  = params['api_key']
    api_secret = params['api_secret']
    dataset = params['dataset']
    collection = params['collection']
    channels = params['channels']
    window_from_start = params['window_from_start']
    start = params['start']
    end = params['end']
    error = 0

    # Process function input:
    if start == -1 or end == -1:
        has_time_window = False
    else:
        has_time_window = True

    #establish connection with the API
    try:
        bf = Blackfynn(api_token=api_key, api_secret=api_secret)
    except:
        fpipe.send({'error': 'Could not connect to the Blackfynn API. Check your API key and internet connection'})
        return




    # get all timeseries collections
    try:
        ds = bf.get_dataset(dataset)
        tstemp = ds.get_items_by_name(collection)
        time_series = tstemp[0]
    except:
        fpipe.send({'error': 'Could not find the requested Dataset and Collection. Please check your names if you have not already'})
        return

    # Get data for all channels according to the length set by parameters

    if has_time_window == False:
        data = time_series.get_data(length=(str(window_from_start) + 's'))
    else:
        data = time_series.get_data(start=start,end=end)

    # take the data from the channel and process it to be passed into binary (in filePipe)

    # process y values
    list_y = data[channels].values.tolist()

    # generate x values
    if has_time_window:
        time = np.linspace(start, end, len(list_y))
    else:
        time = np.linspace(0, window_from_start, len(list_y))
    list_x = time.tolist()

    cache_dict = create_file_cache(data)
    output = {
        'x': list_x,
        'y': list_y,
        'cache': cache_dict,
        'error': False
        }

    fpipe.send(output)
    print('update ran successfully')

def create_file_cache(data_frame):

    cache_dictionary = {}
    for key in data_frame:
        cache_dictionary[key] = data_frame[key].values.tolist()
    return cache_dictionary








blackfynn_get()