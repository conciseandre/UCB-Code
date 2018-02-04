
# coding: utf-8

# In[1]:

import os
import pickle
import numpy as np
import pandas as pd
import quandl
from datetime import datetime as dt
import plotly.offline as py
import plotly.graph_objs as go
import plotly.figure_factory as ff
py.init_notebook_mode(connected = True)


# In[2]:

#Define Quandl Helper Function to Get Data

def get_quandl_data(quandl_id):
    '''Download and cache Quandl dataseries'''
    cache_path = '{}.pkl'.format(quandl_id).replace('/','-')
    quandl.ApiConfig.api_key ='7HhQLaTfqvSEWKWUUyaJ'


    print('Downloading {} from Quandl'.format(quandl_id))
    df = quandl.get(quandl_id, returns="pandas")
    df.to_pickle(cache_path)
    print('Cached {} at {}'.format(quandl_id, cache_path))
    return df


# In[3]:

btc_usd_price_kraken = get_quandl_data('BCHARTS/KRAKENUSD')
btc_usd_price_kraken.tail()


# In[4]:

#plot

btc_trace = go.Scatter(x=btc_usd_price_kraken.index, y=btc_usd_price_kraken['Weighted Price'])
py.iplot([btc_trace])


# In[5]:

# Pull pricing data for 3 more BTC exchanges
exchanges = ['COINBASE','BITSTAMP','ITBIT']

exchange_data = {}

exchange_data['KRAKEN'] = btc_usd_price_kraken

for exchange in exchanges:
    exchange_code = 'BCHARTS/{}USD'.format(exchange)
    btc_exchange_df = get_quandl_data(exchange_code)
    exchange_data[exchange] = btc_exchange_df


# In[6]:

def merge_dfs_on_column(dataframes, labels, col):
    '''Merge a single column of each dataframe into a new combined dataframe'''
    series_dict = {}
    for index in range(len(dataframes)):
        series_dict[labels[index]] = dataframes[index][col]
        
    return pd.DataFrame(series_dict)


# In[7]:

# Merge the BTC price dataseries' into a single dataframe
btc_usd_datasets = merge_dfs_on_column(list(exchange_data.values()), list(exchange_data.keys()), 'Weighted Price')


# In[8]:

label_arr = list(btc_usd_datasets)
series_arr = list(map(lambda col: btc_usd_datasets[col], label_arr))
btc_usd_datasets.tail()


# In[9]:

def df_scatter(df, title, seperate_y_axis=False, y_axis_label='', scale='linear', initial_hide=False):
    '''Generate a scatter plot of the entire dataframe'''
    label_arr = list(df)
    series_arr = list(map(lambda col: df[col], label_arr))
    
    layout = go.Layout(
        title=title,
        legend=dict(orientation="h"),
        xaxis=dict(type='date'),
        yaxis=dict(
            title=y_axis_label,
            showticklabels= not seperate_y_axis,
            type=scale
        )
    )
    
    y_axis_config = dict(
        overlaying='y',
        showticklabels=False,
        type=scale )
    
    visibility = 'visible'
    if initial_hide:
        visibility = 'legendonly'
        
    # Form Trace For Each Series
    trace_arr = []
    for index, series in enumerate(series_arr):
        trace = go.Scatter(
            x=series.index, 
            y=series, 
            name=label_arr[index],
            visible=visibility
        )
        
        # Add seperate axis for the series
        if seperate_y_axis:
            trace['yaxis'] = 'y{}'.format(index + 1)
            layout['yaxis{}'.format(index + 1)] = y_axis_config    
        trace_arr.append(trace)

    fig = go.Figure(data=trace_arr, layout=layout)
    py.iplot(fig)


# In[10]:

# Plot all of the BTC exchange prices
df_scatter(btc_usd_datasets, 'Bitcoin Price (USD) By Exchange')


# In[11]:

#clean data

btc_usd_datasets.replace(0, np.nan, inplace=True)

df_scatter(btc_usd_datasets, 'Bitcoin Price (USD) By Exchange')


# In[12]:

#Mean BTC Price

btc_usd_datasets['avg_btc_price_usd'] = btc_usd_datasets.mean(axis=1)
btc_trace = df_scatter(btc_usd_datasets, 'BitcoinPrice')


# In[13]:

#Get Altcoins - Including ETH

def get_json_data(json_url, cache_path):
    
    '''Download JSON DATA AS DF'''
    
   
    print('Downloading {}' . format(json_url))
    df = pd.read_json(json_url)
    df.to_pickle(cache_path)
    print('Cached {} at {}'.format(json_url, cache_path))
            
    return df
    
    


# In[14]:

base_polo_url = 'https://poloniex.com/public?command=returnChartData&currencyPair={}&start={}&end={}&period={}'
start_date = dt.strptime('2015-01-01', '%Y-%m-%d') # get data from the start of 2015
end_date = dt.now() # up until today
period = 86400 # pull daily data (86,400 seconds per day)

def get_crypto_data(poloniex_pair):
    '''Retrieve cryptocurrency data from poloniex'''
    json_url = base_polo_url.format(poloniex_pair, start_date.timestamp(), end_date.timestamp(), period)
    data_df = get_json_data(json_url, poloniex_pair)
    data_df = data_df.set_index('date')
    return data_df
altcoins = ['ETH','OMG','XRP','VTC','OMG','REP','DASH','STRAT','POT']

altcoin_data = {}
for altcoin in altcoins:
    coinpair = 'BTC_{}'.format(altcoin)
    crypto_price_df = get_crypto_data(coinpair)
    altcoin_data[altcoin] = crypto_price_df


# In[15]:

altcoin_data['POT'].tail()


# In[16]:

#convert to USD

for altcoin in altcoin_data.keys():
    altcoin_data[altcoin]['price_usd'] = altcoin_data[altcoin]['weightedAverage']*btc_usd_datasets['avg_btc_price_usd']


# In[17]:

combined_df = merge_dfs_on_column(list(altcoin_data.values()), list(altcoin_data.keys()), 'price_usd')


# In[18]:

combined_df['BTC'] = btc_usd_datasets['avg_btc_price_usd']
combined_df.tail()


# In[19]:

# Chart all of the altocoin prices
df_scatter(combined_df, 'Cryptocurrency Prices (USD)', seperate_y_axis=False, y_axis_label='Coin Value (USD)', scale='log')


# In[20]:

# Calculate the pearson correlation coefficients for cryptocurrencies in 2016
combined_df_2016 = combined_df[combined_df.index.year == 2016]
combined_df_2016.pct_change().corr(method='pearson')
combined_df_2017 = combined_df[combined_df.index.year == 2017]
combined_df_2017.pct_change().corr(method='pearson')


# In[21]:

def correlation_heatmap(df, title, absolute_bounds=True):
    '''Plot a correlation heatmap for the entire dataframe'''
    heatmap = go.Heatmap(
        z=df.corr(method='pearson').as_matrix(),
        x=df.columns,
        y=df.columns,
        colorbar=dict(title='Pearson Coefficient'),
    )
    
    layout = go.Layout(title=title)
    
    if absolute_bounds:
        heatmap['zmax'] = 1.0
        heatmap['zmin'] = -1.0
        
    fig = go.Figure(data=[heatmap], layout=layout)
    py.iplot(fig)


# In[22]:

correlation_heatmap(combined_df_2016.pct_change(), "Cryptocurrency Correlations in 2016")
correlation_heatmap(combined_df_2017.pct_change(), "Cryptocurrency Correlations in 2017")


# In[23]:

#Let's see if we can predict this
import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import datetime
from subprocess import check_output
from keras.layers.core import Dense, Activation, Dropout
from keras.layers.recurrent import LSTM
from keras.models import Sequential
from sklearn.cross_validation import  train_test_split
import time #helper libraries
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt
from numpy import newaxis



# In[24]:

scaler = MinMaxScaler(feature_range =(0,1))
BTCDataset = combined_df['POT']
BTCDataset = BTCDataset.dropna()
BTCPrices = scaler.fit_transform(BTCDataset.values.reshape(-1,1)) 


# In[25]:

train_size = int(len(BTCPrices) * 0.80)
test_size = len(BTCPrices) - train_size
train, test = BTCPrices[0:train_size, :], BTCPrices[train_size:len(BTCPrices),:]
print('Train Size: {} and Test Size: {}'.format(len(train), len(test)))


# In[26]:

#Convert Array into datasex Matrix

def cre_dataset(dataset, look_back = 1):
    X, Y = [], []
    for i in range(len(dataset) - look_back - 1):
        a = dataset[i:(i+look_back), 0]
        X.append(a)
        Y.append(dataset[i + look_back, 0])
    return np.array(X), np.array(Y)


# In[27]:

#reshappe array to matrices (probably could've been done with Pandas)

look_back = 1

trainX, trainY = cre_dataset(train, look_back)
testX, testY = cre_dataset(test, look_back)


# In[28]:

#Reshape 
trainX = np.reshape(trainX, (trainX.shape[0], 1, trainX.shape[1]))
testX = np.reshape(testX, (testX.shape[0], 1, testX.shape[1]))

print('Shape of Train X: {} and Train Y: {}'.format(trainX.shape, testX.shape))


# In[29]:

#Step 2 Build Model
model = Sequential()

model.add(LSTM(
    input_shape=(None, 1),
    units=100,
    return_sequences=True))
model.add(Dropout(0.2))

model.add(LSTM(
    100,
    return_sequences=False))
model.add(Dropout(0.2))

model.add(Dense(units=1))
model.add(Activation('linear'))

start = time.time()
model.compile(loss='mse', optimizer='rmsprop')
print ('compilation time : ', time.time() - start)


# In[30]:

model.fit(
    trainX,
    trainY,
    batch_size=128,
    epochs=20,
    validation_split=0.05)


# In[31]:

predicted_y = model.predict(testX)
scaled_predicted_y = scaler.inverse_transform(np.array(predicted_y))
Output= pd.DataFrame({'Predicted' : scaled_predicted_y[:,0]},index = BTCDataset.index[train_size + 2:len(BTCDataset)])  
scaled_y_test =scaler.inverse_transform(np.array(testY))
Output['Actual'] = pd.DataFrame({'Actual': scaled_y_test}, index=BTCDataset.index[train_size + 2:len(BTCDataset)])
Output.tail()




# In[32]:


df_scatter(Output, 'Potcoin Price (USD) By Prediction')


# In[33]:

#predict lenght consecutive values from a real one
def predict_sequences_multiple(model, firstValue,length):
    prediction_seqs = []
    curr_frame = firstValue
    
    for i in range(length): 
        predicted = []        
        
        print(model.predict(curr_frame[newaxis,:,:]))
        predicted.append(model.predict(curr_frame[newaxis,:,:])[0,0])
        
        curr_frame = curr_frame[0:]
        curr_frame = np.insert(curr_frame[0:], i+1, predicted[-1], axis=0)
        
        prediction_seqs.append(predicted[-1])
        
    return prediction_seqs


# In[34]:

predict_length=2
predictions = predict_sequences_multiple(model, testX[-1], predict_length)
predictions = scaler.inverse_transform(np.array(predictions).reshape(-1, 1))
print(predictions)


# In[35]:

Output2 = pd.DataFrame(predictions,columns = ['Predicted'], index=[datetime.date.today() + datetime.timedelta(days=1), datetime.date.today() + datetime.timedelta(days=2)])
print(datetime.date.today() + datetime.timedelta(days=1))


# In[36]:

Output2 = Output.append(Output2)
#Output2.tail()
df_scatter(Output2, 'Bitcoin Price (USD) By Prediction')


# In[ ]:



