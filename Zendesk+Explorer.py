
# coding: utf-8

# In[1]:

#Proprietary code investigating the use of ML to intelligently recommend resources for Issue/Ticket assignment

import requests
import json 

def connect (url):

    thisurl = url
    user = 'adouglas@cpsgpartners.com'
    pwd = 'dr4nd-rEad'


    #HTTP Request

    response = requests.get(thisurl, auth= (user, pwd))
    
    if response.status_code != 200:
        return('Problem detected with response, verify credentials')
        exit() #could've used break()

    else:

            return(response)
        


# In[2]:

#Develop DataStructure

#Dependences pandas, datetime, numpy

def fetchdata(url):

    import pandas as pd
    from datetime import datetime


    testurl = url
    response = connect(testurl)

    data = response.json()
    #get all solved int tickets (no pagination yet -- let's start small)

    #resolve lists
    idlist = []
    ticketstart= []
    assigneeid = []
    ticketend = []
    priority = []
    org_id = []
    csat = []

    for idx,ticket in enumerate(data['tickets']):
        #we only want solved integration tickets
        if ((ticket['status'] == 'closed' or ticket['status'] == 'solved') and ticket['custom_fields'][2]['value'].find('integration') != -1):
            #append ticket to list
            idlist.append(ticket['id']) 
            ticketstart.append(datetime.strptime(ticket['created_at'],   "%Y-%m-%dT%H:%M:%SZ"))
            assigneeid.append(ticket['assignee_id'])
            ticketend.append(datetime.strptime(ticket['updated_at'],  "%Y-%m-%dT%H:%M:%SZ"))
            priority.append(ticket['priority'])
            org_id.append(ticket['organization_id'])
            csat.append(ticket['satisfaction_rating']['score'])

    ticket_data = pd.DataFrame({'Ticket_ID' : idlist,
                                'Ticket_Start_Date': ticketstart,
                                'Assignee_ID': assigneeid,
                                'Solved at': ticketend,
                                'Priority': priority,
                                'Organization_ID': org_id,
                                'Satisfaction_Rating': csat})

    
    #clean up data, add time taken column, scale priority to integers and csat to integers.

    import numpy as np
    from datetime import timedelta

    #Who knew that datetime could be so fucking annoying    
    from datetime import datetime

    ticketend = np.array(ticketend)
    ticketstart = np.array(ticketstart)

    timetaken = np.subtract(ticketend, ticketstart)
    timetakendays = []

    for td in timetaken:
        timetakendays.append(td.days)

    ticket_data['timetakendays'] = timetakendays


    #map priority 1-4 and Satisfaction poor = 0, offered/unoffered = 1, good = 2
    #helper functions
    def priority(priority):
        if priority == 'urgent':
            return 4
        elif priority == 'high':
            return 3
        elif priority == 'normal':
            return 2
        else:
            return 1

    def csat(csat):
        if csat == 'good':
            return 4
        elif csat == 'offered':
            return 3
        elif csat == 'unoffered':
            return 2
        else:
            return 1 #BAD


    prioritylist = list(map(priority, ticket_data['Priority']))
    csatlist = list(map(csat, ticket_data['Satisfaction_Rating']))
    ticket_data['Priority'] = prioritylist
    ticket_data['Satisfaction_Rating'] = csatlist
    
    return ticket_data


# In[8]:

url = 'https://cpsgpartners.zendesk.com/api/v2/tickets.json'
ticket_data = fetchdata(url)


#dataset ready, time to train NN

#DEEP NN to select Assignee ID who is best suited for a particular ticket, predict the completion time and ALSO, expected csat

import numpy as np 
from scipy.special import expit 
import sys

class NeuralNetMLP(object):
    
    def __init__(self, n_output, n_features, n_hidden=30,
                 l1=0.0, l2=0.0, epochs=500, eta=0.001,
                alpha=0.0, decrease_const=0.0, shuffle=True,                  
                 minibatches=1, random_state=None):
        np.random.seed(random_state)
        self.n_output = n_output
        self.n_features = n_features
        self.n_hidden = n_hidden
        self.w1, self.w2 = self._initialize_weights()
        self.l1 = l1
        self.l2 = l2
        self.epochs = epochs
        self.eta = eta
        self.alpha = alpha
        self. decreased_const = decrease_const
        self.shuffle = shuffle
        self.minibatches = minibatches
        
    def _encode_labels(self, y, k):
        
        onehot = np.zero((k, y.shape[0]))
        for idx, val in enumerate(y):
            onehot[val, idx] = 1.0
        return onehot
    
    def _initialize_weight(self):
        
        w1 = np.random.uniform(-1.0, 1.0, size=self.n_hidden *(self.n_features + 1))
        w1 = w1.reshape(self.n_hidden, self.n_features+1)
        w2 = np.random.uniform(-1.0, 1.0, size=self.n_output *(self.n_hidden+1))
        w2 = w2.reshape(self.n_output, self.n_hidden+1)
        
        return w1, w2
    
    def _sigmoid(self, z):
        #theres a cool fnc called expit
        return expit(z)
    
    def _add_bias_unit(self, X, how='column'):
        if how == 'column':
            X_new = np.ones((X.shape[0], X.shape[1]+1))
            x_new[:, 1:] = X
            
        elif how == 'row':
            
            X_new = np.ones((X.shape[0] +1, X.shape[1]))
            x_new[1:, :] = X
            
        else:
            raise AttributeError('how must be column or row')
        
        return X_new
    
    def _feedforward(self, X, w1, w2):
        
        a1 = self._add_bias_unit(X, how= 'column')
        z2 = w1.dot(a1.T)
        a2 = self._sigmoid(z2)
        a2 = self._add_bias_unit(a2, how ='row')
        z3 = w2.dot(a2)
        a3 - self._sigmoid(z3)
        
        return a1, a2, a3, z2, z3
    
    def _L2_reg(self, lambda_, w1, w2):
        
        return(lambda_/2.0) * (np.sum(w1[:, 1:]**2)                              + np.sum(w2[:, 1:]**2))
    
    def _L1_reg(self, lamda_, w1, w2):
        
        
        return(lambda_/2.0) * (np.abs(w1[:, 1:]).sum()                              + np.abs(w2[:, 1:]).sum())
        
    def _get_cost(self, y_enc, output, w1, w2):
        
        #implement backpropagation for cost
        
        sigma3 = a3 - y_enc
        z2 = self._add_bias_unit(z2, how = 'row')
        sigma2 = w2.T.dot(sigma3) * self._sigmoid_gradient(z2)
        sigma = sigma[1:, :]
        grad1 = sigma2.dot(a1)
        grad2 = sigma3.dot(a2.T)
        
        
        #Regularized 
        
        grad1[:, 1:] += (w1[:, 1:] * (self.l1 + self.l2))
        grad2[:, 1:] += (w2[:, 1:] * (self.l1 + self.l2))
        
        return grad1, grad2
    
    def predict (self, X):
        
        a1, a2, a3, z2, z3 = self.feedforward(X, self.w1, self.w2)
        
        y_pred = np.argmax(z3, axis =0)
        return y_pred
    
    def fit(self, X, y, print_progress = False):
        
        self.cost = []
        X_data, y_data = X.copy(), y.copy()
        y_enc = self._encode_labels(y, self.n_output)
        
        delta_w1_prev = np.zeros(self.w1.shape)
        delta_w2_prev = np.zeros(self.w2.shape)
        
        
        for i in range(self.epochs):
            
            #learning rate
            
            self.eta /= (1 + self.decrease_const * i)
            
            if print_progress:
                
                sys.stderr.write(
                                    '\rEpoch: %d/%d' % (i+1, self.epochs))
                
                sys.stderr.flush()
                
            
            if self.shuffle:
                
                idx = np.random.permutation(y_data.shape[0])
                X_data, y_data = X_data[idx], y_data[idx]
                
            
            mini = np.array_split(range(y_data.shape[0]), self.minibatches)
            
            for idx in mini:
                
                #how do we feedforward here?
                
                a1, a2, a3, z2, z3 = self.feedforward(X[idx], self.w1, self.w2)
                
                cost = self._get_cost(y_enc = y_enc[:, idx],
                                      output = a3,
                                      w1 = self.w1,
                                      w2 = self.w2)
                
                self.cost_.append(cost)
                
                
                #compute gradient via backprop
                
                grad1, grad2 = self._get_gradient(a1 = a1, a2 = a2, a3=a3, z2 = z2,
                                                  y_enc = y_enc[:, idx],
                                                  w1 = self.w1,
                                                  w2 = self.w2)
                
                #adjust weights
                
                delta_w1, delta_w2 = self.eta * grad1, self.eta * grad2
                
                self.w1 -= (delta_w1 + (self.alpha * delta_w1_prev))
                self.w1 -= (delta_w2 + (self.alpha * delta_w2_prev))
                delta_w1_prev, delta_w2_prev = delta_w1, delta_w2
                
            return self
        
        

    #SANITIZED BEYOND THIS POINT FOR IP
        
                    
            
            
            
        
        
        
        
    
        
        


# In[22]:

import pandas as pd
X_train = pd.DataFrame({'Assignee_ID': ticket_data['Assignee_ID'].values, 
                       'Priority' : ticket_data['Priority'].values, 
                       'Org_ID': ticket_data['Organization_ID'].values})




# In[71]:




# In[87]:



    


# In[88]:





# In[ ]:





