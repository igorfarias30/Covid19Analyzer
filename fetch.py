import pandas as pd
import numpy as np
import os

def fetch(url: str) -> pd:
    """Fetch the CoVid-19 data provide by api hosted on heroku 
        and preprocess to compile it
    
    Arguments:
        url {str} -- url to access the data
    
    Returns:
        pd -- Dataframe with cases
    """
    data = pd.read_json(url, orient='index')
    cases = pd.DataFrame()

    for type_ in data.index[:-1]:
        for country in data.loc[type_, 'locations']:
            
            aux = pd.DataFrame(data = country, columns = country.keys()).reset_index()
            index_ = aux[aux['index'].isin(['lat', 'long'])].index
            aux['lat'], aux['long'] = list(aux.loc[index_, 'coordinates'].values)
            aux.drop(index=index_, columns=['coordinates'], inplace=True)
            aux.rename(columns={'index': 'datetime'}, inplace=True)
            aux.loc[:, 'type'] = type_

            cases = pd.concat([cases, aux])
            cases.reset_index(drop=True, inplace=True)
            del aux

    return cases


if __name__ == '__main__':

    cases = fetch(url="https://coronavirus-tracker-api.herokuapp.com/all")
    
    if not os.path.isdir('dataset'):
        os.mkdir(os.getcwd() + '/dataset')

    cases.to_csv('dataset/covid19_global.csv', index=False, sep=';')