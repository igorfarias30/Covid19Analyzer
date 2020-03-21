import pandas as pd
import os


def process_datetime(date_: str):
    """ Pre process the datetime on dataset
    
    Arguments:
        date_ {str} -- date string
    
    Returns:
        [type] -- date with type as datetime
    """

    month, day, year = date_.split('/')
    return '-'.join([
            '20' + year, 
            '0' + month if int(month) < 10 else month, 
            '0' + day if int(day) < 10 else day
    ])


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
            aux['datetime'] = aux['datetime']\
                .apply(
                    lambda date_string: pd.to_datetime(
                        process_datetime(date_string), 
                        format="%Y-%m-%d", 
                        errors ="coerce"
                    )
                )
            
            aux.loc[:, 'type'] = type_

            cases = pd.concat([cases, aux])
            cases.reset_index(drop=True, inplace=True)
            del aux

    return cases


if __name__ == '__main__':

    cases = fetch(url="https://coronavirus-tracker-api.herokuapp.com/all")
    
    if not os.path.isdir('dataset'):
        os.mkdir(os.getcwd() + '/dataset')

    cases.to_csv('dataset/covid19_global.csv', index=False, sep=';', date_format='%Y-%m-%d')
