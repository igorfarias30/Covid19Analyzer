import pandas as pd
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fetch(url: str) -> pd:
    """Fetch the CoVid-19 data provide by api hosted on heroku 
        and preprocess to compile it
    
    Arguments:
        url {str} -- url to access the data
    
    Returns:
        pd -- Dataframe with cases
    """
    data = pd.read_json(url, orient='index')
    logger.info("\tdata fetched from heroku")

    cases = pd.DataFrame()

    for type_ in data.index[:-1]:
        for country in data.loc[type_, 'locations']:
            aux = pd.DataFrame(data = country, columns = country.keys()).reset_index()
            index_ = aux[aux['index'].isin(['lat', 'long'])].index
            aux['lat'], aux['long'] = list(aux.loc[index_, 'coordinates'].values)
            aux.drop(index=index_, columns=['coordinates'], inplace=True)
            
            aux.rename(columns={'index': 'datetime'}, inplace=True)
            aux['datetime'] = aux['datetime']\
                .apply(lambda date_string: pd.to_datetime(date_string, dayfirst=True))
            
            aux.loc[:, 'type'] = type_

            cases = pd.concat([cases, aux])
            del aux
        logger.info(f"\tsuccess processing '{type_}' covid-19 cases ")
    
    cases.reset_index(drop=True, inplace=True)
    return cases


if __name__ == '__main__':

    cases = fetch(url="https://coronavirus-tracker-api.herokuapp.com/all")
    
    if not os.path.isdir('dataset'):
        os.mkdir(os.getcwd() + '/dataset')

    cases.to_csv('dataset/covid19_global.csv', index=False, sep=';', date_format='%Y-%m-%d')
