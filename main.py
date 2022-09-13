import zeep
from zeep.helpers import serialize_object
import pandas as pd
import json
import openpyxl
import datetime
from pandas.tseries.offsets import MonthEnd
import sqlalchemy

def getData(engine):
    wsdl = 'https://www.copec.cl/WS_cuponElectronicoAllTrx/services/TransaccionesCEWSS?wsdl'
    client = zeep.Client(wsdl=wsdl)
    lista = []
    with client.settings(strict=False):
        today = datetime.datetime.today()
        today = today.strftime('%Y-%m-%d')
        print('Inicializando extracción de datos.')
        for beg in pd.date_range('2022-08-01', today, freq='MS'):
            fec_ini = beg.strftime("%Y-%m-%d")
            fec_end = (beg + MonthEnd(1)).strftime("%Y-%m-%d")
            response = client.service.transaccionesCE(fec_end,
                                                      fec_ini,
                                                      'linkin22',
                                                      '',
                                                      '',
                                                      '76031431-5',
                                                      '76031431')
            obj = serialize_object(response)
            output = json.loads(json.dumps(obj))

            print(output['PERIODO'][:18])
            lista.extend(output['DETALLE']['item'])
            print('{} datos obtenidos'.format(len(lista)))
        df = pd.DataFrame(lista)
        print(df)
        df.to_sql('ws_cvu_copec', engine, method='multi',
                  schema='public', if_exists='replace', index=False)
        print('Proceso finalizado')



if __name__ == '__main__':
    engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:3oJi7jIoJvKg@10.168.0.5:5432/cvu')
    getData(engine)



