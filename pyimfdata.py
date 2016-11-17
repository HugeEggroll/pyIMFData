# -*- coding: utf-8 -*-
""" pyimfdata

This module demonstrates how to query public datasets from International Monetary Fund(IMF)

Todo:
    * Doc for functions
    * sphinx

"""

import requests
import pandas as pd


def dataflow_method():
    """

    Returns:
        pandas.DataFrame: The return value. First column is the database ID for further
            usage. Second column is the description of each database.

    """

    base_url = 'http://dataservices.imf.org/REST/SDMX_JSON.svc/'
    service_name = 'Dataflow/'
    request_url = ''.join([base_url, service_name])
    raw_request = requests.get(request_url)
    par_request = pd.DataFrame(raw_request.json()['Structure']['Dataflows']['Dataflow'])
    ids = par_request['KeyFamilyRef'].apply(lambda x: x['KeyFamilyID'])
    texts = par_request['Name'].apply(lambda x: x['#text'])
    return pd.DataFrame({'DatabaseID': ids, 'DatabaseText': texts})


def data_structure_method(database_id, check_query=False):
    """

    Returns:
	(list, dict): The return tuple. First element is a list includes available
            code for further usage. The second element is a dict. Each value is a
            pandas.DataFrame that have all possible values for each code and the
            description of each values.
    """


    if check_query:
        available_datasets = dataflow_method()['DatabaseID'].tolist()
        if database_id not in available_datasets:
            return None

    base_url = 'http://dataservices.imf.org/REST/SDMX_JSON.svc/'
    service_name = 'DataStructure/'
    request_url = ''.join([base_url, service_name, database_id])
    raw_request = requests.get(request_url)

    if not raw_request.ok:
        return None

    rparsed = raw_request.json()['Structure']
    dim_code = pd.Series(rparsed['KeyFamilies']['KeyFamily']['Components']['Dimension']).apply(
        lambda x: x['@codelist']).tolist()
    dim_codedict = [x for x in rparsed['CodeLists']['CodeList'] if x['@id'] in dim_code]
    dim_codedict = dict(zip(pd.Series(dim_codedict).apply(lambda x: x['@id']).tolist(),
                            dim_codedict))

    for k in dim_codedict.keys():
        dim_codedict[k] = pd.DataFrame({
            'CodeValue': pd.Series(dim_codedict[k]['Code']).apply(lambda x: x['@value']),
            'CodeText': pd.Series(
                dim_codedict[k]['Code']).apply(lambda x: x['Description']['#text'])})

    return (dim_code, dim_codedict)


def compact_data_method(database_id, queryfilter=None, startdate='2001-01-01',
                        enddate='2001-12-31', checkquery=False, verbose=False):
    """

    Returns:
        pandas.DataFrame: includes all metadata


    """

    if checkquery:
        available_datasets = dataflow_method()['DatabaseID'].tolist()
        if database_id not in available_datasets:
            return None

    queryfilterstr = ''
    if len(queryfilter[1]) > 0 and len(queryfilter[0]) == len(queryfilter[1]):
        queryfilterstr = '.'.join(['+'.join(queryfilter[1][k]) for k in queryfilter[0]])

    base_url = 'http://dataservices.imf.org/REST/SDMX_JSON.svc/'
    service_name = 'CompactData/'
    request_url = ''.join([base_url, service_name, queryfilterstr,
                           '?startPeriod=', startdate, '&endPeriod=', enddate])
    raw_request = requests.get(request_url)

    if verbose:
        print('\nmaking API call:\n')
        print(request_url)
        print('\n')

    if not raw_request.ok:
        return None
    rparsed = raw_request.json()

    if 'Series' not in list(rparsed['CompactData']['DataSet'].keys()):
        print('no data available \n')
        return None

    return_df = [x for x in rparsed['CompactData']['DataSet']['Series'] if 'Obs' in x]
    for one_return in return_df:
        if isinstance(one_return['Obs'], dict):
            one_return['Obs'] = [one_return['Obs']]

    return_df = pd.concat([pd.DataFrame(x) for x in return_df])
    obs_keys = return_df['Obs'].apply(lambda x: [y for y in x.keys()]).tolist()
    obs_keys = set([val for sublist in obs_keys for val in sublist])

    for obs_key in obs_keys:
        return_df[obs_key] = return_df['Obs'].apply(
            lambda x: x[obs_key] if (obs_key in x.keys()) else None)

    del return_df['Obs']
    return_df = return_df.reset_index()

    return return_df
