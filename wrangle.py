"""
This is a wrangle file that holds functions for the
acquire & prepare for the cluster zillow exercises and project

"""

import pandas as pd
import numpy as np
from env import get_db_url

def wrangle_zillow():

    sql = """SELECT *
        FROM properties_2017
        LEFT JOIN predictions_2017 AS pe USING (parcelid)
        LEFT JOIN architecturalstyletype AS arch USING (architecturalstyletypeid)
        LEFT JOIN propertylandusetype USING (propertylandusetypeid)
        LEFT JOIN airconditioningtype USING (airconditioningtypeid)
        LEFT JOIN typeconstructiontype USING (typeconstructiontypeid)
        LEFT JOIN storytype USING (storytypeid)
        LEFT JOIN unique_properties USING (parcelid)
        LEFT JOIN heatingorsystemtype USING (heatingorsystemtypeid)
        WHERE propertylandusetype.propertylandusedesc = 'Single Family Residential'
        AND pe.transactiondate LIKE '2017%%';
    
        """

    return pd.read_sql(sql, get_db_url("zillow"))


#def missing_values(df): [working on this one....]
    """
        A function that takes in a df and returns a new df with columns that 
        count numbers of nulls and the %% of those nulls
    """
    #creating a column that counts all the nulls
       # df['num_rows_missing']= df(axis=1)
    #creating a column that finds what % the nulls are based on dataset count
        #df['pct_rows_missing']= round(df.count(axis=1)* 100 / len(df))
    #return df


def remove_columns(df, cols_to_remove): 
    """
    A function that takes in a list of columns to remove
    Use this by writing 'cols_to_remove = [columns you want gone]' in notebook
    then put df = remove_columns(df, cols_to_remove)
    """
    #cols_to_remove = ['heatingorsystemtypeid','parcelid','storytypeid','typeconstructiontypeid','airconditioningtypeid','propertylandusetypeid','architecturalstyletypeid','id','buildingclasstypeid','buildingqualitytypeid','decktypeid','pooltypeid10','pooltypeid2','pooltypeid7','taxamount','taxdelinquencyflag','taxdelinquencyyear','id']

    df = df.drop(columns=cols_to_remove)
    return df

"""
A funtion that takes in a df and returns the df where for 60% of columns that do not have
nulls and returns rows where 75% of them do not have nulls
"""
def handle_missing_values(df, prop_required_column = .6, prop_required_row = .75):
    threshold = int(round(prop_required_column*len(df.index),0))
    df.dropna(axis=1, thresh=threshold, inplace=True)
    threshold = int(round(prop_required_row*len(df.columns),0))
    df.dropna(axis=0, thresh=threshold, inplace=True)
    return df

def change_types(df):
    #cols_to_change = ['bedroomcnt','calculatedfinishedsquarefeet','finishedsquarefeet12','fips','fullbathcnt','lotsizesquarefeet','rawcensustractandblock','regionidcity','regionidcounty','regionidzip','unitcnt','yearbuilt','structuretaxvaluedollarcnt','taxvaluedollarcnt','assessmentyear','landtaxvaluedollarcnt','censustractandblock']
    df['bedroomcnt'] = df['bedroomcnt'].astype('int')
    df['calculatedfinishedsquarefeet'] = df['calculatedfinishedsquarefeet'].astype('int')
    df['finishedsquarefeet12'] = df['finishedsquarefeet12'].astype('int')
    df['fips'] = df['fips'].astype('int')
    df['fullbathcnt'] = df['fullbathcnt'].astype('int')
    df['lotsizesquarefeet'] = df['lotsizesquarefeet'].astype('int')
    df['rawcensustractandblock'] = df['rawcensustractandblock'].astype('int')
    df['regionidcity'] = df['regionidcity'].astype('int')
    df['regionidzip'] = df['regionidzip'].astype('int')
    df['unitcnt'] = df['unitcnt'].astype('int')
    df['yearbuilt'] = df['yearbuilt'].astype('int')
    df['structuretaxvaluedollarcnt'] = df['structuretaxvaluedollarcnt'].astype('int')
    df['taxvaluedollarcnt'] = df['taxvaluedollarcnt'].astype('int')
    df['assessmentyear'] = df['assessmentyear'].astype('int')
    df['censustractandblock'] = df['censustractandblock'].astype('int')
    df['landtaxvaluedollarcnt'] = df['landtaxvaluedollarcnt'].astype('int')

    return df


