"""
This is a wrangle file that holds functions for the
acquire & prepare for the cluster zillow exercises and project

"""

import pandas as pd
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
