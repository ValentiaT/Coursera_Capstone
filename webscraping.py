# import all libraries
import requests
import os
import codecs
import pandas as pd
import numpy as np

from bs4 import BeautifulSoup

def get_wikipedia(wikipage):
    """
        This function does a HTTP get request to retrieve the wikipedia page. 
    """
    page = requests.get(wikipage).text
    soup = BeautifulSoup(page, 'html5lib')

    return soup

def convert_table(html_soup, name='wiki_table', return_df=True):
    """
       This function converts the BeautifulSoup html object 
       to a pandas dataframe, saves the resulting table to a csv file. 
       
    """
    tables = html_soup.findAll("table", { "class" : "wikitable" })
    for tn in range(len(tables)):
        table=tables[tn]
        # Initialize list of lists
        rows=table.findAll("tr")
        row_lengths=[len(r.findAll(['th','td'])) for r in rows]
        ncols=max(row_lengths)
        nrows=len(rows)
        data=[]
        for i in range(nrows):
            rowD=[]
            for j in range(ncols):
                rowD.append('')
            data.append(rowD)

        # processing the html
        for i in range(len(rows)):
            row=rows[i]
            rowD=[]
            cells = row.findAll(["td","th"])
            for j in range(len(cells)):
                cell=cells[j]

                #lots of cells span cols and rows so lets deal with that
                col_span=int(cell.get('colspan',1))
                row_span=int(cell.get('rowspan',1))
                for k in range(row_span):
                    for l in range(col_span):
                        data[i+k][j+l]+=cell.text

            data.append(rowD)

        # write data to a file
            page=name.split('/')[-1]
        fname='table_{}_{}.csv'.format(tn, page)
        f = codecs.open(fname, 'w')
        for i in range(nrows):
            rowStr=','.join(data[i])
            rowStr=rowStr.replace('\n','')
            f.write(rowStr+'\n')    
    
    f.close()
    
    if return_df:
        return pd.read_csv(fname)
    
    return fname
def postal_codes(raw_df):
    """
        This function replaces the 'Not assigned' entries with Not a Number
        and than those entries are filled in with the Borough column entries.
        Returns: a dataframe grouped by Postcode and Borough.
    """
    postal_codes = raw_df.replace(to_replace='Not assigned', value=np.nan)
    
    postal_codes['Neighbourhood'] = postal_codes.Neighbourhood.fillna(postal_codes["Borough"])
    
    postal_codes_df = (postal_codes
            .dropna(axis=0)
            .sort_values('Neighbourhood')
            .groupby(['Postcode', 'Borough'], 
                     as_index=False,
                     sort=False
                    )['Neighbourhood']
            .agg(lambda col: ', '.join(col)))
    
    return postal_codes_df     

def web_scraping_pipeline(page):
    """
        This function calls the above functions that form a web scraping pipeline. 
    """
    page_html = get_wikipedia(page)
    table_df = convert_table(page_html, return_df=True)
    return postal_codes(table_df)    