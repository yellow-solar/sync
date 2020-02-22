import pandas as pd

def headerMap(name, provider, sys=False, core = False, zoho = False):
    # read mapping
    mapping = pd.read_csv('syncmappings.csv')
    if sys:
        mapping = mapping.loc[mapping['table'].isin([name,"all"])]
    else:
        mapping = mapping.loc[mapping['table'] == name]
    # filter columns for provider or zoho
    # select columns in zoho
    if zoho:
        mapping = mapping.loc[mapping['zoho']==1]
    # drop null columns
    else:
        mapping = mapping.dropna(subset=[provider]) if not core else mapping
    
    return(mapping)

if __name__ == "__main__":
    source_mapping = headerMap('accounts', 'angaza', sys=True)
    print(source_mapping['angaza'].values.tolist())



