import pandas as pd

def headerMap(name, provider, sys=False, core = False, zoho = False):
    # read mapping
    mapping = pd.read_csv('syncmappings.csv')
    if not sys:
        mapping = mapping.loc[mapping['table'] == name]
    else:
        mapping = mapping.loc[mapping['table'].isin([name,"all"])]
    # drop null columns
    mapping = mapping.dropna(subset=[provider]) if not core else mapping
    # select columns in zoho
    mapping = mapping.loc[mapping['zoho']==1] if zoho else mapping
    return(mapping)

if __name__ == "__main__":
    source_mapping = headerMap('accounts', 'angaza', sys=True)
    print(source_mapping['angaza'].values.tolist())



