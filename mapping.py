import pandas as pd

def headerMap(name, provider,sys=False):
    # read mapping
    mapping = pd.read_csv('sync_mappings.csv')
    if not sys:
        mapping = mapping.loc[mapping['table'] == name]
    else:
        mapping = mapping.loc[mapping['table'].isin([name,"all"])]
    # drop null columns
    mapping = mapping.dropna(subset=[provider])
    return(mapping)

if __name__ == "__main__":
    source_mapping = headerMap('accounts', 'angaza', sys=True)
    print(source_mapping['angaza'].values.tolist())



