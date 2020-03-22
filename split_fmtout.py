import pandas as pd

fname = 'FMTOUT.csv'
fmtout = pd.read_csv(fname)

fmtout.loc[fmtout.FMTNAME=='I94PRTL',:]
fmtout.loc[fmtout.FMTNAME=='I94CNTYL',:].to_csv('i94cit_res.csv',index=False)
fmtout.loc[fmtout.FMTNAME=='I94ADDRL',:].to_csv('i94addr.csv',index=False)
fmtout.loc[fmtout.FMTNAME=='I94MODEL',:].to_csv('i94mode.csv',index=False)
fmtout.loc[fmtout.FMTNAME=='I94VISA',:].to_csv('i94visa.csv',index=False)

# Split the location into city and state.
i94port['city'] = i94port['LABEL'].apply(lambda x: x.split(',')[0] if ',' in x else '')
i94port['state'] = i94port['LABEL'].apply(lambda x: x.split(',')[1] if ',' in x else '')

i94ports.to_csv('i94port.csv',index=False)