###############################################
### Mapping the transactions per Public Key ###
###############################################

## Import Lib's 
import pandas as pd
from pandas.io import gbq
import random

## Insert the BigQuery Project ID
projectid = ""


## Map the 'hacks' to get pubKeys that transacted with 'bad' transactors
# Mt.Gox Hack 
mt_Gox_Hack = gbq.read_gbq('SELECT timestamp, transaction_id, inputs.input_pubkey_base58, outputs.output_satoshis, outputs.output_pubkey_base58 FROM (FLATTEN(DB.transactions_all, outputs.output_satoshis)) WHERE inputs.input_pubkey_base58 = "1LNWw6yCxkUmkhArb2Nf2MPw6vG7u5WG7q"', projectid)
gbq.to_gbq(mt_Gox_Hack, 'DB.1LNWw6yCxkUmkhArb2Nf2MPw6vG7u5WG7q', projectid, if_exists='replace')

# Map the transaction history of all pubKeys that were associated with the Mt.Gox Hack 
for g in mt_Gox_Hack['outputs_output_pubkey_base58']:
    gox_pubKeys = gbq.read_gbq('SELECT timestamp, transaction_id, inputs.input_pubkey_base58, outputs.output_satoshis, outputs.output_pubkey_base58 FROM (FLATTEN(DB.transactions_all, outputs.output_satoshis)) WHERE inputs.input_pubkey_base58 == "{}"'.format(g), projectid)
    gbq.to_gbq(gox_pubKeys, 'DB.{}'.format(g), projectid, if_exists='replace')

## Creating transaction history per pubKey (address)
# Get transaction table form BigQuery
transaction = gbq.read_gbq('SELECT timestamp, transaction_id, inputs.input_pubkey_base58, outputs.output_satoshis, outputs.output_pubkey_base58 FROM (FLATTEN(DB.transactions_all, outputs.output_satoshis)) LIMIT 1250000', projectid)

# 1. loop through each pubKey in transaction table, 
# 2. query for the history for that pubKey,
# 3. save that query as a table in our DB 
for x in transaction['inputs_input_pubkey_base58']:
    pubKey = gbq.read_gbq('SELECT timestamp, transaction_id, inputs.input_pubkey_base58, outputs.output_satoshis, outputs.output_pubkey_base58 FROM (FLATTEN(DB.transactions_all, outputs.output_satoshis)) WHERE inputs.input_pubkey_base58 == "{}"'.format(x), projectid)
    gbq.to_gbq(pubKey, 'DB.{}'.format(x), projectid, if_exists='replace')

    print(x)












