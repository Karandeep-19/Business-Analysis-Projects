from pathlib import Path
import pandas as pd

def run_etl():
    base=Path(__file__).resolve().parents[1]
    raw=base/'data'/'raw'
    bronze=base/'data'/'bronze'
    silver=base/'data'/'silver'
    gold=base/'data'/'gold'
    for p in [bronze,silver,gold]: p.mkdir(parents=True, exist_ok=True)
    orders=pd.read_csv(raw/'orders.csv', parse_dates=['order_date'])
    vendors=pd.read_csv(raw/'vendors.csv')
    orders['total_cost']=orders['quantity']*orders['unit_cost']
    orders.to_csv(bronze/'orders_clean.csv', index=False)
    vendors.to_csv(bronze/'vendors_clean.csv', index=False)
    joined=orders.merge(vendors, on='supplier_id', how='left')
    joined.to_csv(silver/'orders_joined.csv', index=False)
    agg=joined.groupby(['supplier_id','supplier_name','tier']).agg(total_qty=('quantity','sum'), total_spend=('total_cost','sum')).reset_index()
    agg.to_csv(gold/'supplier_spend.csv', index=False)
    print('ETL done')

if __name__=='__main__':
    run_etl()
