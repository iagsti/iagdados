import dagster as dg
import pandas as pd
from .resources import ComprasGovAPIResource


@dg.asset(kinds={"csv"})
def raw_item_data(context: dg.AssetExecutionContext, api: ComprasGovAPIResource):
    item_data = api.get_items(page=1, page_width=500, group_code=70)
    df = pd.DataFrame(item_data)
    

