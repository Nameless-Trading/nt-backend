from clients import get_bear_lake_client
import bear_lake as bl
import polars as pl
from rich import print




if __name__ == '__main__':
    bear_lake_client = get_bear_lake_client()

    # factor loadings is polars data frame

    table_info = bear_lake_client.list_tables() #gets the list of tables in the data frame on the cloud




    weights_table = bear_lake_client.query(
        bl.table('portfolio_weights')
         .sort('date','ticker') # a table that has the portfolio weights for each ticker
    )
    
    weights = weights_table['weight'].to_numpy()

    min_date = (
        weights_table
        .select(
            pl.col("date").min().cast(str)
    ).item()
    )

    max_date = (
        weights_table
        .select(
            pl.col("date").max().cast(str)
        ).item()
    )

        
    factor_loadings = bear_lake_client.query(
        bl.table('factor_loadings')
        .filter(
        (pl.col("date") >= pl.lit(min_date).str.strptime(pl.Date)) & 
        (pl.col("date") <= pl.lit(max_date).str.strptime(pl.Date))
    )
    .sort("date")
    )

    factor_loadings = factor_loadings.select(["ticker", "date", "factor", "loading"])

    factor_loadings = (
    factor_loadings
    .pivot(
        index=["ticker", "date"],     
        columns="factor",             
        values="loading"              
    )
    .sort(["ticker", "date"])         
    )
    
    print(factor_loadings)
    print(weights_table)
    
