from clients import get_bear_lake_client
import bear_lake as bl
import polars as pl
from rich import print


def today_date(df: pl.DataFrame) -> pl.date:
    return (
        df["date"][-1]
    )


if __name__ == '__main__':
    bear_lake_client = get_bear_lake_client()

    # factor loadings is polars data frame

    table_info = bear_lake_client.list_tables() #gets the list of tables in the data frame on the cloud

    


    weights_table = bear_lake_client.query(
        bl.table('portfolio_weights')
    )
    
    date = today_date(weights_table)

    weights_table = (
        weights_table
        .sort('ticker', 'date')
        .filter(
            pl.col('date') == date
        )
    )
    
    weights = weights_table['weight'].to_numpy()


    factor_loadings = bear_lake_client.query(
        bl.table('factor_loadings')
        .filter(
        pl.col("date") == date
        )
        .sort('ticker')
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

    merged_data = (
        factor_loadings
        .join(weights_table, on=['date', 'ticker'], how='right'
        )
    )
    factors = ['MTUM', 'QUAL', 'SPY', 'USMV']
    
    factor_matrix = (
        merged_data
        .sort('ticker')
        .select(factors)
        .to_numpy()
    )

    weights_vector = (
        merged_data
        .sort('ticker')
        .select('weight')
        .to_numpy()
        .ravel()
    )

    factor_loadings = weights_vector @ factor_matrix
    
    # print(factor_loadings)
    # print(weights_table)
    # print(merged_data)
    # print(factor_matrix)
    # print(weights_vector)
    print(factor_loadings)

   
    
