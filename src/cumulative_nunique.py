import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from src.config import CONNECTION_STRING

engine = create_engine(CONNECTION_STRING)


def cumulative_nunique(agg, partitions, ordering, table_id):
    res_query = f"""
    select *, sum(technical_column) over (partition by {agg} order by {ordering} asc) 
        as nunique_{'_'.join(partitions)}
    from (
        select *, 
        case when row_number() over (partition by {partitions} order by {ordering}) = 1
         then 1 else 0
        end as technical_column
        from {table_id}
) t1
    """
    return res_query


