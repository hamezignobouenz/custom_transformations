import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from src.config import CONNECTION_STRING

engine = create_engine(CONNECTION_STRING)


def cumulative_nunique(agg, partitions, ordering, table_id):
    partitions_str = ', '.join(partitions)
    agg_str = ', '.join(agg)
    ordering_str = ', '.join(ordering)
    res_query = f"""
    select *, sum(technical_column) over (partition by {agg_str} order by {ordering_str} asc) 
        as nunique_{'_'.join(partitions)}
    from (
        select *, 
        case when row_number() over (partition by {partitions_str} order by {ordering_str}) = 1
         then 1 else 0
        end as technical_column
        from {table_id}
) t1
    """
    return res_query


