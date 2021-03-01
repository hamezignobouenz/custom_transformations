import unittest
import pandas as pd
from numpy.testing import assert_array_equal
from ...src.cumulative_nunique import cumulative_nunique
from sqlalchemy import create_engine
from ...src.config import CONNECTION_STRING

engine = create_engine(CONNECTION_STRING)


class TestCumulativeNunique(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def test_cumulative_nunique(self):
        agg = ['product']
        partitions = ['product', 'client']
        ordering = ['date']

        sql_q = cumulative_nunique(agg=agg, partitions=partitions, ordering=ordering, table_id='test_nunique')
        df = pd.read_sql(sql_q, engine)
        out_column = 'nunique_product_client'
        assert_array_equal(df[out_column].values, df['res'].values)
        return
