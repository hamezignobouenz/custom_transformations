import unittest
import pandas as pd
from ...src.cumulative_nunique import cumulative_nunique, engine


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
        print(sql_q)
        df = pd.read_sql(sql_q, engine)
        print(df)
        return
