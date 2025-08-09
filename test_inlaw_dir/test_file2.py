"""
Test file 2 in test directory.
"""

from npd_plainerflow import InLaw


class DirectoryTest2(InLaw):
    """Test class from directory file 2."""
    
    title = "Directory test 2"
    
    @staticmethod
    def run(engine):
        """Test from directory file 2."""
        sql = "SELECT 'world' as word"
        gx_df = InLaw.sql_to_gx_df(sql=sql, engine=engine)
        
        result = gx_df.expect_column_values_to_be_in_set(
            column="word", 
            value_set=["world", "hello"]
        )
        
        if result.success:
            return True
        return "Word was not in expected set"
