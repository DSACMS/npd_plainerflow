"""
Test file 1 in test directory.
"""

from plainerflow import InLaw


class DirectoryTest1(InLaw):
    """Test class from directory file 1."""
    
    title = "Directory test 1"
    
    @staticmethod
    def run(engine):
        """Test from directory file 1."""
        sql = "SELECT 100 as hundred"
        gx_df = InLaw.sql_to_gx_df(sql=sql, engine=engine)
        
        result = gx_df.expect_column_values_to_equal(
            column="hundred", 
            value=100
        )
        
        if result.success:
            return True
        return "Value was not 100"
