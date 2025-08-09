"""
Example external InLaw test file to demonstrate dynamic import functionality.
"""

from npd_plainerflow import InLaw


class ExternalInLawTest(InLaw):
    """External InLaw test class loaded dynamically."""
    
    title = "External test that always passes"
    
    @staticmethod
    def run(engine):
        """External test that demonstrates dynamic loading."""
        sql = "SELECT 42 as answer"
        gx_df = InLaw.sql_to_gx_df(sql=sql, engine=engine)
        
        result = gx_df.expect_column_values_to_equal(
            column="answer", 
            value=42
        )
        
        if result.success:
            return True
        return f"Answer was not 42"


class AnotherExternalTest(InLaw):
    """Another external test class."""
    
    title = "Another external test"
    
    @staticmethod
    def run(engine):
        """Another external test."""
        sql = "SELECT 'hello' as greeting"
        gx_df = InLaw.sql_to_gx_df(sql=sql, engine=engine)
        
        result = gx_df.expect_column_values_to_not_be_null(
            column="greeting"
        )
        
        if result.success:
            return True
        return "Greeting was null"
