from testcontainers.postgres import PostgresContainer
import sqlalchemy

def test_postgres_container():
    try:
        with PostgresContainer("postgres:15") as postgres:
            engine = sqlalchemy.create_engine(postgres.get_connection_url())
            with engine.connect() as conn:
                conn.execute(sqlalchemy.text("CREATE TABLE foo (id INT);"))
                conn.execute(sqlalchemy.text("INSERT INTO foo VALUES (1);"))
                result = conn.execute(sqlalchemy.text("SELECT * FROM foo;")).fetchall()

                if result[0][0] == 1:
                    print("✅ Success: Value inserted and retrieved correctly.")
                else:
                    print("❌ Failure: Value mismatch.")
                    assert False, f"Expected 1, got {result[0][0]}"
    except Exception as e:
        print(f"❌ Exception occurred during test: {e}")
        raise
