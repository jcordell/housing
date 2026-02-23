import duckdb

def check_pins():
    con = duckdb.connect("data/sb79_housing.duckdb")
    
    print("=== PARCELS GEOJSON (pin10) ===")
    print(con.execute("SELECT pin10 FROM parcels WHERE pin10 IS NOT NULL LIMIT 5").df())
    
    print("\n=== ASSESSOR UNIVERSE (pin) ===")
    print(con.execute("SELECT pin FROM assessor_universe WHERE pin IS NOT NULL LIMIT 5").df())
    
    con.close()

if __name__ == "__main__":
    check_pins()
