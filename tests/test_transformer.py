import pytest
import pandas as pd
from src.transformer import Transformer

@pytest.fixture
def config():
    return {
        "transformations": {
            "unit_conversion": {
                "energy_gw": {
                    "factor": 0.001
                }
            },
            "convergence_simulation": {
                "enabled": True,
                "target_column": "energy_gw",
                "max_iterations": 10,
                "tolerance": 0.01,
                "mock_convergence_factor": 1.10 # 10% increase
            }
        }
    }

def test_unit_conversion_and_cleaning(config):
    # Disable simulation for this test
    config["transformations"]["convergence_simulation"]["enabled"] = False
    
    df = pd.DataFrame({
        "energy_gw": [15000, 20000], # MW values to be converted
        "region": [" North_America ", "EUROPE"]
    })
    
    transformer = Transformer(config)
    df_transformed = transformer.transform(df)
    
    # Check unit conversion
    assert list(df_transformed["energy_gw"].values) == [15.0, 20.0]
    
    # Check string cleaning
    assert list(df_transformed["region"].values) == ["north_america", "europe"]

def test_convergence_simulation(config):
    df = pd.DataFrame({
        "energy_gw": [10.0, 20.0],
        "region": ["asia", "africa"]
    })
    
    transformer = Transformer(config)
    df_transformed = transformer.transform(df)
    
    # Check that simulation ran and approximated a 10% increase
    # Initial: 10, 20. Target: 11, 22
    # After 10 iterations it should be very close to 11 and 22
    val1, val2 = df_transformed["energy_gw"].values
    
    assert pytest.approx(val1, rel=0.01) == 11.0
    assert pytest.approx(val2, rel=0.01) == 22.0
