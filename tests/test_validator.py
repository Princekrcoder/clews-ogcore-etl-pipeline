import pytest
import pandas as pd
from src.validator import Validator

@pytest.fixture
def config():
    return {
        "validation": {
            "required_columns": ["raw_id", "raw_year"],
            "year_column": "raw_year",
            "year_range": {
                "min": 2020,
                "max": 2050
            }
        }
    }

def test_missing_required_columns(config):
    df = pd.DataFrame({
        "raw_id": [1, 2],
        "other_col": ["A", "B"]
    })
    validator = Validator(config)
    
    with pytest.raises(ValueError, match="Missing required columns"):
        validator.validate(df)

def test_missing_values(config):
    df = pd.DataFrame({
        "raw_id": [1, 2, 3],
        "raw_year": [2021, None, 2023]
    })
    validator = Validator(config)
    df_valid = validator.validate(df)
    
    assert len(df_valid) == 2
    assert 2 not in df_valid["raw_id"].values

def test_year_range_validation(config):
    df = pd.DataFrame({
        "raw_id": [1, 2, 3, 4],
        "raw_year": [2019, 2025, 2050, 2051] # 2019 and 2051 should be dropped
    })
    validator = Validator(config)
    df_valid = validator.validate(df)
    
    assert len(df_valid) == 2
    assert list(df_valid["raw_year"].values) == [2025, 2050]
