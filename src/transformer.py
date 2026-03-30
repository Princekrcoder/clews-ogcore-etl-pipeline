import pandas as pd
from typing import Dict, Any
from src.logger import get_logger
from src.exceptions import MapTransformError

logger = get_logger()

class Transformer:
    def __init__(self, transform_rules: Dict[str, Any]):
        self.config = transform_rules
        self.unit_conversions = self.config.get("unit_conversion", {})
        
        self.sim_settings = self.config.get("convergence_simulation", {})
        self.target_column = self.sim_settings.get("target_column", "energy_gw")
        self.max_iterations = self.sim_settings.get("max_iterations", 10)
        self.tolerance = self.sim_settings.get("tolerance", 0.01)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Starting data transformations.")
        try:
            df = df.copy()
            
            for col, conversion_rule in self.unit_conversions.items():
                if col in df.columns:
                    factor = conversion_rule.get("factor", 1.0)
                    logger.info(f"Converting units for {col} using factor {factor}")
                    df[col] = df[col] * factor
                    
            string_cols = df.select_dtypes(include=['object']).columns
            for col in string_cols:
                df[col] = df[col].astype(str).str.strip().str.lower()
                
            logger.info("Data cleaning and basic transformations complete.")
            
            df = self._simulate_convergence(df)
            
            return df
        except Exception as e:
            raise MapTransformError(f"Error during transformation process: {e}")

    def _simulate_convergence(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.target_column not in df.columns:
            logger.info(f"Target column '{self.target_column}' for convergence not found. Skipping simulation.")
            return df

        if not self.sim_settings.get("enabled", False):
            logger.info("Convergence simulation is disabled in config. Skipping.")
            return df
            
        logger.info(f"Starting convergence simulation loop on '{self.target_column}'.")
        
        iteration = 0
        diff = float('inf')
        
        convergence_factor = self.sim_settings.get("mock_convergence_factor", 1.05)
        current_values = df[self.target_column].copy()
        target_values = current_values * convergence_factor
        
        while iteration < self.max_iterations and diff > self.tolerance:
            iteration += 1
            prev_values = current_values.copy()
            current_values = current_values + (target_values - current_values) * 0.5
            
            abs_diff = (current_values - prev_values).abs()
            percentage_diff = abs_diff / (prev_values + 1e-9)
            diff = percentage_diff.max()
            
            logger.info(f"Convergence Iteration {iteration}: Max Diff = {diff:.4f}")
            
        df[self.target_column] = current_values
        
        if diff <= self.tolerance:
            logger.info(f"Simulation converged after {iteration} iterations on '{self.target_column}'.")
        else:
            logger.warning(f"Simulation reached max iterations ({self.max_iterations}) without converging.")
            
        return df
