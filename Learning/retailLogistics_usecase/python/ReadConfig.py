import json
import os

def load_config(env, config_path="/Workspace/Users/anto2003.sfn@gmail.com/databricks-code-repo/Learning/retailLogistics_usecase/config/retail_config.json"):
    # 1. Get the environment (defaults to 'dev' if not found)
    if env == "dev":
        env = os.getenv("ENV", "dev")
    else:
        env="prod"
    # 2. Load the full file
    with open(config_path, "r") as f:
        full_config = json.load(f)
    # 3. Return only the sub-section for the current env
    print(f"Loading configuration for environment: {env}")
    return full_config.get(env)

# Usage
config = load_config("prod")
# print(config)
# Now you can access variables cleanly
# spark.sql(f"USE CATALOG {config['catalog']}")
# raw_data_path = config['source_path']