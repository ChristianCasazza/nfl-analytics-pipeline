import os
from pathlib import Path

# Get the path to the root directory (one level up from the script)
root_dir = Path(__file__).resolve().parent.parent

# Construct relative paths from the root directory
dbt_model_paths = root_dir / "transformations" / "dbt" / "models"
evidence_path = root_dir / "app" / "sources" / "app"

# Ensure the evidence_path exists
os.makedirs(evidence_path, exist_ok=True)

# Loop through all .sql files in dbt_model_paths
for file_name in os.listdir(dbt_model_paths):
    if file_name.endswith(".sql"):
        # Extract the base file name without extension
        base_name = os.path.splitext(file_name)[0]
        
        # Create the content for the new .sql file
        content = f"select * from {base_name};\n"
        
        # Determine the path for the new file in evidence_path
        new_file_path = evidence_path / file_name
        
        # Write the content to the new .sql file
        with open(new_file_path, "w") as new_file:
            new_file.write(content)

        print(f"Created: {new_file_path}")
