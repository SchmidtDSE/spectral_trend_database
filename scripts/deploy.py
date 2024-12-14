from pathlib import Path
import shutil

# Create a Path object representing the directory you want to create
directory_path = Path("site/docs")

# Create the directory
# directory_path.mkdir(parents=True, exist_ok=True)


print('1.')
source_dir = Path("_build/html")
destination_dir = Path("site")

shutil.copytree(source_dir, destination_dir)

print('2.')
source_dir = Path("docs/_build/html")
destination_dir = Path("site/docs")

shutil.copytree(source_dir, destination_dir)

