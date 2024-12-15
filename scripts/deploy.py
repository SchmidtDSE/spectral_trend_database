from pathlib import Path
import shutil

# Create a Path object representing the directory you want to create
directory_path = Path("./_site/docs")

# Create the directory
# directory_path.mkdir(parents=True, exist_ok=True)


print('1.')
source_dir = Path("_build/html")
destination_dir = Path("./_site")

shutil.copytree(source_dir, destination_dir)

print('2.')
source_dir = Path("docs/_build/html")
destination_dir = Path("./_site/apidocs")

shutil.copytree(source_dir, destination_dir)

