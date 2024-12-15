from pathlib import Path
import shutil


print('1.')
source_dir = Path("_build/html")
destination_dir = Path("./_site")

shutil.copytree(source_dir, destination_dir)

print('1b.')
source_dir = Path("myst/nb")
destination_dir = Path("./_site/myst/nb")

shutil.copytree(source_dir, destination_dir)

print('2.')
source_dir = Path("docs/_build/html")
destination_dir = Path("./_site/apidocs")

shutil.copytree(source_dir, destination_dir)

