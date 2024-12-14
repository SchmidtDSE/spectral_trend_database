from pathlib import Path

# Create a Path object representing the directory you want to create
directory_path = Path("site")

# Create the directory
directory_path.mkdir(parents=True, exist_ok=True)


f = open("site/index.html", "w")
f.write("<h1>Hello World</h1>\n")
f.write("<p>i will do things one <a href='/other'>day</a></p>")
f.close()

f = open("site/other.html", "w")
f.write("<h1>Hello World</h1>\n")
f.write("<p>i will do things one day</p>")
f.close()