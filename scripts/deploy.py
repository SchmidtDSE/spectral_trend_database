f = open("index.html", "w")
f.write("<h1>Hello World</h1>\n")
f.write("<p>i will do things one <a href='/other'>day</a></p>")
f.close()

f = open("other.html", "w")
f.write("<h1>Hello World</h1>\n")
f.write("<p>i will do things one day</p>")
f.close()