import redis
x = 1
y = 1
num1 = eval("x + y")
print num1

def g():
    x = 2
    y = 2
    num3 = eval("x + y");
    print num3
    # num2 = eval("x + y", globals())
    num2 = eval("x + y", globals(), locals())
    print num2

g()

print locals()["x"]
print locals()["y"] 
print globals()["x"]
print globals()["y"] 