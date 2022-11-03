# mr-mm
### MapReduce **matrix** & **vector** multiplication

- [x] Vector:
```
> python3 vector/__init__.py matrix/examples/short.txt
"vector"        5.0
```

- [x] Square matrix:
```
> python3 matrix/__init__.py matrix/examples/square.txt
"matrix"        "0,0,1,0,0|0,0,0,1,0|0,0,0,0,1|0,1,0,0,0|0,0,0,0,1"
```

- [ ] Rectangular matrix:
```
> python3 matrix/__init__.py matrix/examples/rectangular.txt
"matrix" = "0,0,1,0,0|0,0,0,1,0|0,0,0,0,1|0,1,0,0,0|0,0,0,0,1"
```

- [ ] Non-valid matrix:
```
> python3 matrix/__init__.py matrix/examples/invalid.txt
"matrix" = "0,0,1,0,0|0,0,0,1,0|0,0,0,0,1|0,1,0,0,0|0,0,0,0,1"
```