# mr-mm
### MapReduce **matrix** & **vector** multiplication

Tests for checking the correctness of the algorithm were taken from [here](https://www.wolframalpha.com/input?i=matrix+multiplication+calculator).

- [x] Vector multiplication:
```
> python3 vector/__init__.py matrix/examples/short.txt
"vector"        5.0
```

- [x] Square matrix multiplication:
```
> python3 matrix/__init__.py matrix/examples/square.txt
"4:0"   0
"4:1"   0
"4:2"   0
"4:3"   0
"2:0"   3
"2:1"   0
"0:4"   0
"1:0"   10
"1:1"   1
"1:2"   3
"1:3"   5
"1:4"   0
"4:4"   1
"3:1"   4
"3:2"   0
"0:2"   0
"0:3"   0
"2:2"   1
"2:3"   0
"2:4"   0
"3:0"   2
"0:0"   0
"0:1"   0
"3:3"   1
"3:4"   0
```

- [x] Rectangular matrix multiplication:
```
> python3 matrix/__init__.py matrix/examples/rectangular.txt
"1:0"   2
"1:1"   1
"0:1"   0
"0:0"   6
```