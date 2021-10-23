# User activity

## How to run

1. First you need to build docker images
```bash
make build
```
2. To run parsing
```bash
make run-parsing
```
You can find an output in `./data/output`

3. To run statistics
```bash
make run-stat
```

## Unit tests

```bash
make run-tests
```

## To improve

1. Add validation step(check file exists, the right format, schema etc, clean up the data)
2. Add data cleaning step and drop all the rows that are not satisfied validation criteria
3. Improve job efficiency: play around with different techniques in order to improve efficiency. I was not focusing on making the job run in the most efficient way, but simply make the assignment done.
4. Add more unit tests. I added just simple unit-tests in order to simply show-case it
5. Add more monitoring for each step(how man rows we process etc)
6. Some steps could be simplified and merged, but I wanted to make the logic is clear as possible
7. Add more docstrings and description about methods
