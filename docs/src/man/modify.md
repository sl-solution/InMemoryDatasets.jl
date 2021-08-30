# Transforming data sets

## Introduction

The `modify!` function can be used to transform and modify columns of a data set. Note that the function modifies the data set in-place and operates on actual values (rather than the formatted values). To modify a copy of data we should use the `modify` function. These two functions accept one column of data set and apply the provided functions on the fed column as a vector, this should be compared to `map!/map` functions which apply operations on individual observations.

> Note that `modify!/modify` remove the format of columns as soon as their values are updated by a given transformation.

## Specifying the transformation

The first argument of these two functions is the name of the data set which is going to be modified and the next arguments can be the transform specifications, i.e.

> `modify!(ds, args...)` or `modify(ds, args...)`

The simplest form of `args` is `col => fun` which calls `fun` on `col` as a vector and replaces `col` with the output of the call. `col` can be a column index or a column name. Thus, to replace the value of a column which is called `:x1` in a data set `ds` with their standardised values, we can use the following expression:

> `modify!(ds, :x1 => stdze)`

where `:x1` is a column in `ds`, and `stdze` is a function which subtracts values by their mean and divide them by their standard deviation. If you don't want to replace the column, but instead you like to create a new column based on calling `fun` on `col`, the `col => fun => :newname` (here `:newname` is a name for the new column) form is handy. Thus, to standardised the values of a column, which is called `:x1`, and store them as a new column in the data set, you  can use,

> `modify!(ds, :x1 => stdze => :x1_stdze)`

To modify multiple columns of a data set with the same `fun`, we can use the `cols => fun`, where `cols` is a set of columns, this includes, a vector of columns indices, a vector of column names, a regular expression which selects some of the variables based on their names, or `Between` and `Not` types. When `cols` is referring to multiple columns, `modify!` automatically expands `cols => fun` to `col1 => fun, col2 => fun, ...`, where `col1` is the first column in the selected columns, `col2` is the second one, and so on. Thus to standardised all columns which starts with `x` in a data set, we can use the following expression:

> `modify!(ds, r"^x" => stdze)`

Note that the Julia broadcasting can be also used for specifying `args...`, e.g. something like:

> `[1, 2, 3] .=> [stdze, x -> x .- mean(x), x -> x ./ sum(x)] .=> [:stdze_1, :m_2, :m_3]`

will be translated as:
> `1 => stdze => :stdze_1, 2 => (x -> x .- mean(x)) => :m_2, 3 => (x -> x ./ sum(x)) => :m_3`,

and something like:

> `:x1 .=> [sum, sort] .=> [:x1_sum, :x1_sort]`

will be translated as:

>`:x1 => sum => :x1_sum, :x1 => sort => :x1_sort`.

### Examples

```jldoctest
julia> ds = Dataset(x1 = 1:5, x2 = [-2, -1, missing, 1, 2],
                    x3 = [0.0, 0.1, 0.2, missing, 0.4])
5×3 Dataset
 Row │ x1        x2        x3
     │ identity  identity  identity
     │ Int64?    Int64?    Float64?
─────┼──────────────────────────────
   1 │        1        -2       0.0
   2 │        2        -1       0.1
   3 │        3   missing       0.2
   4 │        4         1   missing
   5 │        5         2       0.4

julia> modify!(ds, 2:3 => sum)
5×3 Dataset
 Row │ x1        x2        x3       
     │ identity  identity  identity
     │ Int64?    Int64?    Float64?
─────┼──────────────────────────────
   1 │        1         0       0.7
   2 │        2         0       0.7
   3 │        3         0       0.7
   4 │        4         0       0.7
   5 │        5         0       0.7

julia> modify!(ds, :x1 => x -> x .- mean(x))
5×3 Dataset
 Row │ x1        x2        x3       
     │ identity  identity  identity
     │ Float64?  Int64?    Float64?
─────┼──────────────────────────────
   1 │     -2.0         0       0.7
   2 │     -1.0         0       0.7
   3 │      0.0         0       0.7
   4 │      1.0         0       0.7
   5 │      2.0         0       0.7
```

## Accessing to modified columns

One of the key features of `modify!/modify` is that these functions have access to all modified/created variable in a single run of the function. It means, every transformation can be done on all columns that have been or updated by `args` arguments or any column which is created by `col => fun => :newname` syntax. In other words, for `args...` from left to right whenever a column is updated or created, the next operation has access to its value (either new or updated values). This will be particularly useful in conjunction with `byrow` which performs row-wise operations.


## Specialised functions

There are two functions in Datasets which are very handy to modify a data set: `byrow`, and `splitter`.

### `byrow`

The `byrow` function is discussed in length in another section as a stand-alone function, however, it can also be used as the `fun` when we want to specify the transformation in `modify!/modify`. The syntax of `byrow` is different from its stand-alone usage in the way that when `byrow` is the `fun` part of `args` in the syntax of `modify!/modify` functions, we don't need to specify `ds` and `cols`, however, every other arguments are the same as the stand-alone usage.

The main feature of `byrow` inside `modify!/modify`  is that it can accept multiple columns as the input argument, opposed to the other functions inside `modify!/modify` which only accept single column. This and the fact that every transformation inside `modify!/modify` has access to modified columns, help to provide complex transformations in a single run of `modify!/modify`.

The form of `args` when `byrow` is the function is similar to other functions with the following exceptions:

* When `cols` refers to multiple columns in `cols => byrow(...)`, `modify!/modify` will create a new column with a names based on the arguments passed to it. The user can provide a custom name by using the `cols => byrow(...) => :newname` syntax.
* When `col` refers to a single column in `col => byrow(...)`, `modify!/modify` will apply operation on single values of the column and replace the column with the new values, i.e. it doesn't create a new column.
* To use broadcasting with `byrow`, i.e. applying the same row-wise operation on multiple columns, the form must be `cols .=> byrow` where `cols` is a vector of column names or column indices (regular expression cannot be used for this purpose).

### `splitter`

`splitter` is also a specialised function which has a single job: splitting a single column which is a `Tuple` of values into multiple columns. It only operates on a single columns and the values inside the column which needs to be split must be in the form of `Tuples`. The form of `args` for `splitter` must be similar to:

> `modify!(ds, col => splitter => [:new_col_1, :new_col_2])`

which means we like to split `col` into two new columns; `:new_col_1` and `:new_col_2`. Here `col` can be a column index or a column name.

> Note, `splitter` produces as many columns as the length of the given new names, i.e. if the user provides fewer names than needed, the output columns will only contain partial components of the input `Tuple`.

### Examples

```jldoctest
julia> body = Dataset(weight = [78.5, 59, 80], height = [160, 171, 183])
3×2 Dataset
 Row │ weight    height   
     │ identity  identity
     │ Float64?  Int64?   
─────┼────────────────────
   1 │     78.5       160
   2 │     59.0       171
   3 │     80.0       183

julia> modify!(body, :height => byrow(x -> (x/100)^2) => :BMI, [1, 3] => byrow(/) => :BMI)
3×3 Dataset
 Row │ weight    height    BMI      
     │ identity  identity  identity
     │ Float64?  Int64?    Float64?
─────┼──────────────────────────────
   1 │     78.5       160   30.6641
   2 │     59.0       171   20.1771
   3 │     80.0       183   23.8884

julia> sale = Dataset(customer = ["Bob Smith", "John Max", "Froon Moore"],
       item1_q1 = [23, 43, 50], item2_q1 = [44, 32, 55],
       item3_q1 = [45, 45, 54])
3×4 Dataset
 Row │ customer     item1_q1  item2_q1  item3_q1
     │ identity     identity  identity  identity
     │ String?      Int64?    Int64?    Int64?   
─────┼───────────────────────────────────────────
   1 │ Bob Smith          23        44        45
   2 │ John Max           43        32        45
   3 │ Froon Moore        50        55        54

julia> modify!(sale, 2:4 => byrow(sum) => :total)
3×5 Dataset
 Row │ customer     item1_q1  item2_q1  item3_q1  total    
     │ identity     identity  identity  identity  identity
     │ String?      Int64?    Int64?    Int64?    Int64?   
─────┼─────────────────────────────────────────────────────
   1 │ Bob Smith          23        44        45       112
   2 │ John Max           43        32        45       120
   3 │ Froon Moore        50        55        54       159

julia> function name_split(x)
           spl = split(x, " ")
           (string(spl[1]), string(spl[2]))
       end
name_split (generic function with 1 method)

julia> modify!(sale, :customer => byrow(name_split),
                     :customer => splitter => [:first_name, :last_name])
3×7 Dataset
 Row │ customer            item1_q1  item2_q1  item3_q1  total     first_name  last_name
     │ identity            identity  identity  identity  identity  identity    identity  
     │ Tuple…?             Int64?    Int64?    Int64?    Int64?    String?     String?   
─────┼───────────────────────────────────────────────────────────────────────────────────
   1 │ ("Bob", "Smith")          23        44        45       112  Bob         Smith
   2 │ ("John", "Max")           43        32        45       120  John        Max
   3 │ ("Froon", "Moore")        50        55        54       159  Froon       Moore

```

In the last example, we use `byrow` to apply `name_split` on each row of `:customer`, and since there is only one column as the input of `byrow`, `modify!` replaces the column with the new values. Also, note that the `modify!` function has access to these new values and we can use `splitter` to split the column into two new columns.
