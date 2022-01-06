# Reshape data sets

## Introduction

In InMemoryDatasets the reshaping of a data set is done by the `transpose` function.
In the simplest case, you can think about a data set as a matrix, and `transpose` simply flips it over its diagonal; that is, `transpose` switches the row and column indices of it. The key feature that makes `transpose`  versatile and powerful is its ability to do the simple transposing within each group of observations created by `groupby!`, `groupby` or `gatherby`. Basically, the two popular functions for reshaping data, `stack` and `unstack`, are special cases of `transpose`; The `stack` function transposes each row of a data set, and the `unstack` function transposes one column of a data set for each group of observations.

> By default, the `transpose` function uses parallel algorithms to perform the transposing, however, this can be switched to single threaded process by setting the `threads` keyword argument to `false`.

In this section we give the details of reshaping a data set using the `transpose` function.

## Simple case

Transpose means switching the row and column indices of a matrix, and in InMemoryDatasets when we select a set of columns, we practically have a matrix shape array of data, thus, its transposition means switching the row and column indices of it. Thus, in the simplest form, the syntax of the `transpose` function is `transpose(ds, cols)`, where `ds` is the input data set and `cols` is any kind of column selector which specifies the selected columns for transposing. Since each column of a data set has also a name, `transpose` creates a new column in the output data set which shows those names from the input data set. By default, the column name for this new column is set to `_variables_`, and it can be change to any name by using the keyword argument `variable_name`. The `transpose` function has a keyword argument `renamerowid`, which can accept a user defined function to apply on the values of this new added column. Additionally, the keyword argument can be set to `nothing` if the new column is not needed in the output data set.

The rows of a data set haven't got names, thus, InMemoryDatasets uses automatic name generation to produce the names for the transposed columns of the output data set. User can supply a custom function for generating the names of the transposed columns by using the keyword argument `renamecolid`, which by default is set as `i -> "_c" * string(i)` where `i` is the sequence of columns. User can also set the `id` keyword argument to a column in the input data set when the values of the column can be used as the names for the transposed columns in the output data set. By default, the `id` keyword argument uses the formatted values as the names of the transposed columns, however, this can be turned off by using the `mapformats = false` keyword argument.

### Examples

```jldoctest
julia> ds = Dataset(x1 = [1,2,3,4], x2 = [1,4,9,16])
4×2 Dataset
 Row │ x1        x2       
     │ identity  identity
     │ Int64?    Int64?   
─────┼────────────────────
   1 │        1         1
   2 │        2         4
   3 │        3         9
   4 │        4        16

julia> transpose(ds, 1:2)
2×5 Dataset
 Row │ _variables_  _c1       _c2       _c3       _c4      
     │ identity     identity  identity  identity  identity
     │ String?      Int64?    Int64?    Int64?    Int64?   
─────┼─────────────────────────────────────────────────────
   1 │ x1                  1         2         3         4
   2 │ x2                  1         4         9        16

julia> transpose(Matrix(ds)) # transpose of a data set, in the simplest case, is similar to matrix transposition
2×4 transpose(::Matrix{Union{Missing, Int64}}) with eltype Union{Missing, Int64}:
 1  2  3   4
 1  4  9  16

julia> insertcols!(ds, 1, :id => ["r1", "r2", "r3" , "r4"])
4×3 Dataset
 Row │ id        x1        x2       
     │ identity  identity  identity
     │ String?   Int64?    Int64?   
─────┼──────────────────────────────
   1 │ r1               1         1
   2 │ r2               2         4
   3 │ r3               3         9
   4 │ r4               4        16

julia> transpose(ds, [:x1, :x2], id = :id)
2×5 Dataset
 Row │ _variables_  r1        r2        r3        r4       
     │ identity     identity  identity  identity  identity
     │ String?      Int64?    Int64?    Int64?    Int64?   
─────┼─────────────────────────────────────────────────────
   1 │ x1                  1         2         3         4
   2 │ x2                  1         4         9        16

julia> transpose(ds, [:x1, :x2], renamecolid = x -> "_COLUMN_" * string(x))
2×5 Dataset
 Row │ _variables_  _COLUMN_1  _COLUMN_2  _COLUMN_3  _COLUMN_4
     │ identity     identity   identity   identity   identity  
     │ String?      Int64?     Int64?     Int64?     Int64?    
─────┼─────────────────────────────────────────────────────────
   1 │ x1                   1          2          3          4
   2 │ x2                   1          4          9         16

julia> transpose(ds, [:x1, :x2], renamerowid = x -> replace(x, "x"=>""))
2×5 Dataset
 Row │ _variables_  _c1       _c2       _c3       _c4      
     │ identity     identity  identity  identity  identity
     │ String?      Int64?    Int64?    Int64?    Int64?   
─────┼─────────────────────────────────────────────────────
   1 │ 1                   1         2         3         4
   2 │ 2                   1         4         9        16

julia> transpose(ds, [:x1, :x2], id = :id, variable_name = nothing)
2×4 Dataset
 Row │ r1        r2        r3        r4       
     │ identity  identity  identity  identity
     │ Int64?    Int64?    Int64?    Int64?   
─────┼────────────────────────────────────────
   1 │        1         2         3         4
   2 │        1         4         9        16

julia> ds2 = Dataset(a=["x", "y"], b=[1, "two"], c=[3, 4], d=[true, false])
2×4 Dataset
 Row │ a         b         c         d        
     │ identity  identity  identity  identity
     │ String?   Any       Int64?    Bool?    
─────┼────────────────────────────────────────
   1 │ x         1                3      true
   2 │ y         two              4     false

julia> transpose(ds2, Between(:b, :d), id = :a) # promoting the values
3×3 Dataset
 Row │ _variables_  x         y        
     │ identity     identity  identity
     │ String?      Any       Any      
─────┼─────────────────────────────────
   1 │ b            1         two
   2 │ c            3         4
   3 │ d            true      false
```

## `transpose` of grouped data sets

When the first argument of the `transpose` function is a grouped data set - created by `groupby!`, `groupby`, or `gatherby` - `transpose` does the simple transposing within each group of observations. Thus, the transposition of a grouped data set can be viewed as transposing the matrix shape data values which are created for each group of observations. Since the size of transposed columns within each group can be different, `transpose` pads them with `missing` values to overcome this problem. The `missing` padding can be replaced by any other values which passed to `default` keyword argument of the function.

```jldoctest
julia> ds = Dataset(group = repeat(1:3, inner = 2),
                                    b = repeat(1:2, inner = 3),
                                    c = repeat(1:1, inner = 6),
                                    d = repeat(1:6, inner = 1),
                                    e = string.('a':'f'))
6×5 Dataset
 Row │ group     b         c         d         e        
     │ identity  identity  identity  identity  identity
     │ Int64?    Int64?    Int64?    Int64?    String?  
─────┼──────────────────────────────────────────────────
   1 │        1         1         1         1  a
   2 │        1         1         1         2  b
   3 │        2         1         1         3  c
   4 │        2         2         1         4  d
   5 │        3         2         1         5  e
   6 │        3         2         1         6  f

julia> transpose(groupby(ds, :group), 2:4)
9×4 Dataset
 Row │ group     _variables_  _c1       _c2      
     │ identity  identity     identity  identity
     │ Int64?    String?      Int64?    Int64?   
─────┼───────────────────────────────────────────
   1 │        1  b                   1         1
   2 │        1  c                   1         1
   3 │        1  d                   1         2
   4 │        2  b                   1         2
   5 │        2  c                   1         1
   6 │        2  d                   3         4
   7 │        3  b                   2         2
   8 │        3  c                   1         1
   9 │        3  d                   5         6

julia> transpose(groupby(ds, :group), 2:4, id = :e)
9×8 Dataset
 Row │ group     _variables_  a         b         c         d         e         f        
     │ identity  identity     identity  identity  identity  identity  identity  identity
     │ Int64?    String?      Int64?    Int64?    Int64?    Int64?    Int64?    Int64?   
─────┼───────────────────────────────────────────────────────────────────────────────────
   1 │        1  b                   1         1   missing   missing   missing   missing
   2 │        1  c                   1         1   missing   missing   missing   missing
   3 │        1  d                   1         2   missing   missing   missing   missing
   4 │        2  b             missing   missing         1         2   missing   missing
   5 │        2  c             missing   missing         1         1   missing   missing
   6 │        2  d             missing   missing         3         4   missing   missing
   7 │        3  b             missing   missing   missing   missing         2         2
   8 │        3  c             missing   missing   missing   missing         1         1
   9 │        3  d             missing   missing   missing   missing         5         6

julia> transpose(groupby(ds, :group), 2:4, id = :e, default = 99999)
9×8 Dataset
 Row │ group     _variables_  a         b         c         d         e         f        
     │ identity  identity     identity  identity  identity  identity  identity  identity
     │ Int64?    String?      Int64?    Int64?    Int64?    Int64?    Int64?    Int64?   
─────┼───────────────────────────────────────────────────────────────────────────────────
   1 │        1  b                   1         1     99999     99999     99999     99999
   2 │        1  c                   1         1     99999     99999     99999     99999
   3 │        1  d                   1         2     99999     99999     99999     99999
   4 │        2  b               99999     99999         1         2     99999     99999
   5 │        2  c               99999     99999         1         1     99999     99999
   6 │        2  d               99999     99999         3         4     99999     99999
   7 │        3  b               99999     99999     99999     99999         2         2
   8 │        3  c               99999     99999     99999     99999         1         1
   9 │        3  d               99999     99999     99999     99999         5         6

julia> pop = Dataset(country = ["c1","c1","c2","c2","c3","c3"],
                               sex = [1, 2, 1, 2, 1, 2],
                               pop_2000 = [100, 120, 150, 155, 170, 190],
                               pop_2010 = [110, 120, 155, 160, 178, 200],
                               pop_2020 = [115, 130, 161, 165, 180, 203])
6×5 Dataset
 Row │ country   sex       pop_2000  pop_2010  pop_2020
     │ identity  identity  identity  identity  identity
     │ String?   Int64?    Int64?    Int64?    Int64?   
─────┼──────────────────────────────────────────────────
   1 │ c1               1       100       110       115
   2 │ c1               2       120       120       130
   3 │ c2               1       150       155       161
   4 │ c2               2       155       160       165
   5 │ c3               1       170       178       180
   6 │ c3               2       190       200       203

julia> gender(x) = x == 1 ? "Male" : "Female"
gender (generic function with 1 method)

julia> setformat!(pop, 2 => gender)
6×5 Dataset
 Row │ country   sex     pop_2000  pop_2010  pop_2020
     │ identity  gender  identity  identity  identity
     │ String?   Int64?  Int64?    Int64?    Int64?   
─────┼────────────────────────────────────────────────
   1 │ c1          Male       100       110       115
   2 │ c1        Female       120       120       130
   3 │ c2          Male       150       155       161
   4 │ c2        Female       155       160       165
   5 │ c3          Male       170       178       180
   6 │ c3        Female       190       200       203

julia> transpose(gatherby(pop, 1, isgathered = true), r"pop", id = :sex)
9×4 Dataset
 Row │ country   _variables_  Male      Female   
     │ identity  identity     identity  identity
     │ String?   String?      Int64?    Int64?   
─────┼───────────────────────────────────────────
   1 │ c1        pop_2000          100       120
   2 │ c1        pop_2010          110       120
   3 │ c1        pop_2020          115       130
   4 │ c2        pop_2000          150       155
   5 │ c2        pop_2010          155       160
   6 │ c2        pop_2020          161       165
   7 │ c3        pop_2000          170       190
   8 │ c3        pop_2010          178       200
   9 │ c3        pop_2020          180       203
julia> ds = Dataset(region = repeat(["North","North","South","South"],2),
                    fuel_type = repeat(["gas","coal"],4),
                    load = rand(8),
                    time = [1,1,1,1,2,2,2,2],
                    )
8×4 Dataset
 Row │ region    fuel_type  load       time     
     │ identity  identity   identity   identity
     │ String?   String?    Float64?   Int64?   
─────┼──────────────────────────────────────────
   1 │ North     gas        0.914918          1
   2 │ North     coal       0.158792          1
   3 │ South     gas        0.415604          1
   4 │ South     coal       0.0702206         1
   5 │ North     gas        0.419423          2
   6 │ North     coal       0.765637          2
   7 │ South     gas        0.222119          2
   8 │ South     coal       0.723559          2

julia> transpose(groupby(ds, :time), :load, id = 1:2)
2×6 Dataset
 Row │ time      _variables_  ("North", "gas")  ("North", "coal")  ("South", "gas")  ("South", "coal")
     │ identity  identity     identity          identity           identity          identity          
     │ Int64?    String?      Float64?          Float64?           Float64?          Float64?          
─────┼─────────────────────────────────────────────────────────────────────────────────────────────────
   1 │        1  load                 0.778866          0.0256356          0.729273           0.786919
   2 │        2  load                 0.676968          0.366241           0.577498           0.294181

julia> ds = Dataset([[1, 2, 3], [1.1, 2.0, 3.3],[1.1, 2.1, 3.0],[1.1, 2.0, 3.2]]
                           ,[:person, Symbol("11/2020"), Symbol("12/2020"), Symbol("1/2021")])
3×4 Dataset
 Row │ person    11/2020   12/2020   1/2021   
     │ identity  identity  identity  identity
     │ Float64?  Float64?  Float64?  Float64?
─────┼────────────────────────────────────────
   1 │      1.0       1.1       1.1       1.1
   2 │      2.0       2.0       2.1       2.0
   3 │      3.0       3.3       3.0       3.2

julia> transpose(gatherby(ds, :person), Not(:person),
                variable_name = "Date",
                renamerowid = x -> Date(x, dateformat"m/y"),
                renamecolid = x -> "measurement")
9×3 Dataset
 Row │ person    Date        measurement
     │ identity  identity    identity    
     │ Float64?  Date?       Float64?    
─────┼───────────────────────────────────
   1 │      1.0  2020-11-01          1.1
   2 │      1.0  2020-12-01          1.1
   3 │      1.0  2021-01-01          1.1
   4 │      2.0  2020-11-01          2.0
   5 │      2.0  2020-12-01          2.1
   6 │      2.0  2021-01-01          2.0
   7 │      3.0  2020-11-01          3.3
   8 │      3.0  2020-12-01          3.0
   9 │      3.0  2021-01-01          3.2

julia> ds = Dataset(A_2018=1:4, A_2019=5:8, B_2017=9:12,
                               B_2018=9:12, B_2019 = [missing,13,14,15],
                                ID = [1,2,3,4])
4×6 Dataset
 Row │ A_2018    A_2019    B_2017    B_2018    B_2019    ID       
     │ identity  identity  identity  identity  identity  identity
     │ Int64?    Int64?    Int64?    Int64?    Int64?    Int64?   
─────┼────────────────────────────────────────────────────────────
   1 │        1         5         9         9   missing         1
   2 │        2         6        10        10        13         2
   3 │        3         7        11        11        14         3
   4 │        4         8        12        12        15         4

julia> f(x) =  replace(x, r"[A_B]"=>"")
f (generic function with 1 method)

julia> # later we provide a simpler solution for this example

julia> dsA = transpose(groupby(ds, :ID), r"A", renamerowid = f, variable_name = "Year", renamecolid = x->"A");

julia> dsB = transpose(groupby(ds, :ID), r"B", renamerowid = f, variable_name = "Year", renamecolid = x->"B");

julia> outerjoin(dsA, dsB, on = [:ID, :Year])
12×4 Dataset
 Row │ ID        Year      A         B        
     │ identity  identity  identity  identity
     │ Int64?    String?   Int64?    Int64?   
─────┼────────────────────────────────────────
   1 │        1  2018             1         9
   2 │        1  2019             5   missing
   3 │        2  2018             2        10
   4 │        2  2019             6        13
   5 │        3  2018             3        11
   6 │        3  2019             7        14
   7 │        4  2018             4        12
   8 │        4  2019             8        15
   9 │        1  2017       missing         9
  10 │        2  2017       missing        10
  11 │        3  2017       missing        11
  12 │        4  2017       missing        12
```

## Advanced options

### `reanemcolid` with two arguments

The `renamecolid` function can also get access to the variable names from the input data set as the second argument. This can be used to generate even more customised column names for the output data set.

### Passing `Tuple` of column selectors

The column selector of the `transpose` function can be also a `Tuple` of column selectors. In this case, InMemoryDatasets does the transposition for each element of the tuple and then horizontally concatenates the output data sets to create a single data set. This provides extra flexibility to the user for reshaping a data set. By default, the `variable_name` is set to `nothing`, when `Tuple` of column selectors is passed as the argument, however, we can supply different names for each element of the `Tuple`.

Since the column names for the output data set can be the same for all elements of the tuple, `transpose` automatically modifies them to make them unique. Nevertheless, by passing `renamecolid`, we can customise the column names.

### Examples

```jldoctest
julia> ds = Dataset([[1, 1, 1, 2, 2, 2],
                        ["foo", "bar", "monty", "foo", "bar", "monty"],
                        ["a", "b", "c", "d", "e", "f"],
                        [1, 2, 3, 4, 5, 6]], [:g, :key, :foo, :bar])
6×4 Dataset
 Row │ g         key       foo       bar      
     │ identity  identity  identity  identity
     │ Int64?    String?   String?   Int64?   
─────┼────────────────────────────────────────
   1 │        1  foo       a                1
   2 │        1  bar       b                2
   3 │        1  monty     c                3
   4 │        2  foo       d                4
   5 │        2  bar       e                5
   6 │        2  monty     f                6

julia> transpose(groupby(ds, :g), (:foo, :bar), id = :key)
2×7 Dataset
 Row │ g         foo       bar       monty     foo_1     bar_1     monty_1  
     │ identity  identity  identity  identity  identity  identity  identity
     │ Int64?    String?   String?   String?   Int64?    Int64?    Int64?   
─────┼──────────────────────────────────────────────────────────────────────
   1 │        1  a         b         c                1         2         3
   2 │        2  d         e         f                4         5         6

julia> transpose(groupby(ds, :g), (:foo, :bar), id = :key,
                  renamecolid = (x,y) -> string(x,"_",y[1]))
2×7 Dataset
 Row │ g         foo_foo   bar_foo   monty_foo  foo_bar   bar_bar   monty_bar
     │ identity  identity  identity  identity   identity  identity  identity  
     │ Int64?    String?   String?   String?    Int64?    Int64?    Int64?    
─────┼────────────────────────────────────────────────────────────────────────
   1 │        1  a         b         c                 1         2          3
   2 │        2  d         e         f                 4         5          6

julia> ds = Dataset(paddockId= [0, 0, 1, 1, 2, 2],
                               color= ["red", "blue", "red", "blue", "red", "blue"],
                               count= [3, 4, 3, 4, 3, 4],
                               weight= [0.2, 0.3, 0.2, 0.3, 0.2, 0.2])
6×4 Dataset
 Row │ paddockId  color     count     weight   
     │ identity   identity  identity  identity
     │ Int64?     String?   Int64?    Float64?
─────┼─────────────────────────────────────────
   1 │         0  red              3       0.2
   2 │         0  blue             4       0.3
   3 │         1  red              3       0.2
   4 │         1  blue             4       0.3
   5 │         2  red              3       0.2
   6 │         2  blue             4       0.2

julia> transpose(groupby(ds, 1), (:count, :weight),
                         id = :color,
                         renamecolid = (x,y)->string(x,"/",y[1]),
                         )
3×5 Dataset
 Row │ paddockId  red/count  blue/count  red/weight  blue/weight
     │ identity   identity   identity    identity    identity    
     │ Int64?     Int64?     Int64?      Float64?    Float64?    
─────┼───────────────────────────────────────────────────────────
   1 │         0          3           4         0.2          0.3
   2 │         1          3           4         0.2          0.3
   3 │         2          3           4         0.2          0.2

julia> ds = Dataset(A_2018=1:4, A_2019=5:8, B_2017=9:12,
                               B_2018=9:12, B_2019 = [missing,13,14,15],
                                ID = [1,2,3,4])
4×6 Dataset
 Row │ A_2018    A_2019    B_2017    B_2018    B_2019    ID       
     │ identity  identity  identity  identity  identity  identity
     │ Int64?    Int64?    Int64?    Int64?    Int64?    Int64?   
─────┼────────────────────────────────────────────────────────────
   1 │        1         5         9         9   missing         1
   2 │        2         6        10        10        13         2
   3 │        3         7        11        11        14         3
   4 │        4         8        12        12        15         4

julia> f(x) =  replace(x, r"[A_B]"=>"")
f (generic function with 1 method)

julia> transpose(gatherby(ds, :ID), ([4,5,3], [1,2]),
                  variable_name = [:year, nothing],
                  renamerowid = f,
                  renamecolid = (x,y)->y[1][1:1])
12×4 Dataset
 Row │ ID        year      B         A        
     │ identity  identity  identity  identity
     │ Int64?    String?   Int64?    Int64?   
─────┼────────────────────────────────────────
   1 │        1  2018             9         1
   2 │        1  2019       missing         5
   3 │        1  2017             9   missing
   4 │        2  2018            10         2
   5 │        2  2019            13         6
   6 │        2  2017            10   missing
   7 │        3  2018            11         3
   8 │        3  2019            14         7
   9 │        3  2017            11   missing
  10 │        4  2018            12         4
  11 │        4  2019            15         8
  12 │        4  2017            12   missing

julia> ds = Dataset(rand(1:10, 2, 6), :auto)
2×6 Dataset
 Row │ x1        x2        x3        x4        x5        x6       
     │ identity  identity  identity  identity  identity  identity
     │ Int64?    Int64?    Int64?    Int64?    Int64?    Int64?   
─────┼────────────────────────────────────────────────────────────
   1 │       10         6         8        10        10         3
   2 │        9         7         9         4         2        10

julia> transpose(ds, ntuple(i->[i, i+3], 3), renamecolid = (x,y)->string(y[x]))
2×6 Dataset
 Row │ x1        x4        x2        x5        x3        x6       
     │ identity  identity  identity  identity  identity  identity
     │ Int64?    Int64?    Int64?    Int64?    Int64?    Int64?   
─────┼────────────────────────────────────────────────────────────
   1 │       10         9         6         7         8         9
   2 │       10         4        10         2         3        10
```

## Spreadsheet-style pivot table

To create a spreadsheet-style pivot tables in InMemoryDatasets, one can use the combination of the `combine` and `transpose` functions. To demonstrate this, we re-produce the documentation's examples of the `pandas`'s `pivot_table` function (ver: 1.3.4).

```jldoctest
julia> ds = Dataset(A = ["foo", "foo", "foo", "foo", "foo",
                       "bar", "bar", "bar", "bar"],
                       B = ["one", "one", "one", "two", "two",
                       "one", "one", "two", "two"],
                       C = ["small", "large", "large", "small",
                       "small", "large", "small", "small",
                       "large"],
                       D = [1, 2, 2, 3, 3, 4, 5, 6, 7],
                       E = [2, 4, 5, 5, 6, 6, 8, 9, 9])
9×5 Dataset
 Row │ A         B         C         D         E        
     │ identity  identity  identity  identity  identity
     │ String?   String?   String?   Int64?    Int64?   
─────┼──────────────────────────────────────────────────
   1 │ foo       one       small            1         2
   2 │ foo       one       large            2         4
   3 │ foo       one       large            2         5
   4 │ foo       two       small            3         5
   5 │ foo       two       small            3         6
   6 │ bar       one       large            4         6
   7 │ bar       one       small            5         8
   8 │ bar       two       small            6         9
   9 │ bar       two       large            7         9

julia> # This first example aggregates values by taking the sum.
julia> _tmp = combine(groupby(ds, 1:3), 4=>sum);

julia> transpose(gatherby(_tmp, 1:2, isgathered = true), :sum_D, id = :C, variable_name = nothing)
4×4 Dataset
 Row │ A         B         large     small    
     │ identity  identity  identity  identity
     │ String?   String?   Int64?    Int64?   
─────┼────────────────────────────────────────
   1 │ bar       one              4         5
   2 │ bar       two              7         6
   3 │ foo       one              4         1
   4 │ foo       two        missing         6

julia> transpose(gatherby(_tmp, 1:2, isgathered = true), :sum_D, id = :C, variable_name = nothing, default = 0)
4×4 Dataset
 Row │ A         B         large     small    
     │ identity  identity  identity  identity
     │ String?   String?   Int64?    Int64?   
─────┼────────────────────────────────────────
   1 │ bar       one              4         5
   2 │ bar       two              7         6
   3 │ foo       one              4         1
   4 │ foo       two              0         6

julia> # The next example aggregates by taking the mean across multiple columns. Here we don't need transposing
julia> combine(groupby(ds, [:A, :C]), [:D, :E] => mean)
4×4 Dataset
 Row │ A         C         mean_D    mean_E   
     │ identity  identity  identity  identity
     │ String?   String?   Float64?  Float64?
─────┼────────────────────────────────────────
   1 │ bar       large      5.5       7.5
   2 │ bar       small      5.5       8.5
   3 │ foo       large      2.0       4.5
   4 │ foo       small      2.33333   4.33333

julia> combine(groupby(ds, [:A, :C]), :D => mean, :E => [minimum, maximum, mean])
4×6 Dataset
 Row │ A         C         mean_D    minimum_E  maximum_E  mean_E   
     │ identity  identity  identity  identity   identity   identity
     │ String?   String?   Float64?  Int64?     Int64?     Float64?
─────┼──────────────────────────────────────────────────────────────
   1 │ bar       large      5.5              6          9   7.5
   2 │ bar       small      5.5              8          9   8.5
   3 │ foo       large      2.0              4          5   4.5
   4 │ foo       small      2.33333          2          6   4.33333
```
