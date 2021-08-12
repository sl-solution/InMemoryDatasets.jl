# First steps with Datasets

## Setting up the environment

To install in memory Datasets package , simply, use the following commands inside a Julia session:

```julia
julia> using Pkg
julia> Pkg.add("InMemoryDatasets")
```

Throughout the rest of the tutorial we will assume that you have installed the "In Memory Datasets" package and
have already typed `using InMemoryDatasets` which loads the package:

```jldoctest
julia> using InMemoryDatasets
```

## Creating a data set

To create a data set, use `Dataset()`. For example

```jldoctest
julia> ds = Dataset(var1 = [1, 2, 3],
                var2 = [1.2, 0.5, 3.3],
                var3 = ["C1", "C2", "C3"])
3×3 Dataset
 Row │ var1      var2      var3
     │ identity  identity  identity
     │ Int64?    Float64?  String?
─────┼────────────────────────────────
   1 │        1       1.2  C1
   2 │        2       0.5  C2
   3 │        3       3.3  C3
```

The first line of the output provides the general information about the data set.
 A data set is shown as a table in Julia, where each column represents a variable
 in the data set. The header section of the table shows three pieces of information
 for each column (variable), the column's name, the column's `format`, and
 the column's data type. The `format` of a column controls how the values
 of a column should be shown or interpreted when working with a data set.

The following example shows how to create a data set by providing a range of values.

```jldoctest
julia> Dataset(A=1:3, B=5:7, fixed=1)
3×3 Dataset
 Row │ A         B         fixed
     │ identity  identity  identity
     │ Int64?    Int64?    Int64?
─────┼──────────────────────────────
   1 │        1         5         1
   2 │        2         6         1
   3 │        3         7         1
```

Observe that using scalars for a column, like `1` for the column `:fixed` get automatically broadcasted to fill all rows of the created `Dataset`.

The missing values in Julia are declare as `missing`, and these values can also be an observation for a particular column, e.g.

```jldoctest
julia> Dataset(a = [1.1, -10.0, missing], b = 1:3)
3×2 Dataset
 Row │ a          b
     │ identity   identity
     │ Float64?   Int64?
─────┼─────────────────────
   1 │       1.1         1
   2 │     -10.0         2
   3 │ missing           3
```

Sometimes one needs to create a data set whose column names are not valid Julia identifiers.
In such a case the following form where column names are passed as strings, and `=` is replaced by `=>` is handy:

```jldoctest
julia> Dataset("customer age" => [15, 20, 25],
                 "first name" => ["Ben", "Steve", "Jule"])
3×2 Dataset
Row │ customer age  first name
    │ identity      identity
    │ Int64?        String?
────┼───────────────────────────
  1 │           15  Ben
  2 │           20  Steve
  3 │           25  Jule
```

It is also possible to construct a data set from the values of a matrix or a vector of vectors, e.g.

```jldoctest
julia> Dataset([1 0; 2 0], :auto)
2×2 Dataset
 Row │ x1        x2
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │        1         0
   2 │        2         0

julia> Dataset([[1 ,2], [0, 0]], :auto)
2×2 Dataset
 Row │ x1        x2
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │        1         0
   2 │        2         0
```

Note that the column names are generated automatically when `:auto` is set as
the second argument.

Alternatively you can pass a vector of column names as a second argument to the
`Dataset`:

```jldoctest
julia> Dataset([1 0; 2 0], ["col1", "col2"])
2×2 Dataset
 Row │ col1      col2
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │        1         0
   2 │        2         0
```

## Basic utility functions

### Getting meta information about a data set

To get information about a data set, use the `content` function. It provides meta information about a data set.

```jldoctest
julia> ds = Dataset(g = [1, 1, 1, 2, 2],
                   x1_int = [0, 0, 1, missing, 2],
                   x2_int = [3, 2, 1, 3, -2],
                   x1_float = [1.2, missing, -1.0, 2.3, 10],
                   x2_float = [missing, missing, 3.0, missing, missing],
                   x3_float = [missing, missing, -1.4, 3.0, -100.0])
5×6 Dataset
 Row │ g         x1_int    x2_int    x1_float   x2_float   x3_float
     │ identity  identity  identity  identity   identity   identity
     │ Int64?    Int64?    Int64?    Float64?   Float64?   Float64?
─────┼───────────────────────────────────────────────────────────────
   1 │        1         0         3        1.2  missing    missing
   2 │        1         0         2  missing    missing    missing
   3 │        1         1         1       -1.0        3.0       -1.4
   4 │        2   missing         3        2.3  missing          3.0
   5 │        2         2        -2       10.0  missing       -100.0

julia> content(ds)
5×6 Dataset
   Created: 2021-08-04T13:11:53.743
  Modified: 2021-08-04T13:11:53.743
      Info:
-----------------------------------
Variables information
┌──────────┬──────────┬─────────┐
│ var      │ format   │ eltype  │
├──────────┼──────────┼─────────┤
│ g        │ identity │ Int64   │
│ x1_int   │ identity │ Int64   │
│ x2_int   │ identity │ Int64   │
│ x1_float │ identity │ Float64 │
│ x2_float │ identity │ Float64 │
│ x3_float │ identity │ Float64 │
└──────────┴──────────┴─────────┘
```

`content` shows that the data set has 5 rows and 6 columns. It also shows when the data set has been created and when is the last time that it has been modified. The `content` function also reports the data type and formats of each variable.

The `Info` field is a string field which can contain any information related to the data set. To set an `Info` for a data set, use `setinfo!`, e.g.

```jldoctest
julia> setinfo!(ds, "An example from the manual")
"An example from the manual"
```

This information will be attached to the data set `ds`.

### Setting and removing formats

To set a specific format for a column of a data set use `setformat!` function, e.g.

```jldoctest
julia> ds = Dataset(x = 1:10,
                    y = repeat(1:5, inner = 2),
                    z = repeat(1:2, 5))
10×3 Dataset
 Row │ x         y         z
     │ identity  identity  identity
     │ Int64?    Int64?    Int64?
─────┼──────────────────────────────
   1 │        1         1         1
   2 │        2         1         2
   3 │        3         2         1
   4 │        4         2         2
   5 │        5         3         1
   6 │        6         3         2
   7 │        7         4         1
   8 │        8         4         2
   9 │        9         5         1
  10 │       10         5         2

julia> setformat!(ds, :y => sqrt)
10×3 Dataset
 Row │ x         y        z
     │ identity  sqrt     identity
     │ Int64?    Int64?   Int64?
─────┼─────────────────────────────
   1 │        1  1.0             1
   2 │        2  1.0             2
   3 │        3  1.41421         1
   4 │        4  1.41421         2
   5 │        5  1.73205         1
   6 │        6  1.73205         2
   7 │        7  2.0             1
   8 │        8  2.0             2
   9 │        9  2.23607         1
  10 │       10  2.23607         2
```

The first argument for `setformat!` is the data set which needs to be modified and the second argument is the name of column, `=>`, and a named function. In the above example, we assign `sqrt` function as a format for the column `:y`.

> Note that `setformat!` doesn't check the validity of a format, so if an invalid format is assigned to a column, for instance assigning `sqrt` to a column which contains negative values, some functionality of data set will be parallelised (like `show`ing the data set). In this cases, simply remove the invalid format by using `removeformat!`.

Let's define a function as a new format for column `:z` in the above example,

```jldoctest
julia> function gender(x)
          x == 1 ? "Male" : x == 2 ? "Female" : missing
       end
```

The format `gender` accepts one value and if the value is equal to `1`, `gender` maps it to "Male", if the value is equal to `2`, it maps it to "Female", and for any other values it maps them to `missing`.

```jldoctest
julia> setformat!(ds, :z => gender)
10×3 Dataset
 Row │ x         y        z
     │ identity  sqrt     gender
     │ Int64?    Int64?   Int64?
─────┼───────────────────────────
   1 │        1  1.0        Male
   2 │        2  1.0      Female
   3 │        3  1.41421    Male
   4 │        4  1.41421  Female
   5 │        5  1.73205    Male
   6 │        6  1.73205  Female
   7 │        7  2.0        Male
   8 │        8  2.0      Female
   9 │        9  2.23607    Male
  10 │       10  2.23607  Female
```

the `removeformat!` function should be used to remove a column's format.

```jldoctest
julia> removeformat!(ds, :y)
10×3 Dataset
 Row │ x         y         z
     │ identity  identity  gender
     │ Int64?    Int64?    Int64?
─────┼────────────────────────────
   1 │        1         1    Male
   2 │        2         1  Female
   3 │        3         2    Male
   4 │        4         2  Female
   5 │        5         3    Male
   6 │        6         3  Female
   7 │        7         4    Male
   8 │        8         4  Female
   9 │        9         5    Male
  10 │       10         5  Female
```

Similar to `setformat!` the first argument is the name of the data set and the second argument is the name of the column which we want to remove its format. Note that assigning or removing a format doesn't change the actual values of the column.

By default, formatted values of a column will be used when operations like displaying, sorting, grouping, or joining are called.

### Accessing individual column or observation

`ds[:, col]`, `ds[i, col]` can be used to access a specific column or specific observation of a specific column of `ds`, respectively. For example,

```jldoctest
julia> ds = Dataset(x = [4,6,3], y = [1,2,43]);
julia> ds[:, :x]
3-element Vector{Union{Missing, Int64}}:
 4
 6
 3

julia> ds[3, :y]
43
```

Note that `ds[:, col]` extracts (copy) a column of a data set as a vector. Thus, this vector can be used as a normal vector in Julia.

Also note that, assigning a new value to `ds[3, :y]` will modify the data set, i.e.

```jldoctest
julia> ds[3, :y] = 3
3

julia> ds
3×2 Dataset
 Row │ x         y
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │        4         1
   2 │        6         2
   3 │        3         3

julia> content(ds)
3×2 Dataset
   Created: 2021-08-04T13:18:51.185
  Modified: 2021-08-04T13:24:33.086
      Info:
-----------------------------------
Variables information
┌─────┬──────────┬────────┐
│ var │ format   │ eltype │
├─────┼──────────┼────────┤
│ x   │ identity │ Int64  │
│ y   │ identity │ Int64  │
└─────┴──────────┴────────┘
```

The `content` function shows that the data set has been created on `2021-08-04T13:18:51.185`, and the last time that it has been modified is on `2021-08-04T13:24:33.086`.

### Adding a new column

To add a new column (variable) to a data set use `ds.newvar` or `ds[:, :newvar]` syntax,

```jldoctest
julia> ds = Dataset(var1 = [1, 2, 3])
3×1 Dataset
 Row │ var1
     │ identity
     │ Int64?
─────┼──────────
   1 │        1
   2 │        2
   3 │        3

julia> ds.var2 = ["val1", "val2", "val3"]
3-element Vector{String}:
 "val1"
 "val2"
 "val3"

julia> ds
3×2 Dataset
 Row │ var1      var2
     │ identity  identity
     │ Int64?    String?
─────┼──────────────────────
   1 │        1  val1
   2 │        2  val2
   3 │        3  val3

julia> ds[:, :var3] = [3.5, 4.6, 32.0]
3-element Vector{Float64}:
  3.5
  4.6
 32.0

julia> ds
3×3 Dataset
 Row │ var1      var2        var3
     │ identity  identity    identity
     │ Int64?    String?     Float64?
─────┼────────────────────────────────
   1 │        1  val1             3.5
   2 │        2  val2             4.6
   3 │        3  val3            32.0
```

Be aware that, when adding a new column to a data set, using the above syntax, if the column already exists in the data set it will be replaced by new one.

### Some useful functions

The following functions are very handy when working with a data set. Note that functions which end with `!` modify the original data set.

* `names(ds)` gives the column names as a vector of string.
* `size(ds)` prints the data set dimension, i.e. number of rows and number of columns
* `nrow(ds)` returns the number of rows
* `ncol(ds)` returns the number of columns
* `first(ds, n)` shows the first `n` rows of a data set
* `last(ds, n)` shows the last `n` rows of a data set
* `rename!` can be used to rename column names
* `select!` can be used to drop, select, or rearrange columns
* `delete!` deletes rows from a data set
* `append!(ds, tds)` appends `tds` at the end of `ds`

```jldoctest
julia> test_data = Dataset(rand(1:10, 4, 3), :auto)
4×3 Dataset
 Row │ x1        x2        x3
     │ identity  identity  identity
     │ Int64?    Int64?    Int64?
─────┼──────────────────────────────
   1 │        5        10         5
   2 │        3         8        10
   3 │        1         7         7
   4 │        2         6         5

julia> names(test_data)
3-element Vector{String}:
 "x1"
 "x2"
 "x3"

julia> size(test_data)
(4, 3)

julia> nrow(test_data)
4

julia> ncol(test_data)
3

julia> first(test_data, 3)
3×3 Dataset
 Row │ x1        x2        x3
     │ identity  identity  identity
     │ Int64?    Int64?    Int64?
─────┼──────────────────────────────
   1 │        5        10         5
   2 │        3         8        10
   3 │        1         7         7

julia> last(test_data, 2)
2×3 Dataset
 Row │ x1        x2        x3
     │ identity  identity  identity
     │ Int64?    Int64?    Int64?
─────┼──────────────────────────────
   1 │        1         7         7
   2 │        2         6         5

julia> rename!(test_data, :x1 => :var1)
4×3 Dataset
 Row │ var1      x2        x3
     │ identity  identity  identity
     │ Int64?    Int64?    Int64?
─────┼──────────────────────────────
   1 │        5        10         5
   2 │        3         8        10
   3 │        1         7         7
   4 │        2         6         5

julia> select!(test_data, :x2, :var1)
4×2 Dataset
 Row │ x2        var1
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │       10         5
   2 │        8         3
   3 │        7         1
   4 │        6         2

julia> test_data
4×2 Dataset
 Row │ x2        var1
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │       10         5
   2 │        8         3
   3 │        7         1
   4 │        6         2

julia> delete!(test_data, 2)
3×2 Dataset
 Row │ x2        var1
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │       10         5
   2 │        7         1
   3 │        6         2

julia> second_data = Dataset(var1 = [1, 3, 5, 6, 6],
                             x2 = [3, 4,5,6, 3])
5×2 Dataset
 Row │ var1      x2
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │        1         3
   2 │        3         4
   3 │        5         5
   4 │        6         6
   5 │        6         3

julia> append!(test_data, second_data)
8×2 Dataset
 Row │ x2        var1
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │       10         5
   2 │        7         1
   3 │        6         2
   4 │        3         1
   5 │        4         3
   6 │        5         5
   7 │        6         6
   8 │        3         6
```
