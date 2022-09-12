# Formats

## Introduction

`format` is a named function that is assigned to a column (variable). The `format` of a column will be called on the individual values of the column before some operations (like `show`, `sort`,...) are done on the data set. Each column (variable) in a data set has a `format` property. The initial `format` of any column is `identity`, however, `setformat!` and `removeformat!` can be used to modify the `format` of columns in a data set. By default, the `format` of a column will be shown in the header when a data set is displayed.

The `format` of a column doesn't change the actual values of the column, thus, the actual  values of a column will be untouched during adding or removing `format`s.

The processing of `format` is lazy, i.e. Datasets doesn't process `format` unless an operation needs to access the formatted values. This also means that modifying the `format` of a column is instance. However, be aware that modifying a column's `format` changes the `modified` meta information (i.e. the last time that the data set has been modified) of the data set.

> Note that processing of formats are usually done in parallel, thus, it is not safe to use a function which is not parallel safe, unless `threads = false` is passed to the function which uses the formatted values.

In this section, we discuss the overall aspects of `format` and we postpone the practical use case of `format` to later sections when we introduce operations which access the formatted values.

### Examples

In this example, we create a simple data set and assign `iseven` function as the `format` for `:x1`, by using `setformat!(ds, 1 => iseven)`, note that we can also use the columns' names to assign `format`, i.e. the function can be called like `setformat!(ds, :x1 => iseven)`. After calling `setformat!`, the format of the column will be set, and from this point any operation which support `format` will use these formatted values. One of the operations which uses formatted values is `show`. For instance, in the following example, the printed data set shows the formatted values.

```jldoctest
julia> ds = Dataset(x1 = 1:5, x2 = [1,2,1,2,1])
5×2 Dataset
 Row │ x1        x2
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │        1         1
   2 │        2         2
   3 │        3         1
   4 │        4         2
   5 │        5         1
julia> setformat!(ds, 1 => iseven)
5×2 Dataset
 Row │ x1      x2
     │ iseven  identity
     │ Int64?  Int64?
─────┼──────────────────
   1 │  false         1
   2 │   true         2
   3 │  false         1
   4 │   true         2
   5 │  false         1

julia> ds[1,1] # note that the actual value is not changed
1
```

## Manipulating formats

There are two functions that are handy for manipulating the columns' `format`s, `setformat!` and `removeformat!`.

`setformat!` and `removeformat!` are for setting and removing columns' `format`, respectively. The syntax of these functions are

> `setformat!(ds, arg...)`

> `removeformat!(ds, cols...)`

For `setformat!` each `arg` in the argument must be of the `cols => fmt` form, where `fmt` is the named function and `cols` is either column(s) name(s), column(s) index(s), or regular expression, thus, expressions like `setformat!(ds, 1:10=>sqrt)`, `setformat!(ds, r"x"=>iseven, :y=>sqrt)` are valid in InMemoryDatasets. When `cols` refers to more than one column, `fmt` will be assigned to all of those columns.

For `removeformat!` each `cols` in the argument is any column selector like column(s) name(s), column(s) index(s), or regular expression.

Beside these two functions, there exists the `getformat` function to query `format` of a column. The syntax of `getformat` is

> `getformat(ds, col)`

where `col` is a single column identifier, i.e. column index or column's name.


### Examples

In the following example we assign user defined functions as the format for the first and the last column and use the `month` function (predefined in Julia Dates) as the format for the column `:date`. Note that, the actual values of columns haven't been modified, they are only shown with the formatted value. As you may observe, the formatted values can help us to scan easily the sale of each store in different month

```jldoctest
julia> sale = Dataset(store = ["store1", "store1", "store2",
 				"store2", "store3", "store3", "store3"],
				date = [Date("2020-05-01"), Date("2020-06-01"),
				Date("2020-05-01"), Date("2020-06-01"),
				Date("2020-05-01"), Date("2020-06-01"), Date("2020-07-01")],
				sale = [10000, 10100, 20020, 21000, 20300, 20400, 5000])
7×3 Dataset
 Row │ store       date        sale
     │ identity    identity    identity
     │ String?     Date?       Int64?
─────┼──────────────────────────────────
   1 │ store1      2020-05-01     10000
   2 │ store1      2020-06-01     10100
   3 │ store2      2020-05-01     20020
   4 │ store2      2020-06-01     21000
   5 │ store3      2020-05-01     20300
   6 │ store3      2020-06-01     20400
   7 │ store3      2020-07-01      5000

julia> storeid(x) = parse(Int, replace(x, "store"=>""))
storeid (generic function with 1 method)
julia> function SALE(x)
           if x < 10000
               "low"
           elseif x < 20000
               "average"
           elseif x < 21000
               "high"
           elseif x >= 21000
               "excellent"
           else
               missing
           end
       end
SALE (generic function with 1 method)

julia> setformat!(sale, 1 => storeid, :date => month, :sale => SALE)
7×3 Dataset
 Row │ store       date   sale
     │ storeid     month  SALE
     │ String?     Date?  Int64?
─────┼──────────────────────────────
   1 │ 1           5        average
   2 │ 1           6        average
   3 │ 2           5           high
   4 │ 2           6      excellent
   5 │ 3           5           high
   6 │ 3           6           high
   7 │ 3           7            low

julia> getformat(sale, "date")
month (generic function with 3 methods)
```

When the formatted values are not needed for some columns, a call to `removeformat!` can remove them,

```jldoctest
julia> removeformat!(sale, [1,2])
7×3 Dataset
 Row │ store       date        sale
     │ identity    identity    SALE
     │ String?     Date?       Int64?
─────┼───────────────────────────────────
   1 │ store1      2020-05-01    average
   2 │ store1      2020-06-01    average
   3 │ store2      2020-05-01       high
   4 │ store2      2020-06-01  excellent
   5 │ store3      2020-05-01       high
   6 │ store3      2020-06-01       high
   7 │ store3      2020-07-01        low
```
## Modifying a data set

The following rules administrate how a column format will automatically be changed if a data set is modified:

- As a general rule, the `format` of a column is preserved during different operations. For example, adding/removing a column to a data set don't change the `format` of the original/remaining columns.

- The `format` of a column wouldn't change if only few observations are updated, modified, added, or deleted, however, if a column goes through a significant change (e.g. all values change, or the column is replaced), its `format` will be automatically removed.

- The `format` of a column will be preserved during some operations where a new data set is created. For example, the `combine` function preserve the format of grouping variables. This feature will be discussed, in more details, when those operations are introduced in later sections.

## Using Dictionary

One way to recode values of a data set is by using `format` which picks the formatted values from a dictionary. Since it is not possible to feed `format` with any extra positional argument rather than the actual values of observations, the dictionary that defines recoded values must be placed with a default value or must be set as keyword argument with a default value which refers to the actual dictionary that has been defined for this purpose. This argument should be type annotated to avoid any unnecessary allocation.

### Example

```jldoctest
julia> ds = Dataset(rand(1:2, 10, 3), :auto)
10×3 Dataset
 Row │ x1        x2        x3       
     │ identity  identity  identity
     │ Int64?    Int64?    Int64?   
─────┼──────────────────────────────
   1 │        1         2         1
   2 │        1         2         2
   3 │        1         2         2
   4 │        2         1         1
   5 │        1         1         2
   6 │        1         2         2
   7 │        2         2         1
   8 │        2         1         2
   9 │        2         1         1
  10 │        1         1         1

julia> dict = Dict(1=>"yes", 2=>"no")
Dict{Int64, String} with 2 entries:
  2 => "no"
  1 => "yes"

julia> fmt1(x, dict::Dict{Int, String} = dict) = get(dict, x, missing)
fmt1 (generic function with 2 methods)

julia> setformat!(ds, 1:3 => fmt1)
10×3 Dataset
 Row │ x1      x2      x3     
     │ fmt1    fmt1    fmt1   
     │ Int64?  Int64?  Int64?
─────┼────────────────────────
   1 │    yes      no     yes
   2 │    yes      no      no
   3 │    yes      no      no
   4 │     no     yes     yes
   5 │    yes     yes      no
   6 │    yes      no      no
   7 │     no      no     yes
   8 │     no     yes      no
   9 │     no     yes     yes
  10 │    yes     yes     yes

```

## `format` validation

InMemoryDatasets doesn't validate the supplied `format` until it needs to use the formatted values for an operation, in that case, if the supplied `format` is not a valid `format`, InMemoryDatasets will throw errors. Also it is important to note that InMemoryDatasets is not aware of changing the definition of a `format` by users, thus, changing the definition of a function which is used as a `format` during a workflow may have some side effects. For example if a data set is `groupby!` with `mapformats = true` option, changing the definition of the formats invalidates the sorting order of the data set, but InMemoryDatasets is unaware of this, so, it is the user responsibility to remove the invalid formats in these situations.

In the following examples we demonstrate some scenarios which end up with an invalid `format`, and provide some remedies to fix the issues. Nevertheless, note that supplying an invalid `format` will not damage a data set and a simple call to `removeformat!` can be helpful to recover the original data set.

### Examples

First we create a data set and define a format.
```jldoctest
julia> ds = Dataset(x1 = [-1, 0, 1], x2 = [1.1, missing, 2.2], x3 = [1,2,3])
3×3 Dataset
 Row │ x1        x2         x3
     │ identity  identity   identity
     │ Int64?    Float64?   Int64?
─────┼───────────────────────────────
   1 │       -1        1.1         1
   2 │        0  missing           2
   3 │        1        2.2         3

julia> custom_format(x) = x[2]
custom_format (generic function with 1 method)
```
* **The function supplied as `format` is not defined for some values**: In this example, we use `sqrt` as `:x1`'s `format`, but `:x1` contains negative values and `sqrt` is not defined for negative integers. Running the following expression will throw bunch of errors, because after setting `format` InMemoryDatasets is trying to display the data set, but it cannot do that.

```jldoctest
julia> setformat!(ds, 1 => sqrt)
Error showing value of type Dataset:
ERROR: DomainError with -1.0:
sqrt will only return a complex result if called with a complex argument. Try sqrt(Complex(x)).
[...]
```

This issue can be solve manually by defining a user defined `format`:

```jldoctest
julia> sqrt_fmt(x) = isless(x, 0) ? "invalid" : sqrt(x)
sqrt_fmt (generic function with 1 method)

julia> setformat!(ds, 1 => sqrt_fmt)
3×3 Dataset
 Row │ x1         x2         x3
     │ sqrt_fmt   identity   identity
     │ Int64?     Float64?   Int64?
─────┼────────────────────────────────
   1 │ invalid          1.1         1
   2 │       0.0  missing           2
   3 │       1.0        2.2         3
```

* **Ignoring `missing` values**: In this example, we use `ROUND(x) = round(Int, x)` as `:x2` `format`, however, `round(Int, x)` doesn't know how to deal with `missing` values, thus, the same as the above example, InMemoryDatasets will throw errors.

```jldoctest
julia> ROUND(x) = round(Int, x)
ROUND (generic function with 1 method)

julia> setformat!(ds, 2 => ROUND)
Error showing value of type Dataset:
ERROR: MissingException: cannot convert a missing value to type Int64: use Union{Int64, Missing} instead
[...]
```

To solve this issue, we can redefine `ROUND` as

> `ROUND(x) = ismissing(x) ? missing : round(Int, x)`

or

> `ROUND(x) = round(Union{Int, Missing}, x)`

and every thing should be fine. Note that after updating the definition of `ROUND`, Datasets automatically fixes the formatted values of `:x2`

```jldoctest
julia> ROUND(x) = ismissing(x) ? missing : round(Int, x)
ROUND (generic function with 1 method)

julia> ds
3×3 Dataset
 Row │ x1         x2        x3
     │ sqrt_fmt   ROUND     identity
     │ Int64?     Float64?  Int64?
─────┼───────────────────────────────
   1 │ invalid           1         1
   2 │       0.0   missing         2
   3 │       1.0         2         3
```

* **The function defined as format assumes the input argument is a vector**: In this example `custom_format` (defined earlier) is used for the third column. `custom_format` is defined in such a way that it assumes the input argument is a vector, but Datasets applies `format` to each value.

```jldoctest
julia> setformat!(ds, 3=>custom_format)
Error showing value of type Dataset:
ERROR: BoundsError
[...]
```

To fix the issue we should redefine `custom_format` or simply remove the column's `format`:

```jldoctest
julia> removeformat!(ds, 3)
3×3 Dataset
 Row │ x1         x2        x3
     │ sqrt_fmt   ROUND     identity
     │ Int64?     Float64?  Int64?
─────┼───────────────────────────────
   1 │ invalid           1         1
   2 │       0.0   missing         2
   3 │       1.0         2         3
```
