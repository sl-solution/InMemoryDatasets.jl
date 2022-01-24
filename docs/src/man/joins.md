# Joins and related topics

## Introduction

In this section we give details of the functions that can be used to combined data sets in InMemoryDatasets. These functions are multi-threaded (unless `threads = false` i s passed to the functions) and thus are very performant for general use. By default, InMemoryDatasets uses the formatted values for joining observations, however, this can be controlled by the `mapformats` keyword argument.

## Database like joins

The main functions for combining two data sets are `leftjoin`, `innerjoin`, `outerjoin`, `semijoin`, and `antijoin`. The basic syntax for all of these functions is `*join(dsl, dsr, on = [...])`, where `dsl` is the left data set, `dsr` is the right data set, and the `on` keyword argument specifies the key(s) which will be used for matching observations between the left and the right data set. In general, the `on` key can be a single column name (either as `Symbol` or `String`) or a vector of column names. When the column names are not identical in both tables, a vector of pair of column names can be used instead.

-   `leftjoin`: joins two data sets and its output contains rows for values of the key(s) that exist in the left data set,
    whether or not that value exists in the right data set.
-   `innerjoin`: joins two data sets and its output contains rows for values of the key(s) that exist in both left and right data sets.
-   `outerjoin`: joins two data sets and its output contains rows for values of the key(s) that exist in any of the left or the right data set.
-   `semijoin`: Like an inner join, but its output is restricted to columns from the left table.
-   `antijoin`: The output contains rows for values of the key(s) that exist in the left data set but not the right data set.
    As with `semijoin`, output is restricted to columns from the left data set.

See [the Wikipedia page on SQL joins](https://en.wikipedia.org/wiki/Join_(SQL)) for more information.

By default (except for `semijoin` and `antijoin`), to match observations, InMemoryDatasets sorts the right data set and uses a binary search algorithm for finding the matches of each observation in the left data set in the right data set based on the passed key column(s), thus, it has better performance when the left data set is larger than the right data set. However, passing `method = :hash` changes the default. The matching is done based on the formatted values of the key column(s), however, using the `mapformats` keyword argument can change this behaviour, one may set it to `false` for one or both data sets.

For `method = :sort` and `leftjoin` and `innerjoin` the order of observations of the output data set is the same as their order in the left data set. However, the order of observations from the right table depends on the stability of the sort algorithm. User can set the `stable` keyword argument to `true` to guarantee a stable sort. For `outerjoin` the order of observations from the left data set in the output data set is also the same as their order in the original data set, however, for those observations which are from the right table when `method = :sort` there is no specific order.

By default, when `method = :sort`, the join functions use a hybrid `Heap Sort` algorithm to sort the observations in the right data set, however, setting `alg = QuickSort` change the default algorithm to a hybrid Quick Sort one.

For very large data sets, if the sorting of the first key is expensive, setting the `accelerate` keyword argument to `true` may improve the overall performance. By setting `accelerate = true`, InMemoryDatasets first divides all observations in the right data set into multiple parts (up to 1024 parts) based on the first passed key, and then for each observations in the left data set finds the corresponding part in the right data set and searches for the matching observations only within that part.

The `leftjoin`, `semijoin`, and `antijoin` functions have in-place version which are `leftjoin!`, `semijoin!`, and `antijoin!`, respectively. Instead of creating a new output dataset, these in-place versions of the functions replace the passed left table. Note that, for the `leftjoin!` there must be no more than one match for each observation from the right table, otherwise, the function raises an error.

### Examples

```jldoctest
julia> name = Dataset(ID = [1, 2, 3], Name = ["John Doe", "Jane Doe", "Joe Blogs"])
3×2 Dataset
 Row │ ID        Name      
     │ identity  identity  
     │ Int64?    String?   
─────┼─────────────────────
   1 │        1  John Doe
   2 │        2  Jane Doe
   3 │        3  Joe Blogs

julia> job = Dataset(ID = [1, 2, 2, 4],
                     Job = ["Lawyer", "Doctor", "Florist", "Farmer"])
4×2 Dataset
 Row │ ID        Job      
     │ identity  identity
     │ Int64?    String?  
─────┼────────────────────
   1 │        1  Lawyer
   2 │        2  Doctor
   3 │        2  Florist
   4 │        4  Farmer

julia> leftjoin(name, job, on = :ID)
4×3 Dataset
 Row │ ID        Name       Job      
     │ identity  identity   identity
     │ Int64?    String?    String?  
─────┼───────────────────────────────
   1 │        1  John Doe   Lawyer
   2 │        2  Jane Doe   Doctor
   3 │        2  Jane Doe   Florist
   4 │        3  Joe Blogs  missing  

julia> dsl = Dataset(year = [Date("2020-3-1"), Date("2021-10-21"), Date("2020-1-4"), Date("2012-12-11")],
                     leap_year = [true, false, true, true])
4×2 Dataset
 Row │ year        leap_year
     │ identity    identity  
     │ Date?       Bool?     
─────┼───────────────────────
   1 │ 2020-03-01       true
   2 │ 2021-10-21      false
   3 │ 2020-01-04       true
   4 │ 2012-12-11       true

julia> dsr = Dataset(year = [2020, 2021], event = ['A', 'B'])
2×2 Dataset
 Row │ year      event    
     │ identity  identity
     │ Int64?    Char?    
─────┼────────────────────
   1 │     2020  A
   2 │     2021  B

julia> setformat!(dsl, 1 => year);

julia> leftjoin(dsl, dsr, on = :year)
4×3 Dataset
 Row │ year   leap_year  event    
     │ year   identity   identity
     │ Date?  Bool?      Char?    
─────┼────────────────────────────
   1 │ 2020        true  A
   2 │ 2021       false  B
   3 │ 2020        true  A
   4 │ 2012        true  missing  

julia> innerjoin(name, job, on = :ID)
3×3 Dataset
 Row │ ID        Name      Job      
     │ identity  identity  identity
     │ Int64?    String?   String?  
─────┼──────────────────────────────
   1 │        1  John Doe  Lawyer
   2 │        2  Jane Doe  Doctor
   3 │        2  Jane Doe  Florist

julia> outerjoin(name, job, on = :ID)
5×3 Dataset
 Row │ ID        Name       Job      
     │ identity  identity   identity
     │ Int64?    String?    String?  
─────┼───────────────────────────────
   1 │        1  John Doe   Lawyer
   2 │        2  Jane Doe   Doctor
   3 │        2  Jane Doe   Florist
   4 │        3  Joe Blogs  missing  
   5 │        4  missing    Farmer
```

To demonstrate the use of the `accelerate` keyword, we generate two data sets and use the `@btime` macro from the `BenchmarkTools` package to benchmark the performance of the `innerjoin` function with and without acceleration.

```jldoctest
julia> using BenchmarkTools

julia> using Random

julia> dsl = Dataset(x1 = [randstring('a':'z', 6) for _ in 1:10^6],
               x2 = rand(1:100, 10^6), x3 = rand(10^6));

julia> dsr = Dataset(y1 = [randstring('a':'z', 6) for _ in 1:10^6],
               y2 = rand(1:100, 10^6), y3 = rand(10^6));

julia> @btime innerjoin(dsl, dsr, on = [:x1=>:y1, :x2=>:y2]);
  382.759 ms (1254 allocations: 55.40 MiB)

julia> @btime innerjoin(dsl, dsr, on = [:x1=>:y1, :x2=>:y2], accelerate = true);
  155.306 ms (2160 allocations: 45.92 MiB)
```

As it can be observed, using `accelerate = true` significantly reduces the joining time. The reason for this reduction is because currently sorting `String` type columns in InMemoryDatasets is relatively expensive, and using `accelerate = true` helps to reduce this by splitting the observations into multiple parts.

And of course for this example we can simply use the hash techniques for matching observations:

```jldoctest
julia> @btime innerjoin(dsl, dsr, on = [:x1=>:y1, :x2=>:y2], method = :hash);
 86.323 ms (1095 allocations: 96.95 MiB)
```

## `contains`

The `contains` function is special function that can be used to enquiry observations of a data set which are contained in another data set. It returns a boolean vector where is true when the key for the corresponding row in the main data set is found in the transaction data set. The syntax of the function is the same as the `leftjoin` function. When a single column is used for matching observations, the function uses hashing techniques to find the matched observations, however, for multiple key columns, it uses the sorting algorithm to search for the matched observations.

Both `semijoin` and `antijoin` use the `contains` function behind the scene for filtering the left data set.

## Close match join

The `closejoin` function joins two data sets based on exact match on the key variable or the closest match (here, closest match depends on the `direction` keyword argument) when the exact match doesn't exist.

The `closejoin!` function does a close join in-place.

A tolerance for finding close matches can be passed via the `tol` keyword argument, and for the situations where the exact match is not allowed, user can pass `allow_exact_match = false`.

`closejoin/!` support `method = :hash` however, for the last key column it uses the sorting method to find the closest match.

### Examples

```jldoctest
julia> classA = Dataset(id = ["id1", "id2", "id3", "id4", "id5"],
                        mark = [50, 69.5, 45.5, 88.0, 98.5])
5×2 Dataset
 Row │ id          mark
     │ identity    identity
     │ String?     Float64?
─────┼──────────────────────
   1 │ id1             50.0
   2 │ id2             69.5
   3 │ id3             45.5
   4 │ id4             88.0
   5 │ id5             98.5
julia> grades = Dataset(mark = [0, 49.5, 59.5, 69.5, 79.5, 89.5, 95.5],
                        grade = ["F", "P", "C", "B", "A-", "A", "A+"])
7×2 Dataset
 Row │ mark      grade
     │ identity  identity
     │ Float64?  String?
─────┼──────────────────────
   1 │      0.0  F
   2 │     49.5  P
   3 │     59.5  C
   4 │     69.5  B
   5 │     79.5  A-
   6 │     89.5  A
   7 │     95.5  A+

julia> closejoin(classA, grades, on = :mark)
5×3 Dataset
 Row │ id          mark      grade
     │ identity    identity  identity
     │ String?     Float64?  String?
─────┼──────────────────────────────────
   1 │ id1             50.0  P
   2 │ id2             69.5  B
   3 │ id3             45.5  F
   4 │ id4             88.0  A-
   5 │ id5             98.5  A+
```

Examples of using `closejoin` for financial data.

```jldoctest
julia> trades = Dataset(
                [["20160525 13:30:00.023",
                  "20160525 13:30:00.038",
                  "20160525 13:30:00.048",
                  "20160525 13:30:00.048",
                  "20160525 13:30:00.048"],
                ["MSFT", "MSFT",
                 "GOOG", "GOOG", "AAPL"],
                [51.95, 51.95,
                 720.77, 720.92, 98.00],
                [75, 155,
                 100, 100, 100]],
               ["time", "ticker", "price", "quantity"]);

julia> modify!(trades, 1 => byrow(x -> DateTime(x, dateformat"yyyymmdd HH:MM:SS.s")))
5×4 Dataset
 Row │ time                     ticker      price     quantity
     │ identity                 identity    identity  identity
     │ DateTime?                String?      Float64?  Int64?
─────┼─────────────────────────────────────────────────────────
   1 │ 2016-05-25T13:30:00.023  MSFT           51.95        75
   2 │ 2016-05-25T13:30:00.038  MSFT           51.95       155
   3 │ 2016-05-25T13:30:00.048  GOOG          720.77       100
   4 │ 2016-05-25T13:30:00.048  GOOG          720.92       100
   5 │ 2016-05-25T13:30:00.048  AAPL           98.0        100

julia> quotes = Dataset(
              [["20160525 13:30:00.023",
                "20160525 13:30:00.023",
                "20160525 13:30:00.030",
                "20160525 13:30:00.041",
                "20160525 13:30:00.048",
                "20160525 13:30:00.049",
                "20160525 13:30:00.072",
                "20160525 13:30:00.075"],
              ["GOOG", "MSFT", "MSFT", "MSFT",
               "GOOG", "AAPL", "GOOG", "MSFT"],
              [720.50, 51.95, 51.97, 51.99,
               720.50, 97.99, 720.50, 52.01],
              [720.93, 51.96, 51.98, 52.00,
               720.93, 98.01, 720.88, 52.03]],
             ["time", "ticker", "bid", "ask"]);

julia> modify!(quotes, 1 => byrow(x -> DateTime(x, dateformat"yyyymmdd HH:MM:SS.s")))
8×4 Dataset
 Row │ time                     ticker      bid       ask
     │ identity                 identity    identity  identity
     │ DateTime?                String?     Float64?  Float64?
─────┼─────────────────────────────────────────────────────────
   1 │ 2016-05-25T13:30:00.023  GOOG          720.5     720.93
   2 │ 2016-05-25T13:30:00.023  MSFT           51.95     51.96
   3 │ 2016-05-25T13:30:00.030  MSFT           51.97     51.98
   4 │ 2016-05-25T13:30:00.041  MSFT           51.99     52.0
   5 │ 2016-05-25T13:30:00.048  GOOG          720.5     720.93
   6 │ 2016-05-25T13:30:00.049  AAPL           97.99     98.01
   7 │ 2016-05-25T13:30:00.072  GOOG          720.5     720.88
   8 │ 2016-05-25T13:30:00.075  MSFT           52.01     52.03

julia> closejoin(trades, quotes, on = :time, makeunique = true)
5×7 Dataset
 Row │ time                     ticker      price     quantity  ticker_1    bid       ask
     │ identity                 identity    identity  identity  identity    identity  identity
     │ DateTime?                String?     Float64?  Int64?    String?     Float64?  Float64?
─────┼─────────────────────────────────────────────────────────────────────────────────────────
   1 │ 2016-05-25T13:30:00.023  MSFT           51.95        75  MSFT           51.95     51.96
   2 │ 2016-05-25T13:30:00.038  MSFT           51.95       155  MSFT           51.97     51.98
   3 │ 2016-05-25T13:30:00.048  GOOG          720.77       100  GOOG          720.5     720.93
   4 │ 2016-05-25T13:30:00.048  GOOG          720.92       100  GOOG          720.5     720.93
   5 │ 2016-05-25T13:30:00.048  AAPL           98.0        100  GOOG          720.5     720.93
```

In the above example, the `closejoin` for each `ticker` can be done by passing `ticker` as the first variable for the `on` keyword, i.e. when more than one key is used for `on` the last one will be used for "close match" and the rest are used for exact match.

When `border` is set to `:missing` (default value) for the `:backward` direction the value below the smallest value will be set to `missing`, and for the `:forward` direction the value above the largest value will be set to `missing`. And when `border = :nearest` the closest non-missing value will be fetched.

Passing `border = :none`, sets missing for values in left data set which are out of the right data set's range.

```jldoctest
julia> closejoin(trades, quotes, on = [:ticker, :time], border = :missing)
5×6 Dataset
 Row │ time                     ticker       price     quantity  bid         ask
     │ identity                 identity     identity  identity  identity    identity
     │ DateTime?                String?      Float64?  Int64?    Float64?    Float64?
─────┼─────────────────────────────────────────────────────────────────────────────────
   1 │ 2016-05-25T13:30:00.023  MSFT           51.95        75       51.95       51.96
   2 │ 2016-05-25T13:30:00.038  MSFT           51.95       155       51.97       51.98
   3 │ 2016-05-25T13:30:00.048  GOOG          720.77       100      720.5       720.93
   4 │ 2016-05-25T13:30:00.048  GOOG          720.92       100      720.5       720.93
   5 │ 2016-05-25T13:30:00.048  AAPL           98.0        100  missing     missing

julia> closejoin(trades, quotes, on = [:ticker, :time], border = :nearest)
5×6 Dataset
 Row │ time                     ticker      price       quantity  bid       ask
     │ identity                 identity    identity    identity  identity  identity
     │ DateTime?                String?     Float64?    Int64?    Float64?  Float64?
─────┼─────────────────────────────────────────────────────────────────────────────
   1 │ 2016-05-25T13:30:00.023  MSFT           51.95        75     51.95     51.96
   2 │ 2016-05-25T13:30:00.038  MSFT           51.95       155     51.97     51.98
   3 │ 2016-05-25T13:30:00.048  GOOG          720.77       100    720.5     720.93
   4 │ 2016-05-25T13:30:00.048  GOOG          720.92       100    720.5     720.93
   5 │ 2016-05-25T13:30:00.048  AAPL           98.0        100     97.99     98.01

```


## Inequality-kind inner join

The `innerjoin` function can also use inequality comparisons to match observations from the left data set with the observations in the right data set. It can find all observations in the right data set that are `<=`(`<`) or `>=`(`>`) than a selected observation in the left data set. Additionally, if the user specifies two columns in the right table for a single key in the left table, it matches the observations in the left data set when they fall into the range specifies by the selected two key columns in the right data set. This conditional joining can be done within groups of observations if the user provide more than one key column for the left and the right data sets, i.e. the last key will be used for "inequality-kind" join and the rest will be used for the exact match.

For this kind of inner join, the key columns for both data sets which are defined for grouping observation must be passed as pair of column names (similar to normal use of `innerjoin`), however, the key column from the left data set which is going to be used for conditional joining must be also passed as a column name, and the key column(s) for conditional joining from the right data set must be passed as a Tuple of column names. For example, if the key column for the left data set is `:l_key`, and there are two columns in the right table called, `:r_start` and `:r_end` the following demonstrates how a user can perform different kinds of conditional joining:

* `:l_key => (:r_start, nothing)`, a match happens if the selected observation from the left data set is `>= :r_start`.
* `:l_key => (nothing, :r_end)`, a match happens if the selected observation from the left data set is `<= :r_end`.
* `:l_key => (:r_start, :r_end)`, a match happens if the selected observation from the left data set is `>= :r_start` and `<= :r_end`.

To change inequalities to strict inequality the `strict_inequality` keyword argument must be set to `true` for one or both sides, e.g. `strict_inequality = true`(both side), `strict_inequality = [false, true]`(only one side).

`innerjoin` supports `method = :hash` for all key columns which are not used for inequality like join.

### Examples

```jldoctest
julia> store = Dataset([[Date("2019-10-01"), Date("2019-10-02"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-03"), Date("2019-10-03")],
               ["A", "A", "B", "A", "B", "A"]], [:date, :store])
6×2 Dataset
 Row │ date        store    
     │ identity    identity
     │ Date?       String?  
─────┼──────────────────────
   1 │ 2019-10-01  A
   2 │ 2019-10-02  A
   3 │ 2019-10-05  B
   4 │ 2019-10-04  A
   5 │ 2019-10-03  B
   6 │ 2019-10-03  A

julia> roster = Dataset([["A", "A", "B", "A"],
                    [4, 1, 8, 2 ],
                    [Date("2019-10-04"), Date("2019-09-30"), Date("2019-10-04"), Date("2019-10-02")],
                    [Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-06"), Date("2019-10-04")]],
                    ["store", "employee_ID", "start_date", "end_date"])
4×4 Dataset
 Row │ store     employee_ID  start_date  end_date   
     │ identity  identity     identity    identity   
     │ String?   Int64?       Date?       Date?      
─────┼───────────────────────────────────────────────
   1 │ A                   4  2019-10-04  2019-10-06
   2 │ A                   1  2019-09-30  2019-10-04
   3 │ B                   8  2019-10-04  2019-10-06
   4 │ A                   2  2019-10-02  2019-10-04

julia> innerjoin(store, roster, on = [:store => :store, :date => (:start_date, nothing)])
9×4 Dataset
 Row │ date        store     employee_ID  end_date   
     │ identity    identity  identity     identity   
     │ Date?       String?   Int64?       Date?      
─────┼───────────────────────────────────────────────
   1 │ 2019-10-01  A                   1  2019-10-04
   2 │ 2019-10-02  A                   1  2019-10-04
   3 │ 2019-10-02  A                   2  2019-10-04
   4 │ 2019-10-05  B                   8  2019-10-06
   5 │ 2019-10-04  A                   1  2019-10-04
   6 │ 2019-10-04  A                   2  2019-10-04
   7 │ 2019-10-04  A                   4  2019-10-06
   8 │ 2019-10-03  A                   1  2019-10-04
   9 │ 2019-10-03  A                   2  2019-10-04

julia> innerjoin(store, roster, on = [:store => :store, :date => (nothing, :end_date)])
14×4 Dataset
 Row │ date        store     employee_ID  start_date
     │ identity    identity  identity     identity   
     │ Date?       String?   Int64?       Date?      
─────┼───────────────────────────────────────────────
   1 │ 2019-10-01  A                   1  2019-09-30
   2 │ 2019-10-01  A                   2  2019-10-02
   3 │ 2019-10-01  A                   4  2019-10-04
   4 │ 2019-10-02  A                   1  2019-09-30
   5 │ 2019-10-02  A                   2  2019-10-02
   6 │ 2019-10-02  A                   4  2019-10-04
   7 │ 2019-10-05  B                   8  2019-10-04
   8 │ 2019-10-04  A                   1  2019-09-30
   9 │ 2019-10-04  A                   2  2019-10-02
  10 │ 2019-10-04  A                   4  2019-10-04
  11 │ 2019-10-03  B                   8  2019-10-04
  12 │ 2019-10-03  A                   1  2019-09-30
  13 │ 2019-10-03  A                   2  2019-10-02
  14 │ 2019-10-03  A                   4  2019-10-04

julia> innerjoin(store, roster, on = [:store => :store, :date => (:start_date, :end_date)])
9×3 Dataset
 Row │ date        store     employee_ID
     │ identity    identity  identity    
     │ Date?       String?   Int64?      
─────┼───────────────────────────────────
   1 │ 2019-10-01  A                   1
   2 │ 2019-10-02  A                   1
   3 │ 2019-10-02  A                   2
   4 │ 2019-10-05  B                   8
   5 │ 2019-10-04  A                   1
   6 │ 2019-10-04  A                   2
   7 │ 2019-10-04  A                   4
   8 │ 2019-10-03  A                   1
   9 │ 2019-10-03  A                   2
```

## Update a data set by values from another data set

`update!` updates a data set values by using values from a transaction data set. The function uses the given keys (`on = ...`) to select rows for updating. By default, the missing values in transaction data set wouldn't replace the values in the main data set, however, using `allowmissing = true` changes this behaviour. If there are multiple rows in the main data set which match the key(s), using `mode = :all` causes all of them to be updated, `mode = :missing` causes only the ones which are missing in the main data set to be updated, and `mode = fun` updates the values which calling `fun` on them returns `true`. If there are multiple rows in the transaction data set which match the key, only the last one (given `stable = true` is passed) will be used to update the main data set.

The `update!` functions replace the main data set with the updated version, however, if a copy of the updated data set is required, the `update` function can be used instead.

Like other join functions, one may pass `method = :hash` for using hash techniques to match observations.

### Examples

```jldoctest
julia> main = Dataset(group = ["G1", "G1", "G1", "G1", "G2", "G2", "G2"],
                      id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
                      x1    = [1.2, 2.3,missing,  2.3, 1.3, 2.1  , 0.0 ],
                      x2    = [ 5  ,  4  ,  4  ,  2  , 1  ,missing, 2  ])
7×4 Dataset
 Row │ group         id        x1         x2
     │ identity     identity  identity   identity
     │ String?      Int64?   Float64?    Int64?
─────┼───────────────────────────────────────────
   1 │ G1                 1        1.2         5
   2 │ G1                 1        2.3         4
   3 │ G1                 2  missing           4
   4 │ G1                 2        2.3         2
   5 │ G2                 1        1.3         1
   6 │ G2                 1        2.1   missing
   7 │ G2                 2        0.0         2


julia> transaction = Dataset(group = ["G1", "G2"], id = [2, 1],
                        x1 = [2.5, missing], x2 = [missing, 3])
2×4 Dataset
 Row │ group       id        x1         x2
     │ identity    identity  identity   identity
     │ String?       Int64?    Float64?   Int64?
─────┼───────────────────────────────────────────
   1 │ G1                 2        2.5   missing
   2 │ G2                 1  missing           3


julia> update(main, transaction, on = [:group, :id],
               allowmissing = false, mode = :missing)
7×4 Dataset
 Row │ group        id        x1        x2
     │ identity     identity  identity  identity
     │ String?       Int64?    Float64?  Int64?
─────┼──────────────────────────────────────────
   1 │ G1                 1       1.2         5
   2 │ G1                 1       2.3         4
   3 │ G1                 2       2.5         4
   4 │ G1                 2       2.3         2
   5 │ G2                 1       1.3         1
   6 │ G2                 1       2.1         3
   7 │ G2                 2       0.0         2


julia> update(main, transaction, on = [:group, :id],
               allowmissing = false, mode = :all)
7×4 Dataset
 Row │ group       id        x1        x2
     │ identity    identity  identity  identity
     │ String?       Int64?    Float64?  Int64?
─────┼──────────────────────────────────────────
   1 │ G1                 1       1.2         5
   2 │ G1                 1       2.3         4
   3 │ G1                 2       2.5         4
   4 │ G1                 2       2.5         2
   5 │ G2                 1       1.3         3
   6 │ G2                 1       2.1         3
   7 │ G2                 2       0.0         2

julia> update(main, transaction, on = [:group, :id],
                              mode = isequal(2.3))
7×4 Dataset
 Row │ group     id        x1         x2       
     │ identity  identity  identity   identity
     │ String?   Int64?    Float64?   Int64?   
─────┼─────────────────────────────────────────
   1 │ G1               1        1.2         5
   2 │ G1               1        2.3         4
   3 │ G1               2  missing           4
   4 │ G1               2        2.5         2
   5 │ G2               1        1.3         1
   6 │ G2               1        2.1   missing
   7 │ G2               2        0.0         2
```
