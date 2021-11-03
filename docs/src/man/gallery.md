# Gallery

This gallery contains some random questions about data manipulation that we found on internet. The original questions are posted in different forums and are related to different packages. Whenever, we can remember the original source of a question we provide a link to it, otherwise, we just re-asked the question as we remember it. There is no particular theme about the questions, we just found them interesting since, a) they are not trivial, b) they can be done relatively easy in InMemoryDatasets, c) our solution is more efficient than what we found in the original source.

## `map!` and `map`

* How to randomly change about 10% of data values to missing?

```julia
julia> ds = Dataset(rand(10, 3), :auto)
10×3 Dataset
 Row │ x1         x2         x3        
     │ identity   identity   identity  
     │ Float64?   Float64?   Float64?  
─────┼─────────────────────────────────
   1 │ 0.829492   0.266336   0.712512
   2 │ 0.206569   0.252967   0.133839
   3 │ 0.0773648  0.420897   0.363549
   4 │ 0.404912   0.495679   0.400661
   5 │ 0.412908   0.740628   0.785319
   6 │ 0.624809   0.971097   0.725747
   7 │ 0.19843    0.378382   0.0453478
   8 │ 0.851221   0.563592   0.450065
   9 │ 0.351243   0.0555308  0.142801
  10 │ 0.208414   0.739952   0.926834

julia> map!(ds, x->rand()<.1 ? missing : x, :)
10×3 Dataset
 Row │ x1               x2               x3        
     │ identity         identity         identity  
     │ Float64?         Float64?         Float64?  
─────┼─────────────────────────────────────────────
   1 │       0.829492         0.266336   0.712512
   2 │       0.206569         0.252967   0.133839
   3 │       0.0773648        0.420897   0.363549
   4 │       0.404912         0.495679   0.400661
   5 │ missing                0.740628   0.785319
   6 │       0.624809   missing          0.725747
   7 │       0.19843          0.378382   0.0453478
   8 │ missing                0.563592   0.450065
   9 │       0.351243         0.0555308  0.142801
  10 │       0.208414         0.739952   0.926834
```

## Row operations, `byrow`

* In each row, how to replace missing values in a column by the first non-missing in previous columns. (Assuming for the first column the direction of search is reversed and all columns has the same type)

```julia
julia> ds = Dataset(rand([1,2,3, missing], 10, 6), :auto)
10×6 Dataset
 Row │ x1        x2        x3        x4        x5        x6       
     │ identity  identity  identity  identity  identity  identity
     │ Int64?    Int64?    Int64?    Int64?    Int64?    Int64?   
─────┼────────────────────────────────────────────────────────────
   1 │        1         1   missing         2   missing   missing
   2 │        2         1         3         3         2         3
   3 │        2         1   missing         3         3   missing
   4 │        1   missing   missing         1         1         1
   5 │        2         2   missing         1         3         1
   6 │  missing   missing         2         1   missing         1
   7 │  missing         2         3   missing         1         2
   8 │        3         3   missing         1         1         1
   9 │        3         1         1         3         1   missing
  10 │  missing         1         3         1         1         3

julia> f_n_m = byrow(ds, coalesce, :); # we will use this if the first column is missing

julia> op(x, y) = y .= ifelse.(ismissing.(y), x, y)
op (generic function with 1 method)

julia> byrow(ds, mapreduce, :, op = op, init = f_n_m);

julia> ds
10×6 Dataset
 Row │ x1        x2        x3        x4        x5        x6       
     │ identity  identity  identity  identity  identity  identity
     │ Int64?    Int64?    Int64?    Int64?    Int64?    Int64?   
─────┼────────────────────────────────────────────────────────────
   1 │        1         1         1         2         2         2
   2 │        2         1         3         3         2         3
   3 │        2         1         1         3         3         3
   4 │        1         1         1         1         1         1
   5 │        2         2         2         1         3         1
   6 │        2         2         2         1         1         1
   7 │        2         2         3         3         1         2
   8 │        3         3         3         1         1         1
   9 │        3         1         1         3         1         1
  10 │        1         1         3         1         1         3
```

## Filtering

* [Filtering based on conditions comparing one column to other columns](https://discourse.julialang.org/t/dataframe-filtering-based-on-conditions-comparing-one-column-to-other-columns/70802) :
In the following example we like to filter rows where columns `:x1` and `:x2` are greater than `:x5`.

```julia
julia> ds = Dataset(rand(10, 5), :auto)
10×5 Dataset
 Row │ x1         x2        x3          x4         x5         
     │ identity   identity  identity    identity   identity   
     │ Float64?   Float64?  Float64?    Float64?   Float64?   
─────┼────────────────────────────────────────────────────────
   1 │ 0.399934   0.855727  0.0807368   0.862328   0.941958
   2 │ 0.71988    0.392431  0.00232494  0.79383    0.0166697
   3 │ 0.9772     0.605505  0.0674462   0.188414   0.885489
   4 │ 0.469928   0.926895  0.0541956   0.329512   0.973956
   5 │ 0.888978   0.529202  0.4728      0.234747   0.00479635
   6 │ 0.955468   0.756809  0.285243    0.78437    0.674951
   7 │ 0.0518425  0.835133  0.24403     0.0364176  0.716292
   8 │ 0.942624   0.345449  0.349736    0.346877   0.449795
   9 │ 0.209472   0.385161  0.118644    0.714821   0.955488
  10 │ 0.162146   0.602297  0.515762    0.145972   0.341771

julia> val_x5 = ds[:, :x5];

julia> op(x, y; colcomp = val_x5) = x .&= y .> colcomp
op (generic function with 1 method)

julia> byrow(ds, mapreduce, 1:2, op = op, init = ones(Bool, nrow(ds)))
10-element Vector{Bool}:
 0
 1
 0
 0
 1
 1
 0
 0
 0
 0
```

## Grouping

* [How to remove rows based on next value in a sequence?](https://stackoverflow.com/questions/69762612/how-to-remove-rows-based-on-next-value-in-a-sequence-pandas) : I have a data set where it is grouped based on `:id` and in ascending order for `:date`. I want to remove a row if the row after it has the same `:outcome`.

```julia
julia> ds = Dataset(id = [1,1,1,1,1,2,2,2,3,3,3],
                    date = Date.(["2019-03-05", "2019-03-12", "2019-04-10",
                            "2019-04-29", "2019-05-10", "2019-03-20",
                            "2019-04-22", "2019-05-04", "2019-11-01",
                            "2019-11-10", "2019-12-12"]),
                    outcome = [false, false, false, true, false, false,
                               true, false, true, true, true])
11×3 Dataset
 Row │ id        date        outcome  
     │ identity  identity    identity
     │ Int64?    Date?       Bool?    
─────┼────────────────────────────────
   1 │        1  2019-03-05     false
   2 │        1  2019-03-12     false
   3 │        1  2019-04-10     false
   4 │        1  2019-04-29      true
   5 │        1  2019-05-10     false
   6 │        2  2019-03-20     false
   7 │        2  2019-04-22      true
   8 │        2  2019-05-04     false
   9 │        3  2019-11-01      true
  10 │        3  2019-11-10      true
  11 │        3  2019-12-12      true

julia> combine(gatherby(ds, [1, 3], isgathered = true),
                        (:) => last,
                        dropgroupcols = true)
7×3 Dataset
 Row │ id_last   date_last   outcome_last
     │ identity  identity    identity     
     │ Int64?    Date?       Bool?        
─────┼────────────────────────────────────
   1 │        1  2019-04-10         false
   2 │        1  2019-04-29          true
   3 │        1  2019-05-10         false
   4 │        2  2019-03-20         false
   5 │        2  2019-04-22          true
   6 │        2  2019-05-04         false
   7 │        3  2019-12-12          true
```
