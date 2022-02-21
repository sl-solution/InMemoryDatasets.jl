# Performance tips

This section contains some performance tips which can improve the experience of working with InMemoryDatasets. These tips are specially important when the package is handling huge data.

## Make use of provided APIs

Every data manipulation, data cleaning, data wrangling should be done via the provided APIs, i.e. don't modify columns out of those APIs. For example, changing values of a column or creating a new column using a `for loop` must be done via `modify!`, `modify` or similar functions,

```jldoctest
julia> ds = Dataset(x = [1,2,1,2], y = [1.5,3.4,-10,2.3])
4×2 Dataset
 Row │ x         y        
     │ identity  identity
     │ Int64?    Float64?
─────┼────────────────────
   1 │        1       1.5
   2 │        2       3.4
   3 │        1     -10.0
   4 │        2       2.3

julia> function r_f!(x)
           for i in 2:length(x)
               if x[i-1] < 2.0
                   x[i] = missing
               end
           end
           x
       end
r_f! (generic function with 1 method)

julia> modify!(groupby(ds, 1), :y=>r_f!)
4×2 Dataset
 Row │ x         y         
     │ identity  identity  
     │ Int64?    Float64?  
─────┼─────────────────────
   1 │        1        1.5
   2 │        2        3.4
   3 │        1  missing   
   4 │        2        2.3

julia> map!(filter(ds, :x, by = ==(2), view = true), x->x+2, :x);

julia> ds
4×2 Dataset
 Row │ x         y         
     │ identity  identity  
     │ Int64?    Float64?  
─────┼─────────────────────
   1 │        1        1.5
   2 │        4        3.4
   3 │        1  missing   
   4 │        4        2.3
```

## Avoid pushing single values

This is because every single change in a data set will trigger multiple functions (InMemoryDatasets changes the modified date, whether the format should be dropped, ...) and the overhead will be significant.

## Avoid using `String` type for large data sets

`String` in julia causes significant Garbage Collection overhead. InMemoryDatasets needs to create many intermediate arrays during its operations and `GC` degrades its performance. Use `PooledArrays` or fixed width Strings for such scenarios.

## Choose `groupby` or `gatherby` based on the problem in hand

Beside the order of the output, note that `groupby` and `gatherby` use very different approaches for grouping observations. `groupby/!` utilises the multithreading efficiently however, `gatherby` exploits the fast path of computations for some specific operations and usually has lower memory footprint.

## Beware that every column must support `missings`

Every columns in InMemoryDatasets will be converted to support `missing`. Thus, it is wise to create the vectors in that way. For example, if you load an `Arrow` file which its columns don't support missing values, InMemoryDatasets materialised the whole file, but if they already support missing values, InMemoryDatasets uses the memory map for accessing values.

## Master `byrow`

`byrow` uses efficient algorithms to apply functions on each row of a data set. It is fine tuned for some specific functions, which are listed in its docstring. And it is the core function for filtering observations, so mastering its capabilities is essential for working with data sets.

## Don't avoid `for loops`!

Julia is very fast program and users don't need to think about vectorisation for the sake of performance. Actually, using loop usually is a better choice and reduces the memory allocations. Thus, users don't need to avoid `for loops` and are encouraged to use them. However, remember to wrap your for loops in a function and pass the function to an appropriate API in InMemoryDatasets, e.g. `modify!`.
