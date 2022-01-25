# TODO the docstring needs some updates, also some of the keyword arguments are missing int he docstring, e.g. accelerate, usehash

"""
    leftjoin(dsl::AbstractDataset, dsr::AbstractDataset; on=nothing, makeunique=false, mapformats=true, alg=HeapSort, stable=false, check=true, accelerate = false, method = :sort, threads = true)

Perform a left join of two `Datasets`: `dsl` and `dsr`, and return a `Dataset` containing all rows from the left table `dsl`.

If the `on` clause matches no records for some rows in the right table `ds`, leave `missing` in the place.

The order of rows will be the same as the left table `dsl`. When multiple matches exist in the right table, their order
will be as they appear if the `stable = true`, otherwise no specific rule is followed.

# Arguments
- `dsl` & `dsr`: two `Dataset`: the left table and the right table to be joined.

# Key Arguments
- `on`: can be a single column name, a vector of column names or a vector of pairs of column names, known as keys that the join function will based on.
- `makeunique`: by default is set to `false`, and there will be an error message if duplicate names are found in columns not joined;
  setting it to `true` if there are duplicated column names to make them unique.
- `mapformats`: is set to `true` by default, which means formatted values are used for matching observations for both `dsl` and `dsr`;
  you can use the function `getformat` to see the format;
  by setting `mapformats` to a `Bool Vector` of length 2, you can specify whether to use formatted values
  for `dsl` and `dsr`, respectively; for example, passing a `[true, false]` means use formatted values for `dsl` and do not use formatted values for `dsr`.
- `method` is either `:sort` or `:hash` for specifiying the method of match finding, default is `:sort`
- `alg`: sorting algorithms used, is `HeapSort` (the Heap Sort algorithm) by default;
  it can also be `QuickSort` (the Quicksort algorithm).
- `stable`: by default is `false`, means that the sorting results have not to be stable;
  if it is set to `true`, then sorting for `dsr` have to be stable.
- `check`: to check whether the output is too large (10 times greater than number of rows if `dsl`, an AssertionError will be raised in this case),
  it is set to `true` by default; if `false` is passed, the function will not check the output size.

See also: [`leftjoin!`](@ref)

# Examples

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

julia> dsl = Dataset(year = [Date("2020-3-1"), Date("2021-10-21"), Date("2020-1-4"), Date("2012-12-11")], leap_year = [true, false, true, true])
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

julia> setformat!(dsl, 1 => year) # Extract years from dates.
4×2 Dataset
 Row │ year   leap_year
     │ year   identity
     │ Date?  Bool?
─────┼──────────────────
   1 │ 2020        true
   2 │ 2021       false
   3 │ 2020        true
   4 │ 2012        true

julia> leftjoin(dsl, dsr, on = :year, mapformats = true) # Use formats for datasets. The mapformats is true in default.
4×3 Dataset
 Row │ year   leap_year  event
     │ year   identity   identity
     │ Date?  Bool?      Char?
─────┼────────────────────────────
   1 │ 2020        true  A
   2 │ 2021       false  B
   3 │ 2020        true  A
   4 │ 2012        true  missing
```
"""
function DataAPI.leftjoin(dsl::AbstractDataset, dsr::AbstractDataset; on = nothing, makeunique = false, mapformats::Union{Bool, Vector{Bool}} = true, stable = false, alg = HeapSort, check = true, accelerate = false, method::Symbol = :sort, threads::Bool = true)
    !(method in (:hash, :sort)) && throw(ArgumentError("method must be :hash or :sort"))
    on === nothing && throw(ArgumentError("`on` keyword must be specified"))
    if !(on isa AbstractVector)
        on = [on]
    else
        on = on
    end

    if !(mapformats isa AbstractVector)
        mapformats = repeat([mapformats], 2)
    else
        length(mapformats) !== 2 && throw(ArgumentError("`mapformats` must be a Bool or a vector of Bool with size two"))
    end

    if typeof(on) <: AbstractVector{<:Union{AbstractString, Symbol}}
        onleft = multiple_getindex(index(dsl), on)
        onright = multiple_getindex(index(dsr), on)

    elseif (typeof(on) <: AbstractVector{<:Pair{<:ColumnIndex, <:ColumnIndex}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        onleft = multiple_getindex(index(dsl), map(x->x.first, on))
        onright = multiple_getindex(index(dsr), map(x->x.second, on))

    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end
    _join_left(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, makeunique = makeunique, mapformats = mapformats, stable = stable, alg = alg, check = check, accelerate = accelerate, method = method, threads = threads)

end
"""
    leftjoin!(dsl::Dataset, dsr::AbstractDataset; on=nothing, makeunique=false, mapformats=true, alg=HeapSort, stable=false, accelerate = false, method = :sort, threads = true)

Variant of `leftjoin` that performs `leftjoin` in place for special case that the number of matching rows from the right data set is at most one.
```
"""
function leftjoin!(dsl::Dataset, dsr::AbstractDataset; on = nothing, makeunique = false, mapformats::Union{Bool, Vector{Bool}} = true, stable = false, alg = HeapSort, accelerate = false, method::Symbol = :sort, threads::Bool = true)
    !(method in (:hash, :sort)) && throw(ArgumentError("method must be :hash or :sort"))
    on === nothing && throw(ArgumentError("`on` keyword must be specified"))
    if !(on isa AbstractVector)
        on = [on]
    else
        on = on
    end
    if !(mapformats isa AbstractVector)
        mapformats = repeat([mapformats], 2)
    else
        length(mapformats) !== 2 && throw(ArgumentError("`mapformats` must be a Bool or a vector of Bool with size two"))
    end
    if typeof(on) <: AbstractVector{<:Union{AbstractString, Symbol}}
        onleft = multiple_getindex(index(dsl), on)
        onright = multiple_getindex(index(dsr), on)

    elseif (typeof(on) <: AbstractVector{<:Pair{<:ColumnIndex, <:ColumnIndex}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        onleft = multiple_getindex(index(dsl), map(x->x.first, on))
        onright = multiple_getindex(index(dsr), map(x->x.second, on))

    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end
    _join_left!(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, makeunique = makeunique, mapformats = mapformats, check = false, method = method, threads = threads)
end

"""
    innerjoin(dsl::AbstractDataset, dsr::AbstractDataset; on=nothing, makeunique=false, mapformats=true, alg=HeapSort, stable=false, check=true, accelerate = false, method = :sort, strict_inequality = false, droprangecols = true, threads = true)

Perform a inner join of two `Datasets`: `dsl` and `dsr`, and return a `Dataset`
containing all rows where matching values exist `on` the keys for both `dsl` and `dsr`.

The order of rows will be the same as the left table `dsl`,
rows that have values in `dsl` while do not have matching values `on` keys in `dsr` will be removed. When multiple matches exist in the right table, their order
will be as they appear if the `stable = true`, otherwise no specific rule is followed.

# Arguments
- `dsl` & `dsr`: two `Dataset`: the left table and the right table to be joined.

# Key Arguments
- `on`: can be a single column name, a vector of column names or a vector of pairs of column names, known as keys that the join function will based on. When an inequlity-like innerjoin is needed, the last key for the right data set should be passed as `Tuple` of column names or column index.
- `makeunique`: by default is set to `false`, and there will be an error message if duplicate names are found in columns not joined;
  setting it to `true` if there are duplicated column names to make them unique.
- `mapformats`: is set to `true` by default, which means formatted values are used for matching observations for both `dsl` and `dsr`;
  you can use the function `getformat` to see the format;
  by setting `mapformats` to a `Bool Vector` of length 2, you can specify whether to use formatted values
  for `dsl` and `dsr`, respectively; for example, passing a `[true, false]` means use formatted values for `dsl` and do not use formatted values for `dsr`.
- `method` is either `:sort` or `:hash` for specifiying the method of match finding, default is `:sort`
- `droprangecols`: determine whether the range columns should be dropped from the final output in case of inequality-like innerjoin. Default is set to `true`
- `strict_inequality`: determine if the inequlities in inequlity-like innerjoins are strict. Default is set to `false`.
- `alg`: sorting algorithms used, is `HeapSort` (the Heap Sort algorithm) by default;
  it can also be `QuickSort` (the Quicksort algorithm).
- `stable`: by default is `false`, means that the sorting results have not to be stable;
  if it is set to `true`, then sorting for `dsr` have to be stable.
- `check`: to check whether the output is too large (10 times greater than number of rows if `dsl`, an AssertionError will be raised in this case),
  it is set to `true` by default; if `false` is passed, the function will not check the output size.

# Examples

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

julia> innerjoin(name, job, on = :ID)
3×3 Dataset
 Row │ ID        Name      Job
     │ identity  identity  identity
     │ Int64?    String?   String?
─────┼──────────────────────────────
   1 │        1  John Doe  Lawyer
   2 │        2  Jane Doe  Doctor
   3 │        2  Jane Doe  Florist

julia> dsl = Dataset(year = [Date("2020-3-1"), Date("2021-10-21"), Date("2020-1-4"), Date("2012-12-11")], leap_year = [true, false, true, true])
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

julia> setformat!(dsl, 1 => year) # Extract years from dates.
4×2 Dataset
 Row │ year   leap_year
     │ year   identity
     │ Date?  Bool?
─────┼──────────────────
   1 │ 2020        true
   2 │ 2021       false
   3 │ 2020        true
   4 │ 2012        true

julia> innerjoin(dsl, dsr, on = :year, mapformats = true) # Use formats for datasets. The mapformats is true in default.
3×3 Dataset
 Row │ year   leap_year  event
     │ year   identity   identity
     │ Date?  Bool?      Char?
─────┼────────────────────────────
   1 │ 2020        true  A
   2 │ 2021       false  B
   3 │ 2020        true  A

julia> dsl = Dataset(id = [1,1,2,2], x = [100.0, 210.0, 55.5, 150.0])
4×2 Dataset
 Row │ id        x
     │ identity  identity
     │ Int64?    Float64?
─────┼────────────────────
   1 │        1     100.0
   2 │        1     210.0
   3 │        2      55.5
   4 │        2     150.0

julia> dsr = Dataset(id = [1,2,3], lower = [110,110,200], value = [1200,2030,1300])
3×3 Dataset
 Row │ id        lower     value
     │ identity  identity  identity
     │ Int64?    Int64?    Int64?
─────┼──────────────────────────────
   1 │        1       110      1200
   2 │        2       110      2030
   3 │        3       200      1300

julia> innerjoin(dsl, dsr, on = [1=>1, 2=>(:lower, nothing)])
2×3 Dataset
 Row │ id        x         value
     │ identity  identity  identity
     │ Int64?    Float64?  Int64?
─────┼──────────────────────────────
   1 │        1     210.0      1200
   2 │        2     150.0      2030

```
"""
function DataAPI.innerjoin(dsl::AbstractDataset, dsr::AbstractDataset; on = nothing, makeunique = false, mapformats::Union{Bool, Vector{Bool}} = true, stable = false, alg = HeapSort, check = true, accelerate = false, droprangecols::Bool = true, strict_inequality = false, method = :sort, threads::Bool = true)
    !(method in (:hash, :sort)) && throw(ArgumentError("method must be :hash or :sort"))
    on === nothing && throw(ArgumentError("`on` keyword must be specified"))
    if !(on isa AbstractVector)
        on = [on]
    else
        on = on
    end
    if !(mapformats isa AbstractVector)
        mapformats = repeat([mapformats], 2)
    else
        length(mapformats) !== 2 && throw(ArgumentError("`mapformats` must be a Bool or a vector of Bool with size two"))
    end
    if !(strict_inequality isa AbstractVector)
        strict_inequality = repeat([strict_inequality], 2)
    else
        length(strict_inequality) !== 2 && throw(ArgumentError("`strict_inequality` must be a Bool or a vector of Bool with size two"))
    end
    if typeof(on) <: AbstractVector{<:Union{AbstractString, Symbol}}
        onleft = multiple_getindex(index(dsl), on)
        onright = multiple_getindex(index(dsr), on)
        onright_range = nothing

    elseif (typeof(on) <: AbstractVector{<:Pair{<:ColumnIndex, <:ColumnIndex}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        onleft = multiple_getindex(index(dsl), map(x->x.first, on))
        onright = multiple_getindex(index(dsr), map(x->x.second, on))
        onright_range = nothing

    elseif (typeof(on) <: AbstractVector{<:Pair{<:ColumnIndex, <:Any}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:Any}})
        onleft = multiple_getindex(index(dsl), map(x->x.first, on))
        onright = multiple_getindex(index(dsr), map(x->x.second, on[1:end-1]))
        onright_range = on[end].second
        !(onright_range isa Tuple) && throw(ArgumentError("For range join the last element of `on` keyword argument for the right table must be a Tuple of column names"))

    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end
    _join_inner(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, onright_range = onright_range, makeunique = makeunique, mapformats = mapformats, stable = stable, alg = alg, check = check, accelerate = accelerate, droprangecols = droprangecols, strict_inequality = strict_inequality, method = method, threads = threads)
end

"""
    outerjoin(dsl::AbstractDataset, dsr::AbstractDataset; on=nothing, makeunique=false, mapformats=true, alg=HeapSort, stable=false, check=true, accelerate = false, method = :sort, threads = true)

Perform an outer join of two `Datasets`: `dsl` and `dsr`, and return a `Dataset`
containing all rows where keys appear in either `dsl` or `dsr`.

The output contains two part.
For the first part, the order of rows will be the same as the left table `dsl` if keys appear in `dsl`;
for the second part, some other rows that have values in `dsr` while do not have matching values `on` keys in `dsl`
will be added after the first part. No rule governs the order of observation for the second part when `method = :sort`.

# Arguments
- `dsl` & `dsr`: two `Dataset`: the left table and the right table to be joined.

# Key Arguments
- `on`: can be a single column name, a vector of column names or a vector of pairs of column names, known as keys that the join function will based on.
- `makeunique`: by default is set to `false`, and there will be an error message if duplicate names are found in columns not joined;
  setting it to `true` if there are duplicated column names to make them unique.
- `mapformats`: is set to `true` by default, which means formatted values are used for matching observations for both `dsl` and `dsr`;
  you can use the function `getformat` to see the format;
  by setting `mapformats` to a `Bool Vector` of length 2, you can specify whether to use formatted values
  for `dsl` and `dsr`, respectively; for example, passing a `[true, false]` means use formatted values for `dsl` and do not use formatted values for `dsr`.
- `method` is either `:sort` or `:hash` for specifiying the method of match finding, default is `:sort`
- `alg`: sorting algorithms used, is `HeapSort` (the Heap Sort algorithm) by default;
  it can also be `QuickSort` (the Quicksort algorithm).
- `stable`: by default is `false`, means that the sorting results have not to be stable;
  if it is set to `true`, then sorting for `dsr` have to be stable.
- `check`: to check whether the output is too large (10 times greater than number of rows if `dsl`, an AssertionError will be raised in this case),
  it is set to `true` by default; if `false` is passed, the function will not check the output size.

# Examples

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

julia> dsl = Dataset(year = [Date("2020-3-1"), Date("2021-10-21"), Date("2020-1-4"), Date("2012-12-11")], leap_year = [true, false, true, true])
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

julia> setformat!(dsl, 1 => year) # Extract years from dates.
4×2 Dataset
 Row │ year   leap_year
     │ year   identity
     │ Date?  Bool?
─────┼──────────────────
   1 │ 2020        true
   2 │ 2021       false
   3 │ 2020        true
   4 │ 2012        true

julia> outerjoin(dsl, dsr, on = :year, mapformats = true) # Use formats for datasets. The mapformats is true in default.
4×3 Dataset
 Row │ year   leap_year  event
     │ year   identity   identity
     │ Date?  Bool?      Char?
─────┼────────────────────────────
   1 │ 2020        true  A
   2 │ 2021       false  B
   3 │ 2020        true  A
   4 │ 2012        true  missing
```
"""
function DataAPI.outerjoin(dsl::AbstractDataset, dsr::AbstractDataset; on = nothing, makeunique = false,  mapformats::Union{Bool, Vector{Bool}} = true, stable = false, alg = HeapSort, check = true, accelerate = false, method = :sort, threads::Bool = true)
    !(method in (:hash, :sort)) && throw(ArgumentError("method must be :hash or :sort"))
    on === nothing && throw(ArgumentError("`on` keyword must be specified"))
    if !(on isa AbstractVector)
        on = [on]
    else
        on = on
    end
    if !(mapformats isa AbstractVector)
        mapformats = repeat([mapformats], 2)
    else
        length(mapformats) !== 2 && throw(ArgumentError("`mapformats` must be a Bool or a vector of Bool with size two"))
    end
    if typeof(on) <: AbstractVector{<:Union{AbstractString, Symbol}}
        onleft = multiple_getindex(index(dsl), on)
        onright = multiple_getindex(index(dsr), on)

    elseif (typeof(on) <: AbstractVector{<:Pair{<:ColumnIndex, <:ColumnIndex}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        onleft = multiple_getindex(index(dsl), map(x->x.first, on))
        onright = multiple_getindex(index(dsr), map(x->x.second, on))

    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end
    _join_outer(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, makeunique = makeunique, mapformats = mapformats, stable = stable, alg = alg, check = check, accelerate = accelerate, method = method, threads = threads)
end

"""
    contains(main::AbstractDataset, transaction::AbstractDataset; on, mapformats = true, alg = HeapSort, stable = false, accelerate = false, method = :hash, strict_inequality = false, threads = true)

returns a boolean vector where is true when the key for the
corresponding row in the `main` data set is found in the transaction data set.

- `on`: can be a single column name, a vector of column names or a vector of pairs of column names, known as keys that the join function will based on. When an inequlity-like `contains` is needed, the last key for the right data set should be passed as `Tuple` of column names or column index.
- `method` is either `:sort` or `:hash` for specifiying the method of match finding, default is `:hash`

# Examples

```jldoctest
julia> main = Dataset(g1 = [1,2,3,4,1,4,1,2], x1 = 1:8)
8×2 Dataset
 Row │ g1        x1
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │        1         1
   2 │        2         2
   3 │        3         3
   4 │        4         4
   5 │        1         5
   6 │        4         6
   7 │        1         7
   8 │        2         8

julia> tds = Dataset(group = [1,2,3])
3×1 Dataset
 Row │ group
     │ identity
     │ Int64?
─────┼──────────
   1 │        1
   2 │        2
   3 │        3

julia> contains(main, tds, on = :g1 => :group)
8-element Vector{Bool}:
 1
 1
 1
 0
 1
 0
 1
 1

julia> dsl = Dataset(x1 = [1,2,1,3], y = [-1.2,-3,2.1,-3.5])
4×2 Dataset
 Row │ x1        y
     │ identity  identity
     │ Int64?    Float64?
─────┼────────────────────
   1 │        1      -1.2
   2 │        2      -3.0
   3 │        1       2.1
   4 │        3      -3.5

julia> dsr = Dataset(x1 = [1,2,3], lower = [0, -3,1], upper = [3,0,2])
3×3 Dataset
 Row │ x1        lower     upper
     │ identity  identity  identity
     │ Int64?    Int64?    Int64?
─────┼──────────────────────────────
   1 │        1         0         3
   2 │        2        -3         0
   3 │        3         1         2

julia> contains(dsl, dsr, on = [1=>1, 2=>(2,3)], method = :hash, strict_inequality = true)
4-element Vector{Bool}:
 0
 0
 1
 0
```
"""
function Base.contains(main::AbstractDataset, transaction::AbstractDataset; on = nothing,  mapformats::Union{Bool, Vector{Bool}} = true, stable = false, alg = HeapSort, accelerate = false, method = :hash, threads::Bool = true,  strict_inequality = false)
    !(method in (:hash, :sort)) && throw(ArgumentError("method must be :hash or :sort"))
    on === nothing && throw(ArgumentError("`on` keyword must be specified"))
    if !(on isa AbstractVector)
        on = [on]
    else
        on = on
    end
    if !(mapformats isa AbstractVector)
        mapformats = repeat([mapformats], 2)
    else
        length(mapformats) !== 2 && throw(ArgumentError("`mapformats` must be a Bool or a vector of Bool with size two"))
    end
    if !(strict_inequality isa AbstractVector)
        strict_inequality = repeat([strict_inequality], 2)
    else
        length(strict_inequality) !== 2 && throw(ArgumentError("`strict_inequality` must be a Bool or a vector of Bool with size two"))
    end
    if typeof(on) <: AbstractVector{<:Union{AbstractString, Symbol}}
        onleft = multiple_getindex(index(main), on)
        onright = multiple_getindex(index(transaction), on)
    elseif (typeof(on) <: AbstractVector{<:Pair{<:ColumnIndex, <:ColumnIndex}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        onleft = multiple_getindex(index(main), map(x->x.first, on))
        onright = multiple_getindex(index(transaction), map(x->x.second, on))
    elseif (typeof(on) <: AbstractVector{<:Pair{<:ColumnIndex, <:Any}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:Any}})
        onleft = multiple_getindex(index(main), map(x->x.first, on))
        onright = multiple_getindex(index(transaction), map(x->x.second, on[1:end-1]))
        onright_range = on[end].second
        !(onright_range isa Tuple) && throw(ArgumentError("For contains the last element of `on` keyword argument for the right table must be a Tuple of column names"))
        ranges = _join_inner(main, transaction, nrow(transaction) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, onright_range = onright_range, makeunique = true, mapformats = mapformats, stable = stable, alg = alg, check = false, accelerate = accelerate, droprangecols = true, strict_inequality = strict_inequality, method = method, threads = threads, onlyreturnrange = true)
        return map(x -> length(x) == 0 ? false : true, ranges)
    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end
    if method == :hash
        _in_hash(main, transaction, nrow(transaction) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, mapformats = mapformats, threads = threads)
    elseif method == :sort
        _in(main, transaction, nrow(transaction) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, mapformats = mapformats, stable = stable, alg = alg, accelerate = accelerate, threads = threads)
    end

end

"""
    antijoin(dsl::AbstractDataset, dsr::AbstractDataset; on=nothing, makeunique=false, mapformats=true, alg=HeapSort, stable=false, view = false, accelerate = false, method = :hash, strict_inequality = false, threads = true)

Opposite to `semijoin`, perform an anti join of two `Datasets`: `dsl` and `dsr`, and return a `Dataset`
containing rows where keys appear in `dsl` but not in `dsr`.
The resulting `Dataset` will only contain columns in the left table `dsl`.

The order of rows will be the same as the left table `dsl`,
rows that have key values appear in `dsr` will be removed.

# Arguments
- `dsl` & `dsr`: two `Dataset`: the left table and the right table to be joined.

# Key Arguments
- `on`: can be a single column name, a vector of column names or a vector of pairs of column names, known as keys that the join function will based on. When an inequlity-like `antijoin` is needed, the last key for the right data set should be passed as `Tuple` of column names or column index.
- `makeunique`: by default is set to `false`, and there will be an error message if duplicate names are found in columns not joined;
  setting it to `true` if there are duplicated column names to make them unique.
- `mapformats`: is set to `true` by default, which means formatted values are used for matching observations for both `dsl` and `dsr`;
  you can use the function `getformat` to see the format;
  by setting `mapformats` to a `Bool Vector` of length 2, you can specify whether to use formatted values
  for `dsl` and `dsr`, respectively; for example, passing a `[true, false]` means use formatted values for `dsl` and do not use formatted values for `dsr`.
- `method` is either `:sort` or `:hash` for specifiying the method of match finding, default is `:hash`
- `alg`: sorting algorithms used, is `HeapSort` (the Heap Sort algorithm) by default;
  it can also be `QuickSort` (the Quicksort algorithm).
- `stable`: by default is `false`, means that the sorting results have not to be stable;
  if it is set to `true`, then sorting for `dsr` have to be stable.

See also: [`antijoin!`](@ref)

# Examples

```jldoctest
julia> name = Dataset(ID = [1, 2, 3],
                              Name = ["John Doe", "Jane Doe", "Joe Blogs"])
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

julia> antijoin(name, job, on = :ID)
1×2 Dataset
 Row │ ID        Name
     │ identity  identity
     │ Int64?    String?
─────┼─────────────────────
   1 │        3  Joe Blogs


julia> dsl = Dataset(year = [Date("2020-3-1"), Date("2021-10-21"), Date("2020-1-4"), Date("2012-12-11")], leap_year = [true, false, true, true])
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

julia> setformat!(dsl, 1 => year) # Extract years from dates.
4×2 Dataset
 Row │ year   leap_year
     │ year   identity
     │ Date?  Bool?
─────┼──────────────────
   1 │ 2020        true
   2 │ 2021       false
   3 │ 2020        true
   4 │ 2012        true

julia> antijoin(dsl, dsr, on = :year, mapformats = true) # Use formats for datasets. The mapformats is true in default.
1×2 Dataset
 Row │ year   leap_year
     │ year   identity
     │ Date?  Bool?
─────┼──────────────────
   1 │ 2012        true
```
"""
function DataAPI.antijoin(dsl::AbstractDataset, dsr::AbstractDataset; on = nothing,  mapformats::Union{Bool, Vector{Bool}} = true, stable = false, alg = HeapSort, accelerate = false, view = false, method = :hash, threads = true, strict_inequality = false)
    !(method in (:hash, :sort)) && throw(ArgumentError("method must be :hash or :sort"))
    if view
        Base.view(dsl, .!contains(dsl, dsr, on = on, mapformats = mapformats, stable = stable, alg = alg, accelerate = accelerate, method = method, threads = threads, strict_inequality = strict_inequality), :)
    else
        dsl[.!contains(dsl, dsr, on = on, mapformats = mapformats, stable = stable, alg = alg, accelerate = accelerate, method = method, threads = threads, strict_inequality = strict_inequality), :]
    end
end
"""
    semijoin(dsl::AbstractDataset, dsr::AbstractDataset; on=nothing, makeunique=false, mapformats=true, alg=HeapSort, stable=false, view = false, accelerate = false, method = :hash, strict_inequality = false, threads = true)

Perform a semi join of two `Datasets`: `dsl` and `dsr`, and return a `Dataset`
containing rows where keys appear in `dsl` and `dsr`.
The resulting `Dataset` will only contain columns in the left table `dsl`.

The order of rows will be the same as the left table `dsl`,
rows that have values in `dsl` while do not have matching values `on` keys in `dsr` will be removed.

# Arguments
- `dsl` & `dsr`: two `Dataset`: the left table and the right table to be joined.

# Key Arguments
- `on`: can be a single column name, a vector of column names or a vector of pairs of column names, known as keys that the join function will based on. When an inequlity-like `semijoin` is needed, the last key for the right data set should be passed as `Tuple` of column names or column index.
- `makeunique`: by default is set to `false`, and there will be an error message if duplicate names are found in columns not joined;
  setting it to `true` if there are duplicated column names to make them unique.
- `mapformats`: is set to `true` by default, which means formatted values are used for matching observations for both `dsl` and `dsr`;
  you can use the function `getformat` to see the format;
  by setting `mapformats` to a `Bool Vector` of length 2, you can specify whether to use formatted values
  for `dsl` and `dsr`, respectively; for example, passing a `[true, false]` means use formatted values for `dsl` and do not use formatted values for `dsr`.
- `method` is either `:sort` or `:hash` for specifiying the method of match finding, default is `:hash`
- `alg`: sorting algorithms used, is `HeapSort` (the Heap Sort algorithm) by default;
  it can also be `QuickSort` (the Quicksort algorithm).
- `stable`: by default is `false`, means that the sorting results have not to be stable;
  if it is set to `true`, then sorting for `dsr` have to be stable.

See also: [`semijoin!`](@ref)

# Examples

```jldoctest
julia> name = Dataset(ID = [1, 2, 3],
                              Name = ["John Doe", "Jane Doe", "Joe Blogs"])
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

julia> semijoin(name, job, on = :ID)
2×2 Dataset
 Row │ ID        Name
     │ identity  identity
     │ Int64?    String?
─────┼────────────────────
   1 │        1  John Doe
   2 │        2  Jane Doe

julia> dsl = Dataset(year = [Date("2020-3-1"), Date("2021-10-21"), Date("2020-1-4"), Date("2012-12-11")], leap_year = [true, false, true, true])
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

julia> setformat!(dsl, 1 => year) # Extract years from dates.
4×2 Dataset
 Row │ year   leap_year
     │ year   identity
     │ Date?  Bool?
─────┼──────────────────
   1 │ 2020        true
   2 │ 2021       false
   3 │ 2020        true
   4 │ 2012        true

julia> semijoin(dsl, dsr, on = :year, mapformats = true) # Use formats for datasets. The mapformats is true in default.
3×2 Dataset
 Row │ year   leap_year
     │ year   identity
     │ Date?  Bool?
─────┼──────────────────
   1 │ 2020        true
   2 │ 2021       false
   3 │ 2020        true
```
"""
function DataAPI.semijoin(dsl::AbstractDataset, dsr::AbstractDataset; on = nothing, mapformats::Union{Bool, Vector{Bool}} = true, stable = false, alg = HeapSort, accelerate = false, view = false, method = :hash, threads = true, strict_inequality = false)
    !(method in (:hash, :sort)) && throw(ArgumentError("method must be :hash or :sort"))
    if view
        Base.view(dsl, contains(dsl, dsr, on = on, mapformats = mapformats, stable = stable, alg = alg, accelerate = accelerate, method = method, threads = threads, strict_inequality = strict_inequality), :)
    else
        dsl[contains(dsl, dsr, on = on, mapformats = mapformats, stable = stable, alg = alg, accelerate = accelerate, method = method, threads = threads, strict_inequality = strict_inequality), :]
    end
end
"""
    antijoin!(dsl::Dataset, dsr::AbstractDataset; on=nothing, makeunique=false, mapformats=true, alg=HeapSort, stable=false, accelerate = false, method = :hash, strict_inequality = false, threads = true)

Opposite to `semijoin`, perform an anti join of two `Datasets`: `dsl` and `dsr`, and change the left table `dsl` into a `Dataset`
containing rows where keys appear in `dsl` but not in `dsr`.
The resulting `Dataset` will only contain columns in the original left table `dsl`.

The order of rows will be the same as the original left table `dsl`,
rows that have key values appear in `dsr` will be removed.

# Arguments
- `dsl` & `dsr`: two `Dataset`: the left table and the right table to be joined.

# Key Arguments
- `on`: can be a single column name, a vector of column names or a vector of pairs of column names, known as keys that the join function will based on. When an inequlity-like `antijoin!` is needed, the last key for the right data set should be passed as `Tuple` of column names or column index.
- `makeunique`: by default is set to `false`, and there will be an error message if duplicate names are found in columns not joined;
  setting it to `true` if there are duplicated column names to make them unique.
- `mapformats`: is set to `true` by default, which means formatted values are used for matching observations for both `dsl` and `dsr`;
  you can use the function `getformat` to see the format;
  by setting `mapformats` to a `Bool Vector` of length 2, you can specify whether to use formatted values
  for `dsl` and `dsr`, respectively; for example, passing a `[true, false]` means use formatted values for `dsl` and do not use formatted values for `dsr`.
- `method` is either `:sort` or `:hash` for specifiying the method of match finding, default is `:hash`
- `alg`: sorting algorithms used, is `HeapSort` (the Heap Sort algorithm) by default;
  it can also be `QuickSort` (the Quicksort algorithm).
- `stable`: by default is `false`, means that the sorting results have not to be stable;
  if it is set to `true`, then sorting for `dsr` have to be stable.

See also: [`antijoin`](@ref)

# Examples

```jldoctest
julia> name = Dataset(ID = [1, 2, 3],
                              Name = ["John Doe", "Jane Doe", "Joe Blogs"])
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

julia> antijoin!(name, job, on = :ID)
1×2 Dataset
 Row │ ID        Name
     │ identity  identity
     │ Int64?    String?
─────┼─────────────────────
   1 │        3  Joe Blogs

julia> name
1×2 Dataset
 Row │ ID        Name
     │ identity  identity
     │ Int64?    String?
─────┼─────────────────────
   1 │        3  Joe Blogs

julia> dsl = Dataset(year = [Date("2020-3-1"), Date("2021-10-21"), Date("2020-1-4"), Date("2012-12-11")], leap_year = [true, false, true, true])
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

julia> setformat!(dsl, 1 => year) # Extract years from dates.
4×2 Dataset
 Row │ year   leap_year
     │ year   identity
     │ Date?  Bool?
─────┼──────────────────
   1 │ 2020        true
   2 │ 2021       false
   3 │ 2020        true
   4 │ 2012        true

julia> antijoin!(dsl, dsr, on = :year, mapformats = true) # Use formats for datasets. The mapformats is true in default.
1×2 Dataset
 Row │ year   leap_year
     │ year   identity
     │ Date?  Bool?
─────┼──────────────────
   1 │ 2012        true

julia> dsl
1×2 Dataset
 Row │ year   leap_year
     │ year   identity
     │ Date?  Bool?
─────┼──────────────────
   1 │ 2012        true
```
"""
function antijoin!(dsl::Dataset, dsr::AbstractDataset; on = nothing, mapformats::Union{Bool, Vector{Bool}} = true, stable = false, alg = HeapSort, accelerate = false, method = :hash, threads = true, strict_inequality = false)
    !(method in (:hash, :sort)) && throw(ArgumentError("method must be :hash or :sort"))
    deleteat!(dsl, contains(dsl, dsr, on = on, mapformats = mapformats, stable = stable, alg = alg, accelerate = accelerate, method = method, threads = threads, strict_inequality = strict_inequality))
end
"""
    semijoin!(dsl::Dataset, dsr::AbstractDataset; on=nothing, makeunique=false, mapformats=true, alg=HeapSort, stable=false, accelerate = false, method = :hash, strict_inequality = false, threads = true)

Perform a semi join of two `Datasets`: `dsl` and `dsr`, and change the left table `dsl` into a `Dataset`
containing rows where keys appear in `dsl` and `dsr`.
The resulting `Dataset` will only contain columns in the original left table `dsl`.

The order of rows will be the same as the original left table `dsl`,
rows that have values in `dsl` while do not have matching values `on` keys in `dsr` will be removed.

# Arguments
- `dsl` & `dsr`: two `Dataset`: the left table and the right table to be joined.

# Key Arguments
- `on`: can be a single column name, a vector of column names or a vector of pairs of column names, known as keys that the join function will based on. When an inequlity-like `semijoin!` is needed, the last key for the right data set should be passed as `Tuple` of column names or column index.
- `makeunique`: by default is set to `false`, and there will be an error message if duplicate names are found in columns not joined;
  setting it to `true` if there are duplicated column names to make them unique.
- `mapformats`: is set to `true` by default, which means formatted values are used for matching observations for both `dsl` and `dsr`;
  you can use the function `getformat` to see the format;
  by setting `mapformats` to a `Bool Vector` of length 2, you can specify whether to use formatted values
  for `dsl` and `dsr`, respectively; for example, passing a `[true, false]` means use formatted values for `dsl` and do not use formatted values for `dsr`.
- `method` is either `:sort` or `:hash` for specifiying the method of match finding, default is `:hash`
- `alg`: sorting algorithms used, is `HeapSort` (the Heap Sort algorithm) by default;
  it can also be `QuickSort` (the Quicksort algorithm).
- `stable`: by default is `false`, means that the sorting results have not to be stable;
  if it is set to `true`, then sorting for `dsr` have to be stable.

See also: [`semijoin`](@ref)

# Examples

```jldoctest
julia> name = Dataset(ID = [1, 2, 3],
                              Name = ["John Doe", "Jane Doe", "Joe Blogs"])
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

julia> semijoin!(name, job, on = :ID)
2×2 Dataset
 Row │ ID        Name
     │ identity  identity
     │ Int64?    String?
─────┼────────────────────
   1 │        1  John Doe
   2 │        2  Jane Doe

julia> name
2×2 Dataset
 Row │ ID        Name
     │ identity  identity
     │ Int64?    String?
─────┼────────────────────
   1 │        1  John Doe
   2 │        2  Jane Doe


julia> dsl = Dataset(year = [Date("2020-3-1"), Date("2021-10-21"), Date("2020-1-4"), Date("2012-12-11")], leap_year = [true, false, true, true])
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

julia> setformat!(dsl, 1 => year) # Extract years from dates.
4×2 Dataset
 Row │ year   leap_year
     │ year   identity
     │ Date?  Bool?
─────┼──────────────────
   1 │ 2020        true
   2 │ 2021       false
   3 │ 2020        true
   4 │ 2012        true

julia> semijoin!(dsl, dsr, on = :year, mapformats = true) # Use formats for datasets. The mapformats is true in default.
3×2 Dataset
 Row │ year   leap_year
     │ year   identity
     │ Date?  Bool?
─────┼──────────────────
   1 │ 2020        true
   2 │ 2021       false
   3 │ 2020        true

julia> dsl
3×2 Dataset
 Row │ year   leap_year
     │ year   identity
     │ Date?  Bool?
─────┼──────────────────
   1 │ 2020        true
   2 │ 2021       false
   3 │ 2020        true
```
"""
function semijoin!(dsl::Dataset, dsr::AbstractDataset; on = nothing,  mapformats::Union{Bool, Vector{Bool}} = true, stable = false, alg = HeapSort, accelerate = false, method = :hash, threads = true, strict_inequality = false)
    deleteat!(dsl, .!contains(dsl, dsr, on = on, mapformats = mapformats, stable = stable, alg = alg, accelerate = accelerate, method = method, threads = threads, strict_inequality = strict_inequality))
end


"""
    closejoin(...)

Variant of `closejoin!` that returns an updated copy of `dsl` leaving `dsl` itself unmodified.
```
"""
function closejoin(dsl::AbstractDataset, dsr::AbstractDataset; on = nothing, direction = :backward, makeunique = false, border = :missing,  mapformats::Union{Bool, Vector{Bool}} = true, stable = true, alg = HeapSort, accelerate = false, tol=nothing, allow_exact_match = true, op = nothing, method = :sort, threads::Bool = true)
    !(method in (:hash, :sort)) && throw(ArgumentError("method must be :hash or :sort"))
    on === nothing && throw(ArgumentError("`on` keyword must be specified"))
    if !(border ∈ (:nearest, :missing, :none))
        throw(ArgumentError("`border` keyword only accept :nearest, :missing, or :none"))
    end
    if !(on isa AbstractVector)
        on = [on]
    else
        on = on
    end
    if !(mapformats isa AbstractVector)
        mapformats = repeat([mapformats], 2)
    else
        length(mapformats) !== 2 && throw(ArgumentError("`mapformats` must be a Bool or a vector of Bool with size two"))
    end

    if typeof(on) <: AbstractVector{<:Union{AbstractString, Symbol}}
        onleft = multiple_getindex(index(dsl), on)
        onright = multiple_getindex(index(dsr), on)
    elseif (typeof(on) <: AbstractVector{<:Pair{<:ColumnIndex, <:ColumnIndex}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        onleft = multiple_getindex(index(dsl), map(x->x.first, on))
        onright = multiple_getindex(index(dsr), map(x->x.second, on))
    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end
    if direction in (:backward, :forward, :nearest)
        _join_closejoin(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, makeunique = makeunique, border = border, mapformats = mapformats, stable = stable, alg = alg, accelerate = accelerate, direction = direction, tol=tol, allow_exact_match = allow_exact_match, op = op, method = method, threads = threads)

    else
        throw(ArgumentError("`direction` can be only :backward, :forward, or :nearest"))
    end
end

"""
    closejoin!(dsl::Dataset, dsr::AbstractDataset; on=nothing, direction=:backward, makeunique=false, border=:missing, mapformats=true, alg=HeapSort, stable=true, accelerate = false, tol = nothing, allow_exact_match = true, op = nothing, method = :sort, threads = true)

Perform a close join for two `Datasets` `dsl` & `dsr` and change the left table into a `Dataset`
based on exact matches on the key variable or the closest matches when the exact match doesn't exist.

The order of rows will be the same as the original left table `dsl`.  When there are multiple matches
in the close match phase, only one of them will be selected, and the selected one depends on the stability of sort and direction of match.

# Arguments
- `dsl` & `dsr`: two `Dataset`: the left table and the right table to be joined.

# Key Arguments
- `on`: can be a single column name, a vector of column names or a vector of pairs of column names, known as keys that the join function will based on.
- `direction`: direction of search in sorted `dsr` based on keys; by default, `:backward` is used
  and search direction are from the last row to the first row until the first value less than the key is found;
  setting to `:forward` can search for matching values top down until the first value larger than the key is found. Setting it to `:nearest` search in both direction and select the nearest one.
- `method` is either `:sort` or `:hash` for specifiying the method of match finding, default is `:sort`
- `makeunique`: by default is set to `false`, and there will be an error message if duplicate names are found in columns not joined;
  setting it to `true` if there are duplicated column names to make them unique.
- `border`: `:missing` is used by default for the border value,
  `:nearest` can be used to set border values to the nearest value rather than a `missing`,
  by setting `border = :none` any observation out of the right data set range will be set as missing.
- `mapformats`: is set to `true` by default, which means formatted values are used to match observations for both `dsl` and `dsr`;
  you can use the function `getformat` to see the format;
  by setting `mapformats` to a `Bool Vector` of length 2, you can specify whether to use formatted values
  for `dsl` and `dsr`, respectively; for example, passing a `[true, false]` means use formatted values for `dsl` and do not use formatted values for `dsr`.
- `tol`: Select close match only if the distance is less than it.
- `allow_exact_match`: If `true`, allows matching with the same key.
- `op`: When `direction = :nearest` user can supply an operator where `closejoin!` call on two nearest points from the right data set close to the currrent observation in the left data set. The order of the argument to `op` is the same as the sorted order of point. It is important that `op` be able to handle misssing values. If there are multiple columns in the right table to be joined to the left table, the `op` operation applies to all of them.
- `alg`: sorting algorithms used, is `HeapSort` (the Heap Sort algorithm) by default;
  it can also be `QuickSort` (the Quicksort algorithm).
- `stable`: by default is `true`, means that the sorting results have to be stable;
  if it is set to `false`, then sorting for `dsr` have not to be stable.
See also: [`closejoin`](@ref)

# Examples

```jldoctest
julia> classA = Dataset(id = ["id1", "id2", "id3", "id4", "id5"],
                               mark = [50, 69.5, 45.5, 88.0, 98.5])
5×2 Dataset
 Row │ id        mark
     │ identity  identity
     │ String?   Float64?
─────┼────────────────────
   1 │ id1           50.0
   2 │ id2           69.5
   3 │ id3           45.5
   4 │ id4           88.0
   5 │ id5           98.5

julia> grades = Dataset(mark = [0, 49.5, 59.5, 69.5, 79.5, 89.5, 95.5],
                               grade = ["F", "P", "C", "B", "A-", "A", "A+"])
7×2 Dataset
 Row │ mark      grade
     │ identity  identity
     │ Float64?  String?
─────┼────────────────────
   1 │      0.0  F
   2 │     49.5  P
   3 │     59.5  C
   4 │     69.5  B
   5 │     79.5  A-
   6 │     89.5  A
   7 │     95.5  A+

julia> closejoin!(classA, grades, on = :mark)
5×3 Dataset
 Row │ id        mark      grade
     │ identity  identity  identity
     │ String?   Float64?  String?
─────┼──────────────────────────────
   1 │ id1           50.0  P
   2 │ id2           69.5  B
   3 │ id3           45.5  F
   4 │ id4           88.0  A-
   5 │ id5           98.5  A+

julia> classA # The left table is changed.
5×3 Dataset
 Row │ id        mark      grade
     │ identity  identity  identity
     │ String?   Float64?  String?
─────┼──────────────────────────────
   1 │ id1           50.0  P
   2 │ id2           69.5  B
   3 │ id3           45.5  F
   4 │ id4           88.0  A-
   5 │ id5           98.5  A+

julia> dsl = Dataset([[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
                [10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
                [3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
                [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]], ["x1", "x2", "x3", "row"])
10×4 Dataset
 Row │ x1        x2        x3        row
     │ identity  identity  identity  identity
     │ Int64?    Int64?    Int64?    Int64?
─────┼────────────────────────────────────────
   1 │       10        10         3         1
   2 │        3         3         6         2
   3 │        4         4         7         3
   4 │        1         1        10         4
   5 │        5         5        10         5
   6 │        5         5         5         6
   7 │        6         6        10         7
   8 │        7         7         9         8
   9 │        2         2         1         9
  10 │       10        10         1        10

julia> dsr = Dataset(x1=[1, 3], y =[100.0, 200.0])
2×2 Dataset
 Row │ x1        y
     │ identity  identity
     │ Int64?    Float64?
─────┼────────────────────
   1 │        1     100.0
   2 │        3     200.0

julia> setformat!(dsl, 1=>iseven)
10×4 Dataset
 Row │ x1      x2        x3        row
     │ iseven  identity  identity  identity
     │ Int64?  Int64?    Int64?    Int64?
─────┼──────────────────────────────────────
   1 │   true        10         3         1
   2 │  false         3         6         2
   3 │   true         4         7         3
   4 │  false         1        10         4
   5 │  false         5        10         5
   6 │  false         5         5         6
   7 │   true         6        10         7
   8 │  false         7         9         8
   9 │   true         2         1         9
  10 │   true        10         1        10

julia> setformat!(dsr, 1=>isodd)
2×2 Dataset
 Row │ x1      y
     │ isodd   identity
     │ Int64?  Float64?
─────┼──────────────────
   1 │   true     100.0
   2 │   true     200.0

julia> closejoin!(dsl, dsr, on = :x1, mapformats = [true, true]) # The mapformats argument takes a Bool vector standing for whether to use format for dsl & dsr.
10×5 Dataset
 Row │ x1      x2        x3        row       y
     │ iseven  identity  identity  identity  identity
     │ Int64?  Int64?    Int64?    Int64?    Float64?
─────┼─────────────────────────────────────────────────
   1 │   true        10         3         1      200.0
   2 │  false         3         6         2  missing
   3 │   true         4         7         3      200.0
   4 │  false         1        10         4  missing
   5 │  false         5        10         5  missing
   6 │  false         5         5         6  missing
   7 │   true         6        10         7      200.0
   8 │  false         7         9         8  missing
   9 │   true         2         1         9      200.0
  10 │   true        10         1        10      200.0

julia> dsl # The left table has been changed after joining.
10×5 Dataset
 Row │ x1      x2        x3        row       y
     │ iseven  identity  identity  identity  identity
     │ Int64?  Int64?    Int64?    Int64?    Float64?
─────┼─────────────────────────────────────────────────
   1 │   true        10         3         1      200.0
   2 │  false         3         6         2  missing
   3 │   true         4         7         3      200.0
   4 │  false         1        10         4  missing
   5 │  false         5        10         5  missing
   6 │  false         5         5         6  missing
   7 │   true         6        10         7      200.0
   8 │  false         7         9         8  missing
   9 │   true         2         1         9      200.0
  10 │   true        10         1        10      200.0

# A financial example.
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

julia> modify!(trades, 1 => byrow(x -> DateTime(x, dateformat"yyyymmdd HH:MM:SS.s")));

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

julia> modify!(quotes, 1 => byrow(x -> DateTime(x, dateformat"yyyymmdd HH:MM:SS.s")));

julia> trades
5×4 Dataset
 Row │ time                     ticker    price     quantity
     │ identity                 identity  identity  identity
     │ DateTime?                String?   Float64?  Int64?
─────┼───────────────────────────────────────────────────────
   1 │ 2016-05-25T13:30:00.023  MSFT         51.95        75
   2 │ 2016-05-25T13:30:00.038  MSFT         51.95       155
   3 │ 2016-05-25T13:30:00.048  GOOG        720.77       100
   4 │ 2016-05-25T13:30:00.048  GOOG        720.92       100
   5 │ 2016-05-25T13:30:00.048  AAPL         98.0        100

julia> quotes
8×4 Dataset
 Row │ time                     ticker    bid       ask
     │ identity                 identity  identity  identity
     │ DateTime?                String?   Float64?  Float64?
─────┼───────────────────────────────────────────────────────
   1 │ 2016-05-25T13:30:00.023  GOOG        720.5     720.93
   2 │ 2016-05-25T13:30:00.023  MSFT         51.95     51.96
   3 │ 2016-05-25T13:30:00.030  MSFT         51.97     51.98
   4 │ 2016-05-25T13:30:00.041  MSFT         51.99     52.0
   5 │ 2016-05-25T13:30:00.048  GOOG        720.5     720.93
   6 │ 2016-05-25T13:30:00.049  AAPL         97.99     98.01
   7 │ 2016-05-25T13:30:00.072  GOOG        720.5     720.88
   8 │ 2016-05-25T13:30:00.075  MSFT         52.01     52.03

julia> closejoin!(trades, quotes, on = [:ticker, :time], border = :nearest)
5×6 Dataset
 Row │ time                     ticker    price     quantity  bid       ask
     │ identity                 identity  identity  identity  identity  identity
     │ DateTime?                String?   Float64?  Int64?    Float64?  Float64?
─────┼───────────────────────────────────────────────────────────────────────────
   1 │ 2016-05-25T13:30:00.023  MSFT         51.95        75     51.95     51.96
   2 │ 2016-05-25T13:30:00.038  MSFT         51.95       155     51.97     51.98
   3 │ 2016-05-25T13:30:00.048  GOOG        720.77       100    720.5     720.93
   4 │ 2016-05-25T13:30:00.048  GOOG        720.92       100    720.5     720.93
   5 │ 2016-05-25T13:30:00.048  AAPL         98.0        100     97.99     98.01

julia> trades # The left table has been changed after joining.
5×6 Dataset
 Row │ time                     ticker    price     quantity  bid       ask
     │ identity                 identity  identity  identity  identity  identity
     │ DateTime?                String?   Float64?  Int64?    Float64?  Float64?
─────┼───────────────────────────────────────────────────────────────────────────
   1 │ 2016-05-25T13:30:00.023  MSFT         51.95        75     51.95     51.96
   2 │ 2016-05-25T13:30:00.038  MSFT         51.95       155     51.97     51.98
   3 │ 2016-05-25T13:30:00.048  GOOG        720.77       100    720.5     720.93
   4 │ 2016-05-25T13:30:00.048  GOOG        720.92       100    720.5     720.93
   5 │ 2016-05-25T13:30:00.048  AAPL         98.0        100     97.99     98.01
```
"""
function closejoin!(dsl::Dataset, dsr::AbstractDataset; on = nothing, direction = :backward, makeunique = false, border = :missing, mapformats::Union{Bool, Vector{Bool}} = true, stable = true, alg = HeapSort, accelerate = false, tol = nothing, allow_exact_match = true, op = nothing, method = :sort, threads::Bool = true)
    !(method in (:hash, :sort)) && throw(ArgumentError("method must be :hash or :sort"))
    on === nothing && throw(ArgumentError("`on` keyword must be specified"))
    if !(border ∈ (:nearest, :missing, :none))
        throw(ArgumentError("`border` keyword only accept :nearest, :missing, or :none"))
    end
    if !(on isa AbstractVector)
        on = [on]
    else
        on = on
    end
    if !(mapformats isa AbstractVector)
        mapformats = repeat([mapformats], 2)
    else
        length(mapformats) !== 2 && throw(ArgumentError("`mapformats` must be a Bool or a vector of Bool with size two"))
    end
    if typeof(on) <: AbstractVector{<:Union{AbstractString, Symbol}}
        onleft = multiple_getindex(index(dsl), on)
        onright = multiple_getindex(index(dsr), on)

    elseif (typeof(on) <: AbstractVector{<:Pair{<:ColumnIndex, <:ColumnIndex}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        onleft = multiple_getindex(index(dsl), map(x->x.first, on))
        onright = multiple_getindex(index(dsr), map(x->x.second, on))
    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end
    if direction in (:backward, :forward, :nearest)
        _join_closejoin(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, makeunique = makeunique, border = border, mapformats = mapformats, stable = stable, alg = alg, accelerate = accelerate, direction = direction, inplace = true, tol = tol, allow_exact_match = allow_exact_match, op = op, method = method, threads = threads)

    else
        throw(ArgumentError("`direction` can be only :backward, :forward, or :nearest"))
    end
end

"""
    update!(dsmain::Dataset, dsupdate::AbstractDataset; on=nothing, allowmissing=false, mode=:missings, mapformats=true, alg=HeapSort, stable=true, accelerate = false, method = :sort, threads = true)

Update a `Dataset` `dsmain` with another `Dataset` `dsupdate` based `on` given keys for matching rows,
and change the left `Dataset` after updating.

Order of output will be the same as the main `Dataset` `dsmain`. In case of multiple match, the `stable` argument governs
the order of selected observation from the right table.

# Arguments
- `dsmain`: the main `Dataset` to be updated.
- `dsupdate`: the transaction `Dataset` used to update `dsmain`.

# Key Arguments
- `on`: can be a single column name, a vector of column names or a vector of pairs of column names, known as keys that the update function will based on.
- `allowmissing`: is set to `false` by default, so `missing` values in `dsupdate` will not replace the values in `dsmain`;
  change this to `true` can update `dsmain` using `missing` values in `dsupdate`.
- `mode`: by default is set to `:missings`, means that only rows in `dsmain` with `missing` values will be updated.
    changing it to `:all` means all matching rows based `on` keys will be updated. Otherwise a function can be passed as `mode` to update only observations which return true when `mode` call on them.
- `mapformats`: is set to `true` by default, which means formatted values are used for matching observations for both `dsl` and `dsr`;
  you can use the function `getformat` to see the format;
  by setting `mapformats` to a `Bool Vector` of length 2, you can specify whether to use formatted values
  for `dsl` and `dsr`, respectively; for example, passing a `[true, false]` means use formatted values for `dsl` and do not use formatted values for `dsr`.
- `method` is either `:sort` or `:hash` for specifiying the method of match finding, default is `:sort`
- `alg`: sorting algorithms used, is `HeapSort` (the Heap Sort algorithm) by default;
  it can also be `QuickSort` (the Quicksort algorithm).
- `stable`: by default is `true`, means that the sorting results have to be stable;
  if it is set to `false`, then sorting for `dsr` have not to be stable.

See also: [`update`](@ref)

# Examples

```jldoctest
julia> dsmain = Dataset(group = ["G1", "G1", "G1", "G1", "G2", "G2", "G2"],
                             id    = [ 1  ,  1  ,  2  ,  2  ,  1  ,  1  ,  2  ],
                             x1    = [1.2, 2.3,missing,  2.3, 1.3, 2.1  , 0.0 ],
                             x2    = [ 5  ,  4  ,  4  ,  2  , 1  ,missing, 2  ])
7×4 Dataset
 Row │ group     id        x1         x2
     │ identity  identity  identity   identity
     │ String?   Int64?    Float64?   Int64?
─────┼─────────────────────────────────────────
   1 │ G1               1        1.2         5
   2 │ G1               1        2.3         4
   3 │ G1               2  missing           4
   4 │ G1               2        2.3         2
   5 │ G2               1        1.3         1
   6 │ G2               1        2.1   missing
   7 │ G2               2        0.0         2

julia> dsupdate = Dataset(group = ["G1", "G2"], id = [2, 1],
                               x1 = [2.5, missing], x2 = [missing, 3])
2×4 Dataset
 Row │ group     id        x1         x2
     │ identity  identity  identity   identity
     │ String?   Int64?    Float64?   Int64?
─────┼─────────────────────────────────────────
   1 │ G1               2        2.5   missing
   2 │ G2               1  missing           3

julia> update!(dsmain, dsupdate, on = [:group, :id], mode = :missings) # Only missing rows are updated.
7×4 Dataset
 Row │ group     id        x1        x2
     │ identity  identity  identity  identity
     │ String?   Int64?    Float64?  Int64?
─────┼────────────────────────────────────────
   1 │ G1               1       1.2         5
   2 │ G1               1       2.3         4
   3 │ G1               2       2.5         4
   4 │ G1               2       2.3         2
   5 │ G2               1       1.3         1
   6 │ G2               1       2.1         3
   7 │ G2               2       0.0         2
```
"""
function update!(dsmain::Dataset, dsupdate::AbstractDataset; on = nothing, allowmissing = false, mode::Union{Symbol, Function} = :missings,  mapformats::Union{Bool, Vector{Bool}} = true, stable = true, alg = HeapSort, accelerate = false, method = :sort, threads::Bool = true)
    !(method in (:hash, :sort)) && throw(ArgumentError("method must be :hash or :sort"))
    on === nothing && throw(ArgumentError("`on` keyword must be specified"))
    mode isa Symbol && !(mode ∈ (:all, :missing, :missings))  && throw(ArgumentError("`mode` can be either :all or :missing"))
    if !(on isa AbstractVector)
        on = [on]
    else
        on = on
    end
    if !(mapformats isa AbstractVector)
        mapformats = repeat([mapformats], 2)
    else
        length(mapformats) !== 2 && throw(ArgumentError("`mapformats` must be a Bool or a vector of Bool with size two"))
    end
    if typeof(on) <: AbstractVector{<:Union{AbstractString, Symbol}}
        onleft = multiple_getindex(index(dsmain), on)
        onright = multiple_getindex(index(dsupdate), on)
    elseif (typeof(on) <: AbstractVector{<:Pair{<:ColumnIndex, <:ColumnIndex}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        onleft = multiple_getindex(index(dsmain), map(x->x.first, on))
        onright = multiple_getindex(index(dsupdate), map(x->x.second, on))
    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end
    _update!(dsmain, dsupdate, nrow(dsupdate) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, allowmissing = allowmissing, mode = mode, mapformats = mapformats, stable = stable, alg = alg, accelerate = accelerate, method = method, threads = threads)

    dsmain
end
"""
    update(...)

Variant of `update!` that returns an updated copy of `dsmain` leaving `dsmain` itself unmodified.
"""
update(dsmain::AbstractDataset, dsupdate::AbstractDataset; on = nothing, allowmissing = false, mode = :all,  mapformats::Union{Bool, Vector{Bool}} = true, stable = true, alg = HeapSort, accelerate = false, method = :sort, threads = true) = update!(copy(dsmain), dsupdate; on = on, allowmissing = allowmissing, mode = mode,  mapformats = mapformats, stable = stable, alg = alg, accelerate = accelerate, method = method, threads = threads)
