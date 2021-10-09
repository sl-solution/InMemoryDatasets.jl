"""
    leftjoin(dsl, dsr; on)

Returns all rows from the left `ds`. If the `on` clause matches no records for some rows in the right `ds`, leave `missing` in the place.

# Examples

```jldoctest
julia> name = Dataset(ID = Union{Int, Missing}[1, 2, 3],
                       Name = Union{String, Missing}["John Doe", "Jane Doe", "Joe Blogs"])
3×2 Dataset
 Row │ ID        Name
     │ identity  identity
     │ Int64?    String?
─────┼─────────────────────
   1 │        1  John Doe
   2 │        2  Jane Doe
   3 │        3  Joe Blogs

julia> job = Dataset(ID = Union{Int, Missing}[1, 2, 2, 4],
                       Job = Union{String, Missing}["Lawyer", "Doctor", "Florist", "Farmer"])
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

julia> setformat!(name, 1=>isodd)
3×2 Dataset
 Row │ ID      Name
     │ isodd   identity
     │ Int64?  String?
─────┼───────────────────
   1 │   true  John Doe
   2 │  false  Jane Doe
   3 │   true  Joe Blogs

julia> setformat!(job, 1=>isodd)
4×2 Dataset
 Row │ ID      Job
     │ isodd   identity
     │ Int64?  String?
─────┼──────────────────
   1 │   true  Lawyer
   2 │  false  Doctor
   3 │  false  Florist
   4 │  false  Farmer

julia> leftjoin(name, job, on = :ID, mapformats = [true, true]) # The mapformats argument takes a Bool vector standing for whether to use format for dsl & dsr. 
5×3 Dataset
 Row │ ID      Name       Job
     │ isodd   identity   identity
     │ Int64?  String?    String?
─────┼─────────────────────────────
   1 │   true  John Doe   Lawyer
   2 │  false  Jane Doe   Doctor
   3 │  false  Jane Doe   Florist
   4 │  false  Jane Doe   Farmer
   5 │   true  Joe Blogs  Lawyer

julia> leftjoin(name, job, on = :ID, mapformats = [false, false])
4×3 Dataset
 Row │ ID      Name       Job
     │ isodd   identity   identity
     │ Int64?  String?    String?
─────┼─────────────────────────────
   1 │   true  John Doe   missing
   2 │  false  Jane Doe   Doctor
   3 │  false  Jane Doe   Florist
   4 │   true  Joe Blogs  missing
```
"""
function DataAPI.leftjoin(dsl::Dataset, dsr::Dataset; on = nothing, makeunique = false, mapformats::Union{Bool, Vector{Bool}} = true, stable = false, alg = HeapSort, check = true)
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
        onleft = index(dsl)[on]
        onright = index(dsr)[on]
        _join_left(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, makeunique = makeunique, mapformats = mapformats, stable = stable, alg = alg, check = check)
    elseif (typeof(on) <: AbstractVector{<:Pair{Symbol, Symbol}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        onleft = index(dsl)[map(x->x.first, on)]
        onright = index(dsr)[map(x->x.second, on)]
        _join_left(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, makeunique = makeunique, mapformats = mapformats, stable = stable, alg = alg, check = check)
    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end
end
function leftjoin!(dsl::Dataset, dsr::Dataset; on = nothing, makeunique = false, mapformats::Union{Bool, Vector{Bool}} = true, stable = false, alg = HeapSort, check = true)
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
        onleft = index(dsl)[on]
        onright = index(dsr)[on]
        _join_left!(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, makeunique = makeunique, mapformats = mapformats, stable = stable, alg = alg, check = check)
    elseif (typeof(on) <: AbstractVector{<:Pair{Symbol, Symbol}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        onleft = index(dsl)[map(x->x.first, on)]
        onright = index(dsr)[map(x->x.second, on)]
        _join_left!(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, makeunique = makeunique, mapformats = mapformats, stable = stable, alg = alg, check = check)
    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end
end

function DataAPI.rightjoin(dsl::Dataset, dsr::Dataset; on = nothing, makeunique = false,  mapformats::Union{Bool, Vector{Bool}} = true, stable = false, alg = HeapSort, check = true)
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
        onleft = index(dsl)[on]
        onright = index(dsr)[on]
        _join_left(dsr, dsl, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onright, onright = onleft, makeunique = makeunique, mapformats = mapformats, stable = stable, alg = alg, check = check)
    elseif (typeof(on) <: AbstractVector{<:Pair{Symbol, Symbol}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        onleft = index(dsl)[map(x->x.first, on)]
        onright = index(dsr)[map(x->x.second, on)]
        _join_left(dsr, dsl, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onright, onright = onleft, makeunique = makeunique, mapformats = mapformats, stable = stable, alg = alg, check = check)
    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end
end

"""
    innerjoin(dsl, dsr; on)

Returns all rows where matches values exist `on` the field for both `ds`s.

# Examples

```jldoctest
julia> name = Dataset(ID = Union{Int, Missing}[1, 2, 3],
                              Name = Union{String, Missing}["John Doe", "Jane Doe", "Joe Blogs"])
3×2 Dataset
 Row │ ID        Name
     │ identity  identity
     │ Int64?    String?
─────┼─────────────────────
   1 │        1  John Doe
   2 │        2  Jane Doe
   3 │        3  Joe Blogs

julia> job = Dataset(ID = Union{Int, Missing}[1, 2, 2, 4],
                              Job = Union{String, Missing}["Lawyer", "Doctor", "Florist", "Farmer"])
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

julia> setformat!(name, 1=>isodd)
3×2 Dataset
 Row │ ID      Name
     │ isodd   identity
     │ Int64?  String?
─────┼───────────────────
   1 │   true  John Doe
   2 │  false  Jane Doe
   3 │   true  Joe Blogs

julia> setformat!(job, 1=>isodd)
4×2 Dataset
 Row │ ID      Job
     │ isodd   identity
     │ Int64?  String?
─────┼──────────────────
   1 │   true  Lawyer
   2 │  false  Doctor
   3 │  false  Florist
   4 │  false  Farmer

julia> innerjoin(name, job, on = :ID, mapformats = [true, true]) # The mapformats argument takes a Bool vector standing for whether to use format for dsl & dsr.
5×3 Dataset
 Row │ ID      Name       Job
     │ isodd   identity   identity
     │ Int64?  String?    String?
─────┼─────────────────────────────
   1 │   true  John Doe   Lawyer
   2 │  false  Jane Doe   Doctor
   3 │  false  Jane Doe   Florist
   4 │  false  Jane Doe   Farmer
   5 │   true  Joe Blogs  Lawyer

julia> innerjoin(name, job, on = :ID, mapformats = [false, false])
2×3 Dataset
 Row │ ID      Name      Job
     │ isodd   identity  identity
     │ Int64?  String?   String?
─────┼────────────────────────────
   1 │  false  Jane Doe  Doctor
   2 │  false  Jane Doe  Florist
```
"""
function DataAPI.innerjoin(dsl::Dataset, dsr::Dataset; on = nothing, makeunique = false, mapformats::Union{Bool, Vector{Bool}} = true, stable = false, alg = HeapSort, check = true)
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
        onleft = index(dsl)[on]
        onright = index(dsr)[on]
        _join_inner(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, makeunique = makeunique, mapformats = mapformats, stable = stable, alg = alg, check = check)
    elseif (typeof(on) <: AbstractVector{<:Pair{Symbol, Symbol}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        onleft = index(dsl)[map(x->x.first, on)]
        onright = index(dsr)[map(x->x.second, on)]
        _join_inner(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, makeunique = makeunique, mapformats = mapformats, stable = stable, alg = alg, check = check)
    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end
end

"""
    outerjoin(dsl, dsr; on)

Returns all rows if there are matching values `on` the field either in the left `ds` or in the right `ds`.

# Examples

```jldoctest
julia> name = Dataset(ID = Union{Int, Missing}[1, 2, 3],
                              Name = Union{String, Missing}["John Doe", "Jane Doe", "Joe Blogs"])
3×2 Dataset
 Row │ ID        Name
     │ identity  identity
     │ Int64?    String?
─────┼─────────────────────
   1 │        1  John Doe
   2 │        2  Jane Doe
   3 │        3  Joe Blogs

julia> job = Dataset(ID = Union{Int, Missing}[1, 2, 2, 4],
                              Job = Union{String, Missing}["Lawyer", "Doctor", "Florist", "Farmer"])
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

julia> setformat!(name, 1=>isodd)
3×2 Dataset
 Row │ ID      Name
     │ isodd   identity
     │ Int64?  String?
─────┼───────────────────
   1 │   true  John Doe
   2 │  false  Jane Doe
   3 │   true  Joe Blogs

julia> setformat!(job, 1=>isodd)
4×2 Dataset
 Row │ ID      Job
     │ isodd   identity
     │ Int64?  String?
─────┼──────────────────
   1 │   true  Lawyer
   2 │  false  Doctor
   3 │  false  Florist
   4 │  false  Farmer

julia> outerjoin(name, job, on = :ID, mapformats = [true, true]) # The mapformats argument takes a Bool vector standing for whether to use format for dsl & dsr.
5×3 Dataset
 Row │ ID      Name       Job
     │ isodd   identity   identity
     │ Int64?  String?    String?
─────┼─────────────────────────────
   1 │   true  John Doe   Lawyer
   2 │  false  Jane Doe   Doctor
   3 │  false  Jane Doe   Florist
   4 │  false  Jane Doe   Farmer
   5 │   true  Joe Blogs  Lawyer

julia> outerjoin(name, job, on = :ID, mapformats = [false, false])
6×3 Dataset
 Row │ ID      Name       Job
     │ isodd   identity   identity
     │ Int64?  String?    String?
─────┼─────────────────────────────
   1 │   true  John Doe   missing
   2 │  false  Jane Doe   Doctor
   3 │  false  Jane Doe   Florist
   4 │   true  Joe Blogs  missing
   5 │  false  missing    Farmer
   6 │   true  missing    Lawyer
```
"""
function DataAPI.outerjoin(dsl::Dataset, dsr::Dataset; on = nothing, makeunique = false,  mapformats::Union{Bool, Vector{Bool}} = true, stable = false, alg = HeapSort, check = true)
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
        onleft = index(dsl)[on]
        onright = index(dsr)[on]
        _join_outer(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, makeunique = makeunique, mapformats = mapformats, stable = stable, alg = alg, check = check)
    elseif (typeof(on) <: AbstractVector{<:Pair{Symbol, Symbol}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        onleft = index(dsl)[map(x->x.first, on)]
        onright = index(dsr)[map(x->x.second, on)]
        _join_outer(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, makeunique = makeunique, mapformats = mapformats, stable = stable, alg = alg, check = check)
    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end
end

"""
    contains(main, transaction; on)

returns a boolean vector where is true when the key for the
corresponding row in the `main` data set is found in the transaction data set.

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
```
"""
function Base.contains(main::Dataset, transaction::Dataset; on = nothing,  mapformats::Union{Bool, Vector{Bool}} = true, stable = false, alg = HeapSort)
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
        onleft = index(main)[on]
        onright = index(transaction)[on]
        _in(main, transaction, nrow(transaction) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, mapformats = mapformats, stable = stable, alg = alg)
    elseif (typeof(on) <: AbstractVector{<:Pair{Symbol, Symbol}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        onleft = index(main)[map(x->x.first, on)]
        onright = index(transaction)[map(x->x.second, on)]
        _in(main, transaction, nrow(transaction) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, mapformats = mapformats, stable = stable, alg = alg)
    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end
end


"""
    antijoin(dsl, dsr; on)

Opposite to `semijoin`, returns only records in the left `ds` with rows that have `NO` matching values `on` the field in the right `ds`.

# Examples

```jldoctest
julia> name = Dataset(ID = Union{Int, Missing}[1, 2, 3],
                              Name = Union{String, Missing}["John Doe", "Jane Doe", "Joe Blogs"])
3×2 Dataset
 Row │ ID        Name
     │ identity  identity
     │ Int64?    String?
─────┼─────────────────────
   1 │        1  John Doe
   2 │        2  Jane Doe
   3 │        3  Joe Blogs

julia> job = Dataset(ID = Union{Int, Missing}[1, 2, 2, 4],
                              Job = Union{String, Missing}["Lawyer", "Doctor", "Florist", "Farmer"])
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
```
"""
function DataAPI.antijoin(dsl::Dataset, dsr::Dataset; on = nothing,  mapformats::Union{Bool, Vector{Bool}} = true, stable = false, alg = HeapSort)
    # on === nothing && throw(ArgumentError("`on` keyword must be specified"))
    # if !(on isa AbstractVector)
    #     on = [on]
    # else
    #     on = on
    # end
    # if typeof(on) <: AbstractVector{<:Union{AbstractString, Symbol}}
    #     onleft = index(dsl)[on]
    #     onright = index(dsr)[on]
        dsl[.!contains(dsl, dsr, on = on, mapformats = mapformats, stable = stable, alg = alg), :]
        # _join_anti(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, makeunique = makeunique, check = check)
    # elseif (typeof(on) <: AbstractVector{<:Pair{Symbol, Symbol}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
    #     onleft = index(dsl)[map(x->x.first, on)]
    #     onright = index(dsr)[map(x->x.second, on)]
        # _join_anti(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, makeunique = makeunique, check = check)
    # else
    #     throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    # end
end
"""
    semijoin(dsl, dsr; on)

Returns only records in the left `ds` with rows that have matching values `on` the field in the right `ds`.

# Examples

```jldoctest
julia> name = Dataset(ID = Union{Int, Missing}[1, 2, 3],
                              Name = Union{String, Missing}["John Doe", "Jane Doe", "Joe Blogs"])
3×2 Dataset
 Row │ ID        Name
     │ identity  identity
     │ Int64?    String?
─────┼─────────────────────
   1 │        1  John Doe
   2 │        2  Jane Doe
   3 │        3  Joe Blogs

julia> job = Dataset(ID = Union{Int, Missing}[1, 2, 2, 4],
                              Job = Union{String, Missing}["Lawyer", "Doctor", "Florist", "Farmer"])
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
```
"""
function DataAPI.semijoin(dsl::Dataset, dsr::Dataset; on = nothing, mapformats::Union{Bool, Vector{Bool}} = true, stable = false, alg = HeapSort)
    dsl[contains(dsl, dsr, on = on, mapformats = mapformats, stable = stable, alg = alg), :]
end
function antijoin!(dsl::Dataset, dsr::Dataset; on = nothing, mapformats::Union{Bool, Vector{Bool}} = true, stable = false, alg = HeapSort)
    delete!(dsl, contains(dsl, dsr, on = on, mapformats = mapformats, stable = stable, alg = alg))
end
function semijoin!(dsl::Dataset, dsr::Dataset; on = nothing,  mapformats::Union{Bool, Vector{Bool}} = true, stable = false, alg = HeapSort)
    delete!(dsl, .!contains(dsl, dsr, on = on, mapformats = mapformats, stable = stable, alg = alg))
end


"""
    closejoin(dsl, dsr; on)

Joins two data sets based on exact match on the key variable or the closest match when the exact match doesn't exist.

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

julia> closejoin(classA, grades, on = :mark) # Here values in the right ds are entries and exits points.
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


julia> dsl = Dataset([Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
                Union{Missing, Int64}[10, 3, 4, 1, 5, 5, 6, 7, 2, 10],
                Union{Missing, Int64}[3, 6, 7, 10, 10, 5, 10, 9, 1, 1],
                Union{Missing, Int64}[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]], ["x1", "x2", "x3", "row"])
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

julia> closejoin(dsl, dsr, on = :x1, mapformats = [true, true]) # The mapformats argument takes a Bool vector standing for whether to use format for dsl & dsr.
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

julia> closejoin(dsl, dsr, on = :x1, direction = :forward) # Set direction to forward. In default, direction = :backward.
10×5 Dataset
 Row │ x1      x2        x3        row       y
     │ iseven  identity  identity  identity  identity
     │ Int64?  Int64?    Int64?    Int64?    Float64?
─────┼────────────────────────────────────────────────
   1 │   true        10         3         1     100.0
   2 │  false         3         6         2     100.0
   3 │   true         4         7         3     100.0
   4 │  false         1        10         4     100.0
   5 │  false         5        10         5     100.0
   6 │  false         5         5         6     100.0
   7 │   true         6        10         7     100.0
   8 │  false         7         9         8     100.0
   9 │   true         2         1         9     100.0
  10 │   true        10         1        10     100.0

julia> closejoin(dsl, dsr, on = :x1, border = :nearest) # Set border to nearest In default, border = :missing.
10×5 Dataset
 Row │ x1      x2        x3        row       y
     │ iseven  identity  identity  identity  identity
     │ Int64?  Int64?    Int64?    Int64?    Float64?
─────┼────────────────────────────────────────────────
   1 │   true        10         3         1     200.0
   2 │  false         3         6         2     100.0
   3 │   true         4         7         3     200.0
   4 │  false         1        10         4     100.0
   5 │  false         5        10         5     100.0
   6 │  false         5         5         6     100.0
   7 │   true         6        10         7     200.0
   8 │  false         7         9         8     100.0
   9 │   true         2         1         9     200.0
  10 │   true        10         1        10     200.0
```
"""
function closejoin(dsl::Dataset, dsr::Dataset; on = nothing, direction = :backward, makeunique = false, border = :missing,  mapformats::Union{Bool, Vector{Bool}} = true, stable = true, alg = HeapSort)
    on === nothing && throw(ArgumentError("`on` keyword must be specified"))
    if !(border ∈ (:nearest, :missing))
        throw(ArgumentError("`border` keyword only accept :nearest or :missing"))
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
        onleft = index(dsl)[on]
        onright = index(dsr)[on]
        # length(onleft) > 1 && throw(ArgumentError("for `asofjoin` only one column must be specified for the `on` keyword"))
        if direction == :backward
            _join_asofback(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, makeunique = makeunique, border = border, mapformats = mapformats, stable = stable, alg = alg)
        elseif direction == :forward
            _join_asoffor(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, makeunique = makeunique, border = border, mapformats = mapformats, stable = stable, alg = alg)
        else
            throw(ArgumentError("`direction` can be only :backward or :forward"))
        end

    elseif (typeof(on) <: AbstractVector{<:Pair{Symbol, Symbol}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        onleft = index(dsl)[map(x->x.first, on)]
        onright = index(dsr)[map(x->x.second, on)]
        # length(onleft) > 1 && throw(ArgumentError("for `asofjoin` only one column must be specified for the `on` keyword"))
        if direction == :backward
            _join_asofback(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, makeunique = makeunique, border = border, mapformats = mapformats, stable = stable, alg = alg)
        elseif direction == :forward
            _join_asoffor(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, makeunique = makeunique, border = border, mapformats = mapformats, stable = stable, alg = alg)
        else
            throw(ArgumentError("`direction` can be only :backward or :forward"))
        end
    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end
end

function closejoin!(dsl::Dataset, dsr::Dataset; on = nothing, direction = :backward, makeunique = false, border = :missing, mapformats::Union{Bool, Vector{Bool}} = true, stable = true, alg = HeapSort)
    on === nothing && throw(ArgumentError("`on` keyword must be specified"))
    if !(border ∈ (:nearest, :missing))
        throw(ArgumentError("`border` keyword only accept :nearest or :missing"))
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
        onleft = index(dsl)[on]
        onright = index(dsr)[on]
        # length(onleft) > 1 && throw(ArgumentError("for `asofjoin` only one column must be specified for the `on` keyword"))
        if direction == :backward
            _join_asofback!(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, makeunique = makeunique, border = border, mapformats = mapformats, stable = stable, alg = alg)
        elseif direction == :forward
            _join_asoffor!(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, makeunique = makeunique, border = border, mapformats = mapformats, stable = stable, alg = alg)
        else
            throw(ArgumentError("`direction` can be only :backward or :forward"))
        end

    elseif (typeof(on) <: AbstractVector{<:Pair{Symbol, Symbol}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        onleft = index(dsl)[map(x->x.first, on)]
        onright = index(dsr)[map(x->x.second, on)]
        # length(onleft) > 1 && throw(ArgumentError("for `asofjoin` only one column must be specified for the `on` keyword"))
        if direction == :backward
            _join_asofback!(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, makeunique = makeunique, border = border, mapformats = mapformats, stable = stable, alg = alg)
        elseif direction == :forward
            _join_asoffor!(dsl, dsr, nrow(dsr) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, makeunique = makeunique, border = border, mapformats = mapformats, stable = stable, alg = alg)
        else
            throw(ArgumentError("`direction` can be only :backward or :forward"))
        end
    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end
end

function update!(dsmain::Dataset, dsupdate::Dataset; on = nothing, allowmissing = false, mode = :all,  mapformats::Union{Bool, Vector{Bool}} = true, stable = true, alg = HeapSort)
    on === nothing && throw(ArgumentError("`on` keyword must be specified"))
    !(mode ∈ (:all, :missing, :missings))  && throw(ArgumentError("`mode` can be either :all or :missing"))
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
        onleft = index(dsmain)[on]
        onright = index(dsupdate)[on]
        _update!(dsmain, dsupdate, nrow(dsupdate) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, allowmissing = allowmissing, mode = mode, mapformats = mapformats, stable = stable, alg = alg)
    elseif (typeof(on) <: AbstractVector{<:Pair{Symbol, Symbol}}) || (typeof(on) <: AbstractVector{<:Pair{<:AbstractString, <:AbstractString}})
        onleft = index(dsmain)[map(x->x.first, on)]
        onright = index(dsupdate)[map(x->x.second, on)]
        _update!(dsmain, dsupdate, nrow(dsupdate) < typemax(Int32) ? Val(Int32) : Val(Int64), onleft = onleft, onright = onright, allowmissing = allowmissing, mode = mode, mapformats = mapformats, stable = stable, alg = alg)
    else
        throw(ArgumentError("`on` keyword must be a vector of column names or a vector of pairs of column names"))
    end
    dsmain
end
update(dsmain::Dataset, dsupdate::Dataset; on = nothing, allowmissing = false, mode = :all,  mapformats::Union{Bool, Vector{Bool}} = true, stable = true, alg = HeapSort) = update!(copy(dsmain), dsupdate; on = on, allowmissing = allowmissing, mode = mode,  mapformats = mapformats, stable = stable, alg = alg)
