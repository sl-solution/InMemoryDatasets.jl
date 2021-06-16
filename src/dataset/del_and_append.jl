"""
    delete!(ds::Dataset, inds)

Delete rows specified by `inds` from a `Dataset` `ds` in place and return it.

Internally `deleteat!` is called for all columns so `inds` must be:
a vector of sorted and unique integers, a boolean vector, an integer, or `Not`.

# Examples
```jldoctest
julia> ds = Dataset(a=1:3, b=4:6)
3×2 Dataset
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      4
   2 │     2      5
   3 │     3      6

julia> delete!(ds, 2)
2×2 Dataset
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      4
   2 │     3      6
```

"""
function Base.delete!(ds::Dataset, inds)

# Modify Dataset
    if !isempty(inds) && size(ds, 2) == 0
        throw(BoundsError(ds, (inds, :)))
    end

    # we require ind to be stored and unique like in Base
    # otherwise an error will be thrown and the data set will get corrupted
    return _delete!_helper(ds, inds)
end

# Modify Dataset
function Base.delete!(ds::Dataset, inds::AbstractVector{Bool})
    if length(inds) != size(ds, 1)
        throw(BoundsError(ds, (inds, :)))
    end
    # drop = _findall(inds)
    return _delete!_helper(ds, inds)
end

# Modify Dataset
Base.delete!(ds::Dataset, inds::Not) = delete!(ds, axes(ds, 1)[inds])

# Modify Dataset
function _delete!_helper(ds::Dataset, drop)
    cols = _columns(ds)
    isempty(cols) && return ds

    n = nrow(ds)
    col1 = cols[1]
    deleteat!(col1, drop)
    newn = length(col1)

    for i in 2:length(cols)
        col = cols[i]
        if length(col) == n
            deleteat!(col, drop)
        end
    end

    for i in 1:length(cols)
        # this should never happen, but we add it for safety
        @assert length(cols[i]) == newn corrupt_msg(ds, i)
    end
    _reset_grouping_info!(ds)
    _modified(_attributes(ds))
    return ds
end

"""
    empty!(ds::Dataset)

Remove all rows from `ds`, making each of its columns empty.
"""
function Base.empty!(ds::Dataset)

# Modify Dataset
    foreach(empty!, eachcol(ds))
    _reset_grouping_info!(ds)
    _modified(_attributes(ds))
    return ds
end

"""
    append!(ds::Dataset, ds2::AbstractDataset; cols::Symbol=:setequal,
            promote::Bool=(cols in [:union, :subset]))
    append!(ds::Dataset, table; cols::Symbol=:setequal,
            promote::Bool=(cols in [:union, :subset]))

Add the rows of `ds2` to the end of `ds`. If the second argument `table` is not an
`AbstractDataset` then it is converted using `Dataset(table, copycols=false)`
before being appended.

The exact behavior of `append!` depends on the `cols` argument:
* If `cols == :setequal` (this is the default)
  then `ds2` must contain exactly the same columns as `ds` (but possibly in a
  different order).
* If `cols == :orderequal` then `ds2` must contain the same columns in the same
  order (for `AbstractDict` this option requires that `keys(row)` matches
  `propertynames(ds)` to allow for support of ordered dicts; however, if `ds2`
  is a `Dict` an error is thrown as it is an unordered collection).
* If `cols == :intersect` then `ds2` may contain more columns than `ds`, but all
  column names that are present in `ds` must be present in `ds2` and only these
  are used.
* If `cols == :subset` then `append!` behaves like for `:intersect` but if some
  column is missing in `ds2` then a `missing` value is pushed to `ds`.
* If `cols == :union` then `append!` adds columns missing in `ds` that are present
  in `ds2`, for columns present in `ds` but missing in `ds2` a `missing` value
  is pushed.

If `promote=true` and element type of a column present in `ds` does not allow
the type of a pushed argument then a new column with a promoted element type
allowing it is freshly allocated and stored in `ds`. If `promote=false` an error
is thrown.

The above rule has the following exceptions:
* If `ds` has no columns then copies of columns from `ds2` are added to it.
* If `ds2` has no columns then calling `append!` leaves `ds` unchanged.

Please note that `append!` must not be used on a `Dataset` that contains
columns that are aliases (equal when compared with `===`).

# See also

Use [`push!`](@ref) to add individual rows to a data set and [`vcat`](@ref)
to vertically concatenate data sets.

# Examples
```jldoctest
julia> ds1 = Dataset(A=1:3, B=1:3)
3×2 Dataset
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3

julia> ds2 = Dataset(A=4.0:6.0, B=4:6)
3×2 Dataset
 Row │ A        B
     │ Float64  Int64
─────┼────────────────
   1 │     4.0      4
   2 │     5.0      5
   3 │     6.0      6

julia> append!(ds1, ds2);

julia> ds1
6×2 Dataset
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3
   4 │     4      4
   5 │     5      5
   6 │     6      6
```
"""
function Base.append!(ds1::Dataset, ds2::AbstractDataset; cols::Symbol=:setequal,
                      promote::Bool=(cols in [:union, :subset]))
# appending ds2 to ds1 should keep the formats of ds1, and metadata of ds1, if ds1 is
# empty then formats of ds2 transfer to ds1, but not metadata
# Modify Dataset
    if !(cols in (:orderequal, :setequal, :intersect, :subset, :union))
        throw(ArgumentError("`cols` keyword argument must be " *
                            ":orderequal, :setequal, :intersect, :subset or :union)"))
    end

    if ncol(ds1) == 0
        for (n, v) in pairs(eachcol(ds2))
            f_col = getformat(ds2, n)
            ds1[!, n] = copy(v) # make sure ds1 does not reuse ds2
            setformat!(ds1, n => f_col)
        end
        _copy_grouping_info!(ds1, ds2)
        setinfo!(ds1, _attributes(ds2).meta.info[])
        return ds1
    end
    ncol(ds2) == 0 && return ds1

    if cols == :orderequal && _names(ds1) != _names(ds2)
        wrongnames = symdiff(_names(ds1), _names(ds2))
        if isempty(wrongnames)
            mismatches = findall(_names(ds1) .!= _names(ds2))
            @assert !isempty(mismatches)
            throw(ArgumentError("Columns number " *
                                join(mismatches, ", ", " and ") *
                                " do not have the same names in both passed " *
                                "data sets and `cols == :orderequal`"))
        else
            mismatchmsg = " Column names :" *
            throw(ArgumentError("Column names :" *
                                join(wrongnames, ", :", " and :") *
                                " were found in only one of the passed data sets " *
                                "and `cols == :orderequal`"))
        end
    elseif cols == :setequal
        wrongnames = symdiff(_names(ds1), _names(ds2))
        if !isempty(wrongnames)
            throw(ArgumentError("Column names :" *
                                join(wrongnames, ", :", " and :") *
                                " were found in only one of the passed data sets " *
                                "and `cols == :setequal`"))
        end
    elseif cols == :intersect
        wrongnames = setdiff(_names(ds1), _names(ds2))
        if !isempty(wrongnames)
            throw(ArgumentError("Column names :" *
                                join(wrongnames, ", :", " and :") *
                                " were found in only in destination data set " *
                                "and `cols == :intersect`"))
        end
    end

    nrows, ncols = size(ds1)
    targetrows = nrows + nrow(ds2)
    current_col = 0
    # in the code below we use a direct access to _columns because
    # we resize the columns so temporarily the `Dataset` is internally
    # inconsistent and normal data set indexing would error.

    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    ##### modify the code to take care of meta data
    try
        current_modified_time = _attributes(ds1).meta.modified[]
        for (j, n) in enumerate(_names(ds1))
            current_col += 1
            format_of_cur_col = getformat(ds1, n)
            if hasproperty(ds2, n)
                ds2_c = ds2[!, n]
                S = eltype(ds2_c)
                ds1_c = ds1[!, j]
                T = eltype(ds1_c)
                if S <: T || !promote || promote_type(S, T) <: T
                    # if S <: T || promote_type(S, T) <: T this should never throw an exception
                    append!(ds1_c, ds2_c)
                else
                    newcol = similar(ds1_c, promote_type(S, T), targetrows)
                    copyto!(newcol, 1, ds1_c, 1, nrows)
                    copyto!(newcol, nrows+1, ds2_c, 1, targetrows - nrows)
                    firstindex(newcol) != 1 && _onebased_check_error()
                    _columns(ds1)[j] = newcol
                end
            else
                if Missing <: eltype(ds1[!, j])
                    resize!(ds1[!, j], targetrows)
                    ds1[nrows+1:targetrows, j] .= missing
                elseif promote
                    newcol = similar(ds1[!, j], Union{Missing, eltype(ds1[!, j])},
                                     targetrows)
                    copyto!(newcol, 1, ds1[!, j], 1, nrows)
                    newcol[nrows+1:targetrows] .= missing
                    firstindex(newcol) != 1 && _onebased_check_error()
                    _columns(ds1)[j] = newcol
                else
                    throw(ArgumentError("promote=false and source data set does " *
                                        "not contain column :$n, while destination " *
                                        "column does not allow for missing values"))
                end
            end
            setformat!(ds1, n => format_of_cur_col)
            _reset_grouping_info!(ds1)
            _modified(_attributes(ds1))
        end
        current_col = 0
        for col in _columns(ds1)
            current_col += 1
            @assert length(col) == targetrows
        end
        if cols == :union
            for n in setdiff(_names(ds2), _names(ds1))
                newcol = similar(ds2[!, n], Union{Missing, eltype(ds2[!, n])},
                                 targetrows)
                @inbounds newcol[1:nrows] .= missing
                copyto!(newcol, nrows+1, ds2[!, n], 1, targetrows - nrows)
                ds1[!, n] = newcol
                _modified(_attributes(ds1))
                _reset_grouping_info!(ds1)
            end
        end
    catch err
        # Undo changes in case of error
        for col in _columns(ds1)
            resize!(col, nrows)
        end
        # go back to original modified time
        _attributes(ds1).meta.modified[] = current_modified_time
        @error "Error adding value to column :$(_names(ds1)[current_col])."
        rethrow(err)
    end
    return ds1
end

# TODO needs more works on how to handle formats??
# push! always reset the grouping information (??)
# Modify Dataset
function Base.push!(ds::Dataset, row::Union{AbstractDict, NamedTuple};
                    cols::Symbol=:setequal,
                    promote::Bool=(cols in [:union, :subset]))
    # push keep formats
    possible_cols = (:orderequal, :setequal, :intersect, :subset, :union)
    if !(cols in possible_cols)
        throw(ArgumentError("`cols` keyword argument must be any of :" *
                            join(possible_cols, ", :")))
    end

    nrows, ncols = size(ds)
    targetrows = nrows + 1
    # here the formats should be kept, setproperty! modifies time
    if ncols == 0 && row isa NamedTuple
        for (n, v) in pairs(row)
            format_of_cur_col = getformat(ds, n)
            setproperty!(ds, n, fill!(Tables.allocatecolumn(typeof(v), 1), v))
            setformat!(ds, n => format_of_cur_col)
        end
        _reset_grouping_info!(ds)
        return ds
    end

    old_row_type = typeof(row)
    if row isa AbstractDict && keytype(row) !== Symbol &&
        (keytype(row) <: AbstractString || all(x -> x isa AbstractString, keys(row)))
        row = (;(Symbol.(keys(row)) .=> values(row))...)
    end

    # in the code below we use a direct access to _columns because
    # we resize the columns so temporarily the `Dataset` is internally
    # inconsistent and normal data set indexing would error.
    if cols == :union
        current_modified = _attributes(ds).meta.modified[]
        if row isa AbstractDict && keytype(row) !== Symbol && !all(x -> x isa Symbol, keys(row))
            throw(ArgumentError("when `cols == :union` all keys of row must be Symbol"))
        end
        for (i, colname) in enumerate(_names(ds))
            format_of_cur_col = getformat(ds, colname)
            col = _columns(ds)[i]
            if haskey(row, colname)
                val = row[colname]
            else
                val = missing
            end
            S = typeof(val)
            T = eltype(col)
            if S <: T || promote_type(S, T) <: T
                push!(col, val)
            elseif !promote
                try
                    push!(col, val)
                catch err
                    setformat!(ds, colname => format_of_cur_col)
                    for col in _columns(ds)
                        resize!(col, nrows)
                    end
                    _attributes(ds).meta.modified[] = current_modified
                    @error "Error adding value to column :$colname."
                    rethrow(err)
                end
            else
                newcol = similar(col, promote_type(S, T), targetrows)
                copyto!(newcol, 1, col, 1, nrows)
                newcol[end] = val
                firstindex(newcol) != 1 && _onebased_check_error()
                _columns(ds)[i] = newcol
                setformat!(ds, colname => format_of_cur_col)
                _modified(_attributes(ds))
            end
        end
        for (colname, col) in zip(_names(ds), _columns(ds))
            if length(col) != targetrows
                for col2 in _columns(ds)
                    resize!(col2, nrows)
                end
                _attributes(ds).meta.modified[] = current_modified
                throw(AssertionError("Error adding value to column :$colname"))
            end
        end
        for colname in setdiff(keys(row), _names(ds))
            val = row[colname]
            S = typeof(val)
            if nrows == 0
                newcol = [val]
            else
                newcol = Tables.allocatecolumn(Union{Missing, S}, targetrows)
                fill!(newcol, missing)
                newcol[end] = val
            end
            ds[!, colname] = newcol
        end
        _reset_grouping_info!(ds)
        return ds
    end

    if cols == :orderequal
        if old_row_type <: Dict
            throw(ArgumentError("passing `Dict` as `row` when `cols == :orderequal` " *
                                "is not allowed as it is unordered"))
        elseif length(row) != ncol(ds) || any(x -> x[1] != x[2], zip(keys(row), _names(ds)))
            throw(ArgumentError("when `cols == :orderequal` pushed row must " *
                                "have the same column names and in the " *
                                "same order as the target data set"))
        end
    elseif cols === :setequal
        # Only check for equal lengths if :setequal is selected,
        # as an error will be thrown below if some names don't match
        if length(row) != ncols
            # an explicit error is thrown as this was allowed in the past
            throw(ArgumentError("`push!` with `cols` equal to `:setequal` " *
                                "requires `row` to have the same number of elements " *
                                "as the number of columns in `ds`."))
        end
    end
    current_col = 0
    current_modified = _attributes(ds).meta.modified[]
    try
        for (col, nm) in zip(_columns(ds), _names(ds))
            format_of_cur_col = getformat(ds, nm)
            current_col += 1
            if cols === :subset
                val = get(row, nm, missing)
            else
                val = row[nm]
            end
            S = typeof(val)
            T = eltype(col)
            if S <: T || !promote || promote_type(S, T) <: T
                push!(col, val)
            else
                newcol = similar(col, promote_type(S, T), targetrows)
                copyto!(newcol, 1, col, 1, nrows)
                newcol[end] = val
                firstindex(newcol) != 1 && _onebased_check_error()
                _columns(ds)[columnindex(ds, nm)] = newcol
                setformat!(ds, nm => format_of_cur_col)
            end
        end
        current_col = 0
        for col in _columns(ds)
            current_col += 1
            @assert length(col) == targetrows
        end
    catch err
        for col in _columns(ds)
            resize!(col, nrows)
        end
        _attributes(ds).meta.modified[] = current_modified
        @error "Error adding value to column :$(_names(ds)[current_col])."
        rethrow(err)
    end
    _reset_grouping_info!(ds)
    return ds
end

"""
    push!(ds::Dataset, row::Union{Tuple, AbstractArray}; promote::Bool=false)
    push!(ds::Dataset, row::Union{DatasetRow, NamedTuple, AbstractDict};
          cols::Symbol=:setequal, promote::Bool=(cols in [:union, :subset]))

Add in-place one row at the end of `ds` taking the values from `row`.

Column types of `ds` are preserved, and new values are converted if necessary.
An error is thrown if conversion fails.

If `row` is neither a `DatasetRow`, `NamedTuple` nor `AbstractDict` then
it must be a `Tuple` or an `AbstractArray`
and columns are matched by order of appearance. In this case `row` must contain
the same number of elements as the number of columns in `ds`.

If `row` is a `DatasetRow`, `NamedTuple` or `AbstractDict` then
values in `row` are matched to columns in `ds` based on names. The exact behavior
depends on the `cols` argument value in the following way:
* If `cols == :setequal` (this is the default)
  then `row` must contain exactly the same columns as `ds` (but possibly in a
  different order).
* If `cols == :orderequal` then `row` must contain the same columns in the same
  order (for `AbstractDict` this option requires that `keys(row)` matches
  `propertynames(ds)` to allow for support of ordered dicts; however, if `row`
  is a `Dict` an error is thrown as it is an unordered collection).
* If `cols == :intersect` then `row` may contain more columns than `ds`,
  but all column names that are present in `ds` must be present in `row` and only
  they are used to populate a new row in `ds`.
* If `cols == :subset` then `push!` behaves like for `:intersect` but if some
  column is missing in `row` then a `missing` value is pushed to `ds`.
* If `cols == :union` then columns missing in `ds` that are present in `row` are
  added to `ds` (using `missing` for existing rows) and a `missing` value is
  pushed to columns missing in `row` that are present in `ds`.

If `promote=true` and element type of a column present in `ds` does not allow
the type of a pushed argument then a new column with a promoted element type
allowing it is freshly allocated and stored in `ds`. If `promote=false` an error
is thrown.

As a special case, if `ds` has no columns and `row` is a `NamedTuple` or
`DatasetRow`, columns are created for all values in `row`, using their names
and order.

Please note that `push!` must not be used on a `Dataset` that contains columns
that are aliases (equal when compared with `===`).

# Examples
```jldoctest
julia> ds = Dataset(A=1:3, B=1:3);

julia> push!(ds, (true, false))
4×2 Dataset
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3
   4 │     1      0

julia> push!(ds, ds[1, :])
5×2 Dataset
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3
   4 │     1      0
   5 │     1      1

julia> push!(ds, (C="something", A=true, B=false), cols=:intersect)
6×2 Dataset
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3
   4 │     1      0
   5 │     1      1
   6 │     1      0

julia> push!(ds, Dict(:A=>1.0, :C=>1.0), cols=:union)
7×3 Dataset
 Row │ A        B        C
     │ Float64  Int64?   Float64?
─────┼─────────────────────────────
   1 │     1.0        1  missing
   2 │     2.0        2  missing
   3 │     3.0        3  missing
   4 │     1.0        0  missing
   5 │     1.0        1  missing
   6 │     1.0        0  missing
   7 │     1.0  missing        1.0

julia> push!(ds, NamedTuple(), cols=:subset)
8×3 Dataset
 Row │ A          B        C
     │ Float64?   Int64?   Float64?
─────┼───────────────────────────────
   1 │       1.0        1  missing
   2 │       2.0        2  missing
   3 │       3.0        3  missing
   4 │       1.0        0  missing
   5 │       1.0        1  missing
   6 │       1.0        0  missing
   7 │       1.0  missing        1.0
   8 │ missing    missing  missing
```
"""
function Base.push!(ds::Dataset, row::Any; promote::Bool=false)

# Modify Dataset
    if !(row isa Union{Tuple, AbstractArray})
        # an explicit error is thrown as this was allowed in the past
        throw(ArgumentError("`push!` does not allow passing collections of type " *
                            "$(typeof(row)) to be pushed into a Dataset. Only " *
                            "`Tuple`, `AbstractArray`, `AbstractDict`, `DatasetRow` " *
                            "and `NamedTuple` are allowed."))
    end
    nrows, ncols = size(ds)
    targetrows = nrows + 1
    if length(row) != ncols
        msg = "Length of `row` does not match `Dataset` column count."
        throw(DimensionMismatch(msg))
    end
    current_col = 0
    current_modified = _attributes(ds).meta.modified[]
    try
        for (i, (col, val)) in enumerate(zip(_columns(ds), row))
            current_col += 1
            format_of_cur_col = getformat(ds, current_col)
            S = typeof(val)
            T = eltype(col)
            if S <: T || !promote || promote_type(S, T) <: T
                push!(col, val)
            else
                newcol = Tables.allocatecolumn(promote_type(S, T), targetrows)
                copyto!(newcol, 1, col, 1, nrows)
                newcol[end] = val
                firstindex(newcol) != 1 && _onebased_check_error()
                _columns(ds)[i] = newcol
                setformat!(ds, i => format_of_cur_col)
                _modified(_attributes(ds))
            end
        end
        current_col = 0
        for col in _columns(ds)
            current_col += 1
            @assert length(col) == targetrows
        end
    catch err
        #clean up partial row
        for col in _columns(ds)
            resize!(col, nrows)
        end
        _attributes(ds).meta.modified[] = current_modified
        @error "Error adding value to column :$(_names(ds)[current_col])."
        rethrow(err)
    end
    _reset_grouping_info!(ds)
    ds
end
