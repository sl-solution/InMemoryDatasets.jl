##############################################################################
##
## setindex!()
##
##############################################################################

# Will automatically add a new column if needed

# Modify Dataset
function insert_single_column!(ds::Dataset, v::AbstractVector, col_ind::ColumnIndex)
    if ncol(ds) != 0 && nrow(ds) != length(v)
        throw(ArgumentError("New columns must have the same length as old columns"))
    end
    dv = isa(v, AbstractRange) ? collect(v) : v
    firstindex(dv) != 1 && _onebased_check_error()

    if haskey(index(ds), col_ind)
        j = index(ds)[col_ind]
        # if the modified column is with in sorting columns empty the gattributes
        # TODO we can be more clever about this
        if j ∈ index(ds).sortedcols
          _reset_grouping_info!(index(ds))
        end
        # change the type of column j
        _columns(ds)[j] = dv
        removeformat!(ds, j)
        _modified(_attributes(ds))
    else
        if col_ind isa SymbolOrString
            push!(index(ds), Symbol(col_ind))
            push!(_columns(ds), dv)
            _modified(_attributes(ds))
        else
            throw(ArgumentError("Cannot assign to non-existent column: $col_ind"))
        end
    end
    return ds
end

# Modify Dataset
function insert_single_entry!(ds::Dataset, v::Any, row_ind::Integer, col_ind::ColumnIndex)
    if haskey(index(ds), col_ind)
      # single entry doesn't remove format
        colidx = index(ds)[col_ind]
        _columns(ds)[colidx][row_ind] = v
        # if the modified column is with in sorting columns empty the gattributes
       if colidx ∈ index(ds).sortedcols
          _reset_grouping_info!(index(ds))
        end
        _modified(_attributes(ds))
        return v
    else
        throw(ArgumentError("Cannot assign to non-existent column: $col_ind"))
    end
end

# ds[!, SingleColumnIndex] = AbstractVector

# Modify Dataset
function Base.setindex!(ds::Dataset, v::AbstractVector, ::typeof(!), col_ind::ColumnIndex)
    insert_single_column!(ds, v, col_ind)
    return ds
end

# ds.col = AbstractVector
# separate methods are needed due to dispatch ambiguity

# Modify Dataset
# v must be promotable to ds[!, col_ind], thus, we can keep the format but sorting based on it should be revised
function Base.setproperty!(ds::Dataset, col_ind::Symbol, v::AbstractVector)
    insert_single_column!(ds, v, col_ind)
    return ds
end

# Modify Dataset
function Base.setproperty!(ds::Dataset, col_ind::AbstractString, v::AbstractVector)
    insert_single_column!(ds, v, col_ind)
    return ds
end

# Modify Dataset
Base.setproperty!(::Dataset, col_ind::Symbol, v::Any) =
    throw(ArgumentError("It is only allowed to pass a vector as a column of a Dataset. " *
                        "Instead use `ds[!, col_ind] .= v` if you want to use broadcasting."))

# Modify Dataset
Base.setproperty!(::Dataset, col_ind::AbstractString, v::Any) =
    throw(ArgumentError("It is only allowed to pass a vector as a column of a Dataset. " *
                        "Instead use `ds[!, col_ind] .= v` if you want to use broadcasting."))

# ds[SingleRowIndex, SingleColumnIndex] = Single Item

# Modify Dataset
function Base.setindex!(ds::Dataset, v::Any, row_ind::Integer, col_ind::ColumnIndex)
    insert_single_entry!(ds, v, row_ind, col_ind)
    return ds
end

# ds[SingleRowIndex, MultiColumnIndex] = value
# the method for value of type DatasetRow, AbstractDict and NamedTuple
# is defined in datasetrow.jl

# Modify Dataset
for T in MULTICOLUMNINDEX_TUPLE
    @eval function Base.setindex!(ds::Dataset,
                                  v::Union{Tuple, AbstractArray},
                                  row_ind::Integer,
                                  col_inds::$T)
        idxs = index(ds)[col_inds]
        if length(v) != length(idxs)
            throw(DimensionMismatch("$(length(idxs)) columns were selected but the assigned " *
                                    "collection contains $(length(v)) elements"))
        end
        for (i, x) in zip(idxs, v)
            ds[row_ind, i] = x
        end
        return ds
    end
end

# ds[MultiRowIndex, SingleColumnIndex] = AbstractVector

# Modify Dataset
for T in (:AbstractVector, :Not, :Colon)
    @eval function Base.setindex!(ds::Dataset,
                                  v::AbstractVector,
                                  row_inds::$T,
                                  col_ind::ColumnIndex)
        if row_inds isa Colon && !haskey(index(ds), col_ind)
            ds[!, col_ind] = copy(v)
            return ds
        end
        x = ds[!, col_ind].x
        x[row_inds] = v
        return ds
    end
end

# ds[MultiRowIndex, MultiColumnIndex] = AbstractDataset

# Modify Dataset
for T1 in (:AbstractVector, :Not, :Colon),
    T2 in MULTICOLUMNINDEX_TUPLE
    @eval function Base.setindex!(ds::Dataset,
                                  new_ds::AbstractDataset,
                                  row_inds::$T1,
                                  col_inds::$T2)
        idxs = index(ds)[col_inds]
        if view(_names(ds), idxs) != _names(new_ds)
            throw(ArgumentError("column names in source and target do not match"))
        end
        for (j, col) in enumerate(idxs)
            ds[row_inds, col] = new_ds[!, j].x
        end
        return ds
    end
end

# Modify Dataset
for T in MULTICOLUMNINDEX_TUPLE
    @eval function Base.setindex!(ds::Dataset,
                                  new_ds::AbstractDataset,
                                  row_inds::typeof(!),
                                  col_inds::$T)
        idxs = index(ds)[col_inds]
        if view(_names(ds), idxs) != _names(new_ds)
            throw(ArgumentError("Column names in source and target data sets do not match"))
        end
        for (j, col) in enumerate(idxs)
            # make sure we make a copy on assignment
            ds[!, col] = new_ds[:, j]
        end
        return ds
    end
end

# ds[MultiRowIndex, MultiColumnIndex] = AbstractMatrix

# Modify Dataset
for T1 in (:AbstractVector, :Not, :Colon, :(typeof(!))),
    T2 in MULTICOLUMNINDEX_TUPLE
    @eval function Base.setindex!(ds::Dataset,
                                  mx::AbstractMatrix,
                                  row_inds::$T1,
                                  col_inds::$T2)
        idxs = index(ds)[col_inds]
        if size(mx, 2) != length(idxs)
            throw(DimensionMismatch("number of selected columns ($(length(idxs))) " *
                                    "and number of columns in " *
                                    "matrix ($(size(mx, 2))) do not match"))
        end
        for (j, col) in enumerate(idxs)
            ds[row_inds, col] = (row_inds === !) ? mx[:, j] : view(mx, :, j)
        end
        return ds
    end
end


##############################################################################
##
## Mutating methods
##
##############################################################################

"""
    insertcols!(ds::Dataset[, col], (name=>val)::Pair...;
                makeunique::Bool=false, copycols::Bool=true)

Insert a column into a data set in place. Return the updated `Dataset`.
If `col` is omitted it is set to `ncol(ds)+1`
(the column is inserted as the last column).

# Arguments
- `ds` : the Dataset to which we want to add columns
- `col` : a position at which we want to insert a column, passed as an integer
  or a column name (a string or a `Symbol`); the column selected with `col`
  and columns following it are shifted to the right in `ds` after the operation
- `name` : the name of the new column
- `val` : an `AbstractVector` giving the contents of the new column or a value of any
  type other than `AbstractArray` which will be repeated to fill a new vector;
  As a particular rule a values stored in a `Ref` or a `0`-dimensional `AbstractArray`
  are unwrapped and treated in the same way.
- `makeunique` : Defines what to do if `name` already exists in `ds`;
  if it is `false` an error will be thrown; if it is `true` a new unique name will
  be generated by adding a suffix
- `copycols` : whether vectors passed as columns should be copied

If `val` is an `AbstractRange` then the result of `collect(val)` is inserted.

# Examples
```jldoctest
julia> ds = Dataset(a=1:3)
3×1 Dataset
 Row │ a
     │ Int64
─────┼───────
   1 │     1
   2 │     2
   3 │     3

julia> insertcols!(ds, 1, :b => 'a':'c')
3×2 Dataset
 Row │ b     a
     │ Char  Int64
─────┼─────────────
   1 │ a         1
   2 │ b         2
   3 │ c         3

julia> insertcols!(ds, 2, :c => 2:4, :c => 3:5, makeunique=true)
3×4 Dataset
 Row │ b     c      c_1    a
     │ Char  Int64  Int64  Int64
─────┼───────────────────────────
   1 │ a         2      3      1
   2 │ b         3      4      2
   3 │ c         4      5      3
```
"""
function insertcols!(ds::Dataset, col::ColumnIndex, name_cols::Pair{Symbol, <:Any}...;
                     makeunique::Bool=false, copycols::Bool=true)

# Modify Dataset
    col_ind = Int(col isa SymbolOrString ? columnindex(ds, col) : col)
    if !(0 < col_ind <= ncol(ds) + 1)
        throw(ArgumentError("attempt to insert a column to a data set with " *
                            "$(ncol(ds)) columns at index $col_ind"))
    end

    if !makeunique
        if !allunique(first.(name_cols))
            throw(ArgumentError("Names of columns to be inserted into a data set " *
                                "must be unique when `makeunique=true`"))
        end
        for (n, _) in name_cols
            if hasproperty(ds, n)
                throw(ArgumentError("Column $n is already present in the data set " *
                                    "which is not allowed when `makeunique=true`"))
            end
        end
    end

    if ncol(ds) == 0
        target_row_count = -1
    else
        target_row_count = nrow(ds)
    end

    for (n, v) in name_cols
        if v isa AbstractVector
            if target_row_count == -1
                target_row_count = length(v)
            elseif length(v) != target_row_count
                if target_row_count == nrow(ds)
                    throw(DimensionMismatch("length of new column $n which is " *
                                            "$(length(v)) must match the number " *
                                            "of rows in data set ($(nrow(ds)))"))
                else
                    throw(DimensionMismatch("all vectors passed to be inserted into " *
                                            "a data set must have the same length"))
                end
            end
        elseif v isa AbstractArray && ndims(v) > 1
            throw(ArgumentError("adding AbstractArray other than AbstractVector as " *
                                "a column of a data set is not allowed"))
        end
    end
    if target_row_count == -1
        target_row_count = 1
    end

    for (name, item) in name_cols
        if !(item isa AbstractVector)
            if item isa Union{AbstractArray{<:Any, 0}, Ref}
                x = item[]
                item_new = fill!(Tables.allocatecolumn(typeof(x), target_row_count), x)
            else
                @assert !(item isa AbstractArray)
                item_new = fill!(Tables.allocatecolumn(typeof(item), target_row_count), item)
            end
        elseif item isa AbstractRange
            item_new = collect(item)
        elseif copycols
            item_new = copy(item)
        else
            item_new = item
        end

        firstindex(item_new) != 1 && _onebased_check_error()

        if ncol(ds) == 0
            ds[!, name] = item_new
        else
            if hasproperty(ds, name)
                @assert makeunique
                k = 1
                while true
                    nn = Symbol("$(name)_$k")
                    if !hasproperty(ds, nn)
                        name = nn
                        break
                    end
                    k += 1
                end
            end
            # insert! modifies index, thus it should modifies gattributes
            insert!(index(ds), col_ind, name)
            insert!(_columns(ds), col_ind, item_new)
            _modified(_attributes(ds))
        end
        col_ind += 1
    end
    return ds
end

# Modify Dataset
insertcols!(ds::Dataset, col::ColumnIndex, name_cols::Pair{<:AbstractString, <:Any}...;
                     makeunique::Bool=false, copycols::Bool=true) =
    insertcols!(ds, col, (Symbol(n) => v for (n, v) in name_cols)...,
                makeunique=makeunique, copycols=copycols)

# Modify Dataset
insertcols!(ds::Dataset, name_cols::Pair{Symbol, <:Any}...;
            makeunique::Bool=false, copycols::Bool=true) =
    insertcols!(ds, ncol(ds)+1, name_cols..., makeunique=makeunique, copycols=copycols)

# Modify Dataset
insertcols!(ds::Dataset, name_cols::Pair{<:AbstractString, <:Any}...;
            makeunique::Bool=false, copycols::Bool=true) =
    insertcols!(ds, (Symbol(n) => v for (n, v) in name_cols)...,
                makeunique=makeunique, copycols=copycols)

# Modify Dataset
function insertcols!(ds::Dataset, col::Int=ncol(ds)+1; makeunique::Bool=false, name_cols...)
    if !(0 < col <= ncol(ds) + 1)
        throw(ArgumentError("attempt to insert a column to a data set with " *
                            "$(ncol(ds)) columns at index $col"))
    end
    if !isempty(name_cols)
        # an explicit error is thrown as keyword argument was supported in the past
        throw(ArgumentError("inserting colums using a keyword argument is not supported, " *
                            "pass a Pair as a positional argument instead"))
    end
    return ds
end
