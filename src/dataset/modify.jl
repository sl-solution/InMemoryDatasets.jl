struct MultiCol
    x
end

struct splitter end

# should we also define byrow as a structure?
function byrow(@nospecialize(f); @nospecialize(args...))
    br = [:($f, $args)]
    br[1].head = :BYROW
    br
end

function _check_ind_and_add!(outidx::Index, val)
    if !haskey(outidx, val)
        push!(outidx, val)
    end
end

# splitting a column to multiple columns
function normalize_modify!(outidx::Index, idx, @nospecialize(sel::Pair{<:ColumnIndex,
                                                                <:Pair{<:typeof(splitter),
                                                                    <:Vector{<:Union{Symbol, AbstractString}}}}))
    src, (fun, dst) = sel
    for i in 1:length(dst)
        _check_ind_and_add!(outidx, Symbol(dst[i]))
    end
    return outidx[src] => fun => MultiCol(Symbol.(dst))

end
function normalize_modify!(outidx::Index, idx, @nospecialize(sel::Pair{<:ColumnIndex,
                                                                <:splitter}
                                                                    ))
    throw(ArgumentError("for `splitter` the destinations must be specified"))

end

# col => fun => dst, the job is to create col => fun => :dst
function normalize_modify!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:ColumnIndex,
                                                    <:Pair{<:Union{Base.Callable},
                                                        <:Union{Symbol, AbstractString}}})
                                                        )
    src, (fun, dst) = sel
    _check_ind_and_add!(outidx, Symbol(dst))
    return outidx[src] => fun => Symbol(dst)
end

# (col1, col2) => fun => dst, the job is to create (col1, col2) => fun => :dst
function normalize_modify!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:NTuple{N, ColumnIndex},
                                                    <:Pair{<:Union{Base.Callable},
                                                        <:Union{Symbol, AbstractString}}})
                                                        ) where N
    src, (fun, dst) = sel
    N < 2 && throw(ArgumentError("For multivariate functions (Tuple of column names), the number of input columns must be greater than 1"))
    _check_ind_and_add!(outidx, Symbol(dst))
    return ntuple(i->outidx[src[i]], N) => fun => Symbol(dst)
end

# this is add to support byrow for multivariate functions
# (col1, col2) => byrow(fun) => dst, the job is to create (col1, col2) => byrow(fun) => :dst
function normalize_modify!(outidx::Index, idx,
    @nospecialize(sel::Pair{<:NTuple{N, ColumnIndex},
                            <:Pair{<:Vector{Expr},
                                <:Union{Symbol, AbstractString}}})
                                ) where N
    src = sel.first
    if sel.second.first[1].head == :BYROW
        _check_ind_and_add!(outidx, Symbol(sel.second.second))
        return ntuple(i->outidx[src[i]], length(src)) => sel.second.first[1] => Symbol(sel.second.second)
    end
    throw(ArgumentError("only byrow is accepted when using expressions"))
end
function normalize_modify!(outidx::Index, idx,
    @nospecialize(sel::Pair{<:NTuple{N, ColumnIndex},
                            <:Pair{<:Expr,
                                <:Union{Symbol, AbstractString}}})
                                ) where N
    src = sel.first
    if sel.second.first.head == :BYROW
        _check_ind_and_add!(outidx, Symbol(sel.second.second))
        return ntuple(i->outidx[src[i]], length(src)) => sel.second.first[1] => Symbol(sel.second.second)
    end
    throw(ArgumentError("only byrow is accepted when using expressions"))
end
function normalize_modify!(outidx::Index, idx,
    @nospecialize(sel::Pair{<:NTuple{N, ColumnIndex},
                            <:Vector{Expr}})
                                ) where N
    src = sel.first
    N < 2 && throw(ArgumentError("For multivariate functions (Tuple of column names), the number of input columns must be greater than 1"))
    col1, col2 = outidx[src[1]], outidx[src[2]]
    var1, var2 = _names(outidx)[col1], _names(outidx)[col2]
    if sel.second[1].head == :BYROW
        if N > 2
            nname = Symbol(funname(sel.second[1].args[1]), "_", var1, "_", var2, "_etc")
        else
            nname = Symbol(funname(sel.second[1].args[1]), "_", var1, "_", var2)
        end
        _check_ind_and_add!(outidx, nname)
        return ntuple(i->outidx[src[i]], length(src)) => sel.second[1] => nname
    end
    throw(ArgumentError("only byrow is accepted when using expressions"))
end
function normalize_modify!(outidx::Index, idx,
    @nospecialize(sel::Pair{<:NTuple{N, ColumnIndex},
                            <:Expr})
                                ) where N
    src = sel.first
    N < 2 && throw(ArgumentError("For multivariate functions (Tuple of column names), the number of input columns must be greater than 1"))
    col1, col2 = outidx[src[1]], outidx[src[2]]
    var1, var2 = _names(outidx)[col1], _names(outidx)[col2]
    if sel.second.head == :BYROW
        if N > 2
            nname = Symbol(funname(sel.second.args[1]), "_", var1, "_", var2, "_etc")
        else
            nname = Symbol(funname(sel.second.args[1]), "_", var1, "_", var2)
        end
        _check_ind_and_add!(outidx, nname)
        return ntuple(i->outidx[src[i]], length(src)) => sel.second => nname
    end
    throw(ArgumentError("only byrow is accepted when using expressions"))
end

# col => fun, the job is to create col => fun => :colname
function normalize_modify!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:ColumnIndex,
                                                    <:Union{Base.Callable}}))

    src, fun = sel
    return outidx[src] => fun => _names(outidx)[outidx[src]]
end

# (col1, col2) => fun, the job is to create (col1, col2) => fun => :colname
function normalize_modify!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:NTuple{N, ColumnIndex},
                                                    <:Union{Base.Callable}})) where N

    src, fun = sel
    N < 2 && throw(ArgumentError("For multivariate functions (Tuple of column names), the number of input columns must be greater than 1"))
    col1, col2 = outidx[src[1]], outidx[src[2]]
    var1, var2 = _names(outidx)[col1], _names(outidx)[col2]
    if N > 2
        nname = Symbol(funname(sel.second), "_", var1, "_", var2, "_etc")
    else
        nname = Symbol(funname(sel.second), "_", var1, "_", var2)
    end

    _check_ind_and_add!(outidx, nname)
    return ntuple(i->outidx[src[i]], length(src)) => fun => nname
end


# col => byrow
function normalize_modify!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:ColumnIndex,
                                                    <:Vector{Expr}}))
    colsidx = outidx[sel.first]
    if sel.second[1].head == :BYROW
        # TODO needs a better name for destination
        # _check_ind_and_add!(outidx, Symbol("row_", funname(sel.second.args[1])))
        return outidx[colsidx] => sel.second[1] => _names(outidx)[colsidx]
    end
    throw(ArgumentError("only byrow is accepted when using expressions"))
end
function normalize_modify!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:ColumnIndex,
                                                    <:Expr}))
    colsidx = outidx[sel.first]
    if sel.second.head == :BYROW
        # TODO needs a better name for destination
        # _check_ind_and_add!(outidx, Symbol("row_", funname(sel.second.args[1])))
        return outidx[colsidx] => sel.second => _names(outidx)[colsidx]
    end
    throw(ArgumentError("only byrow is accepted when using expressions"))
end
# col => byrow => dst
function normalize_modify!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:ColumnIndex,
                                        <:Pair{<:Vector{Expr},
                                            <:Union{Symbol, AbstractString}}}))
    colsidx = outidx[sel.first]
    if sel.second.first[1].head == :BYROW
        # TODO needs a better name for destination
        _check_ind_and_add!(outidx, Symbol(sel.second.second))
        return outidx[colsidx] => sel.second.first[1] => Symbol(sel.second.second)
    end
    throw(ArgumentError("only byrow is accepted when using expressions"))
end
function normalize_modify!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:ColumnIndex,
                                        <:Pair{<:Expr,
                                            <:Union{Symbol, AbstractString}}}))
    colsidx = outidx[sel.first]
    if sel.second.first.head == :BYROW
        # TODO needs a better name for destination
        _check_ind_and_add!(outidx, Symbol(sel.second.second))
        return outidx[colsidx] => sel.second.first => Symbol(sel.second.second)
    end
    throw(ArgumentError("only byrow is accepted when using expressions"))
end

# cols => fun, the job is to create [col1 => fun => :col1name, col2 => fun => :col2name ...]
function normalize_modify!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:MultiColumnIndex,
                                                    <:Vector{Expr}}))
    colsidx = outidx[sel.first]
    if sel.second isa AbstractVector && sel.second[1] isa Expr
        if sel.second[1].head == :BYROW
            # TODO needs a better name for destination
            _check_ind_and_add!(outidx, Symbol("row_", funname(sel.second[1].args[1])))
            return outidx[colsidx] => sel.second[1] => Symbol("row_", funname(sel.second[1].args[1]))
        end
    end
    # res = Any[normalize_modify!(outidx, idx, colsidx[1] => sel.second)]
    # for i in 2:length(colsidx)
    #     push!(res, normalize_modify!(outidx, idx, colsidx[i] => sel.second))
    # end
    # return res
end
function normalize_modify!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:MultiColumnIndex,
                                                    <:Union{Base.Callable, Expr}}))
    colsidx = outidx[sel.first]
    if sel.second isa Expr
        if sel.second.head == :BYROW
            # TODO needs a better name for destination
            _check_ind_and_add!(outidx, Symbol("row_", funname(sel.second.args[1])))
            return outidx[colsidx] => sel.second => Symbol("row_", funname(sel.second.args[1]))
        end
    end
    res = Any[normalize_modify!(outidx, idx, colsidx[1] => sel.second)]
    for i in 2:length(colsidx)
        push!(res, normalize_modify!(outidx, idx, colsidx[i] => sel.second))
    end
    return res
end
# cols => funs which will be normalize as col1=>fun1, col2=>fun2, ...
function normalize_modify!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:MultiColumnIndex,
                                                    <:Vector{<:Base.Callable}}))
    colsidx = outidx[sel.first]
    if !(length(colsidx) == length(sel.second))
        throw(ArgumentError("The input number of columns and the length of the number of functions should match"))
    end
    res = Any[normalize_modify!(outidx, idx, colsidx[1] => sel.second[1])]
    for i in 2:length(colsidx)
        push!(res, normalize_modify!(outidx, idx, colsidx[i] => sel.second[i]))
    end
    return res
end

function normalize_modify!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:ColumnIndex,
                                                    <:Vector{<:Base.Callable}}))
    colsidx = outidx[sel.first]
    normalize_modify!(outidx, idx, colsidx .=> sel.second[i])
    return res
end

# special case cols => byrow(...) => :name
function normalize_modify!(outidx::Index, idx,
    @nospecialize(sel::Pair{<:MultiColumnIndex,
                            <:Pair{<:Vector{Expr},
                                <:Union{Symbol, AbstractString}}}))
    colsidx = outidx[sel.first]
    if sel.second.first[1].head == :BYROW
        _check_ind_and_add!(outidx, Symbol(sel.second.second))
        return outidx[colsidx] => sel.second.first[1] => Symbol(sel.second.second)
    else
        throw(ArgumentError("only byrow operation is supported for cols => fun => :name"))
    end
end
function normalize_modify!(outidx::Index, idx,
    @nospecialize(sel::Pair{<:MultiColumnIndex,
                            <:Pair{<:Expr,
                                <:Union{Symbol, AbstractString}}}))
    colsidx = outidx[sel.first]
    if sel.second.first.head == :BYROW
        _check_ind_and_add!(outidx, Symbol(sel.second.second))
        return outidx[colsidx] => sel.second.first => Symbol(sel.second.second)
    else
        throw(ArgumentError("only byrow operation is supported for cols => fun => :name"))
    end
end

# cols .=> fun .=> dsts, the job is to create col1 => fun => :dst1, col2 => fun => :dst2, ...
function normalize_modify!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:MultiColumnIndex,
                                                    <:Pair{<:Union{Base.Callable,Vector{Expr}},
                                                        <:AbstractVector{<:Union{Symbol, AbstractString}}}}))
    colsidx = outidx[sel.first]
    if !(length(colsidx) == length(sel.second.second))
        throw(ArgumentError("The input number of columns and the length of the output names should match"))
    end
    res = Any[normalize_modify!(outidx, idx, colsidx[1] => sel.second.first => sel.second.second[1])]
    for i in 2:length(colsidx)
        push!(res, normalize_modify!(outidx, idx, colsidx[i] => sel.second.first => sel.second.second[i]))
    end
    return res
end
# cols .=> fun .=> dsts, the job is to create col1 => fun => :dst1, col2 => fun => :dst2, ...
function normalize_modify!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:MultiColumnIndex,
                                                    <:Pair{<:Expr,
                                                        <:AbstractVector{<:Union{Symbol, AbstractString}}}}))
    colsidx = outidx[sel.first]
    if !(length(colsidx) == length(sel.second.second))
        throw(ArgumentError("The input number of columns and the length of the output names should match"))
    end
    res = Any[normalize_modify!(outidx, idx, colsidx[1] => sel.second.first => sel.second.second)]
    for i in 2:length(colsidx)
        push!(res, normalize_modify!(outidx, idx, colsidx[i] => sel.second.first => sel.second.second[i]))
    end
    return res
end

function normalize_modify!(outidx::Index, idx, arg::AbstractVector)
    res = Any[]
    for i in 1:length(arg)
        _res = normalize_modify!(outidx::Index, idx, arg[i])
        if _res isa AbstractVector
            for j in 1:length(_res)
                push!(res, _res[j])
            end
        else
            push!(res, _res)
        end
    end
    return res
end

function normalize_modify_multiple!(outidx::Index, idx, @nospecialize(args...))
    res = Any[]
    for i in 1:length(args)
        _res = normalize_modify!(outidx, idx, args[i])
        if typeof(_res) <: Pair
            push!(res, _res)
        else
            for j in 1:length(_res)
                push!(res, _res[j])
            end
        end
    end
    res
end

"""
    modify(...)

A variant of `modify!` which modifies a copy of the passed data set.

See [`modify!`](@ref)
"""
modify(origninal_ds::AbstractDataset, @nospecialize(args...); threads::Bool = true) = modify!(copy(origninal_ds), args...; threads = threads)

modify!(ds::Dataset; threads::Bool = true) = parent(ds)

"""
    modify!(ds::AbstractDataset, args...; [threads = true])

Modify columns of a data set. The `args` arguments must be in the form of `cols => fun => newnames`. The `fun` function will be called on passed `cols`, with the excpetion of two special functions: `byrow` and `splitter`. `fun` assumes a single column as its input, thus passing multiple columns will be broadcasted, i.e. `cols => fun` will be translated to `col1=>fun`, `col2=>fun`, .... When `newname` is not provided `modify!` modifies the passed column.

When a grouped data set is passed to `modify!`, the operation is done on each group of observations.

each `args` can be constructed based on columns in the original data set or the columns which have been created before it.

# Special functions

`byrow` and `splitter` are two special functions which can be passed as `fun`.

`byrow` can accept multiple columns as input and does a given operation on each row of the data set. When a single column is passed to `byrow`, `modify!` modifies the passed column, however, when multiple columns are passed, `byrow` applies the row-wise operation on them and creates a new column.

`splitter` splits a column of tuples to multiple columns. When `splitter` is set as `fun` the `newnames` must be given.

# Using multivariate functions

To pass multiple columns to a `fun` function which operates on multiple inputs, the columns must be passed as tuple of column names, or column indices.

See [`modify`](@ref)

# Examples

```jldoctest
julia> ds = Dataset(x1 = 1:5, x2 = [-2, -1, missing, 1, 2],
                    x3 = [0.0, 0.1, 0.2, missing, 0.4])
5×3 Dataset
 Row │ x1        x2        x3
     │ identity  identity  identity
     │ Int64?    Int64?    Float64?
─────┼──────────────────────────────
   1 │        1        -2       0.0
   2 │        2        -1       0.1
   3 │        3   missing       0.2
   4 │        4         1   missing
   5 │        5         2       0.4

julia> modify!(ds, 2:3 => IMD.sum)
5×3 Dataset
 Row │ x1        x2        x3
     │ identity  identity  identity
     │ Int64?    Int64?    Float64?
─────┼──────────────────────────────
   1 │        1         0       0.7
   2 │        2         0       0.7
   3 │        3         0       0.7
   4 │        4         0       0.7
   5 │        5         0       0.7

julia> modify!(ds, :x1 => x -> x .- mean(x))
5×3 Dataset
 Row │ x1        x2        x3
     │ identity  identity  identity
     │ Float64?  Int64?    Float64?
─────┼──────────────────────────────
   1 │     -2.0         0       0.7
   2 │     -1.0         0       0.7
   3 │      0.0         0       0.7
   4 │      1.0         0       0.7
   5 │      2.0         0       0.7

julia> body = Dataset(weight = [78.5, 59, 80], height = [160, 171, 183])
3×2 Dataset
 Row │ weight    height
     │ identity  identity
     │ Float64?  Int64?
─────┼────────────────────
   1 │     78.5       160
   2 │     59.0       171
   3 │     80.0       183

julia> modify!(body, :height => byrow(x -> (x/100)^2) => :BMI, [1, 3] => byrow(/) => :BMI)
3×3 Dataset
 Row │ weight    height    BMI
     │ identity  identity  identity
     │ Float64?  Int64?    Float64?
─────┼──────────────────────────────
   1 │     78.5       160   30.6641
   2 │     59.0       171   20.1771
   3 │     80.0       183   23.8884

julia> body = Dataset(weight = [78.5, 59, 80], height = [160, 171, 183])
3×2 Dataset
 Row │ weight    height
     │ identity  identity
     │ Float64?  Int64?
─────┼────────────────────
   1 │     78.5       160
   2 │     59.0       171
   3 │     80.0       183

julia> modify!(body, (:weight, :height)=> cor)
3×3 Dataset
 Row │ weight    height    cor_weight_height
     │ identity  identity  identity
     │ Float64?  Int64?    Float64?
─────┼───────────────────────────────────────
   1 │     78.5       160          0.0890411
   2 │     59.0       171          0.0890411
   3 │     80.0       183          0.0890411
```
"""
function modify!(ds::AbstractDataset, @nospecialize(args...); threads::Bool = true)
    if ds isa SubDataset
		idx_cpy = copy(index(parent(ds)))
	else
        idx_cpy = Index(copy(index(ds).lookup), copy(index(ds).names), copy(index(ds).format))
    end
    if isgrouped(ds)
        norm_var = normalize_modify_multiple!(idx_cpy, index(ds), args...)
        allnewvars = map(x -> x.second.second, norm_var)
        all_new_var = Symbol[]
        for i in 1:length(allnewvars)
            if typeof(allnewvars[i]) <: MultiCol
                for j in 1:length(allnewvars[i].x)
                    push!(all_new_var, allnewvars[i].x[j])
                end
            else
                push!(all_new_var, allnewvars[i])
            end
        end
        var_index = idx_cpy[unique(all_new_var)]
        any(index(ds).sortedcols .∈ Ref(var_index)) && throw(ArgumentError("the grouping variables cannot be modified, first use `ungroup!(ds)` to ungroup the data set"))
        _modify_grouped(ds, norm_var, threads)
    else
        _modify(ds, normalize_modify_multiple!(idx_cpy, index(ds), args...))
    end
end

# we must take care of all possible types, because, fallback is slow
# _is_scalar(::T, sz) where T <: Number = true
# _is_scalar(::Missing, sz) = true
# _is_scalar(::T, sz) where T <: Tuple = true
# _is_scalar(::TimeType, sz) = true
# _is_scalar(::T, sz) where T <: AbstractString = true
_is_scalar(x, sz) = true
_is_scalar(x::T, sz) where T <: AbstractVector = length(x) != sz

# # TODO can we memorise this and avoid calling it repeatedly in a sesssion
# _is_scalar_barrier(::Val{T}) where T = hasmethod(size, (T,))
#
# function _is_scalar(_res::T, sz) where T
#      resize_col = false
#     if _is_scalar_barrier(Val(T))
#         if size(_res) == () || size(_res,1) != sz
#             resize_col = true
#         end
#     else
#         resize_col = true
#     end
#     return resize_col
# end

function _resize_result!(ds, _res, newcol)
    resize_col = _is_scalar(_res, nrow(ds))
    if resize_col
        if ds isa SubDataset
            if haskey(index(ds), newcol)
                ds[:, newcol] = fill!(allocatecol(typeof(_res), nrow(ds)), _res)
            elseif !haskey(index(parent(ds)), newcol)
                parent(ds)[!, newcol] = _missings(typeof(_res), nrow(parent(ds)))
                _update_subindex!(index(ds), index(parent(ds)), newcol)
                ds[:, newcol] = fill!(allocatecol(typeof(_res), nrow(ds)), _res)
            else
                throw(ArgumentError("modifing a parent's column which doesn't appear in SubDataset is not allowed"))
            end
        else
            ds[!, newcol] = fill!(allocatecol(typeof(_res), nrow(ds)), _res)
        end
    else
        if ds isa SubDataset
            if haskey(index(ds), newcol)
                ds[:, newcol] = _res
            elseif !haskey(index(parent(ds)), newcol)
                parent(ds)[!, newcol] = _missings(eltype(_res), nrow(parent(ds)))
                _update_subindex!(index(ds), index(parent(ds)), newcol)
                ds[:, newcol] = _res
            else
                throw(ArgumentError("modifing a parent's column which doesn't appear in SubDataset is not allowed"))
            end
        else
            ds[!, newcol] = _res
        end
    end
end


function _modify_single_var!(ds, _f, x, dst)
    _res = _f(x)
    _resize_result!(ds, _res, dst)
end
function _modify_single_tuple_var!(ds, _f, x, dst)
    _res = _f(x...)
    _resize_result!(ds, _res, dst)
end

# the number of destination can be smaller or greater than the number of elements of Tuple,
function _modify_multiple_out!(ds, x, dst)
    !(nonmissingtype(eltype(x)) <: Tuple) && throw(ArgumentError("to use `splitter`, the source column must be a vector of Tuple"))
    tb = Tables.columntable(x)
    for j in 1:length(dst)
        try
            _resize_result!(ds, Tables.getcolumn(tb, j), dst[j])
        catch
            _resize_result!(ds, _missings(nrow(ds)), dst[j])
        end
    end
end

function _modify_f_barrier(ds, msfirst, mssecond, mslast)
    if (mssecond isa Base.Callable) && !(mslast isa MultiCol)
        if msfirst isa NTuple
            _modify_single_tuple_var!(ds, mssecond, ntuple(i -> _columns(ds)[msfirst[i]], length(msfirst)), mslast)
        else
            _modify_single_var!(ds, mssecond, _columns(ds)[msfirst], mslast)
        end
    elseif (mssecond isa Expr) && mssecond.head == :BYROW
        try
            if ds isa SubDataset
                _res = byrow(ds, mssecond.args[1], msfirst; mssecond.args[2]...)
                if haskey(index(ds), mslast)
                    ds[:, mslast] = _res
                elseif !haskey(index(parent(ds)), mslast)
                    parent(ds)[!, mslast] = _missings(eltype(_res), nrow(parent(ds)))
                    _update_subindex!(index(ds), index(parent(ds)), mslast)
                    ds[:, mslast] = _res
                else
                    throw(ArgumentError("modifing a parent's column which doesn't appear in SubDataset is not allowed"))
                end
            else
                ds[!, mslast] = byrow(ds, mssecond.args[1], msfirst; mssecond.args[2]...)
            end
        catch e
            if e isa MethodError
                throw(ArgumentError("There might be a problem in the `byrow` usage, make sure that the output of `byrow` is a vector"))
            end
            rethrow(e)
        end
    elseif  (mssecond isa Base.Callable) && (mslast isa MultiCol) && (mssecond isa typeof(splitter))
        _modify_multiple_out!(ds, _columns(ds)[msfirst], mslast.x)
    else
        @error "not yet know how to handle this situation $(msfirst => mssecond => mslast)"
    end
end

function _modify(ds, ms)
    needs_reset_grouping = false
    for i in 1:length(ms)
        _modify_f_barrier(ds, ms[i].first, ms[i].second.first, ms[i].second.second)
    end
    return ds
end

function _check_the_output_type(ds::Dataset, ms)
    if ms.first isa Tuple
        CT = return_type(ms.second.first, ntuple(i -> _columns(ds)[ms.first[i]], length(ms.first)))
    else
        CT = return_type(ms.second.first, _columns(ds)[ms.first])
    end
    # TODO check other possibilities:
    # the result can be
    # * AbstractVector{T} where T
    # * Vector{T}
    # * not a Vector
    CT == Union{} && throw(ArgumentError("compiler cannot assess the return type of calling `$(ms.second.first)` on `:$(_names(ds)[ms.first])`, you may want to try using `byrow`"))
    if CT <: AbstractVector
        if hasproperty(CT, :var)
            T = Union{Missing, CT.var.ub}
        else
            T = Union{Missing, eltype(CT)}
        end
    else
        T = Union{Missing, CT}
    end
    T
end

# FIXME notyet complete
# fill _res for grouped data: col => f => :newcol
function _modify_grouped_fill_one_col!(_res, x, _f, starts, ngroups, nrows, threads)
    @_threadsfor threads for g in 1:ngroups
        lo = starts[g]
        g == ngroups ? hi = nrows : hi = starts[g + 1] - 1
        _tmp_res = _f(view(x, lo:hi))
        resize_col = _is_scalar(_tmp_res, length(lo:hi))
        if resize_col
            fill!(view(_res, lo:hi), _tmp_res)
        else
            copy!(view(_res, lo:hi), _tmp_res)
        end
    end
    _res
end

function _modify_grouped_fill_one_col_tuple!(_res, x, _f, starts, ngroups, nrows, threads)
    @_threadsfor threads for g in 1:ngroups
        lo = starts[g]
        g == ngroups ? hi = nrows : hi = starts[g + 1] - 1
        _tmp_res = do_call(_f, x, lo:hi)
        resize_col = _is_scalar(_tmp_res, length(lo:hi))
        if resize_col
            fill!(view(_res, lo:hi), _tmp_res)
        else
            copy!(view(_res, lo:hi), _tmp_res)
        end
    end
    _res
end


function _modify_grouped_f_barrier(ds, msfirst, mssecond, mslast, threads)
    if (mssecond isa Base.Callable) && !(mslast isa MultiCol)
        T = _check_the_output_type(ds, msfirst=>mssecond=>mslast)
        _res = allocatecol(T, nrow(ds))
        if msfirst isa Tuple
            _modify_grouped_fill_one_col_tuple!(_res, ntuple(i->_columns(ds)[msfirst[i]], length(msfirst)), mssecond, index(ds).starts, index(ds).ngroups[], nrow(ds), threads)
        else
            _modify_grouped_fill_one_col!(_res, _columns(ds)[msfirst], mssecond, index(ds).starts, index(ds).ngroups[], nrow(ds), threads)
        end
        ds[!, mslast] = _res
    elseif (mssecond isa Expr)  && mssecond.head == :BYROW
        #TODO we should think about how to pass threads here
        ds[!, mslast] = byrow(ds, mssecond.args[1], msfirst; mssecond.args[2]...)
    elseif (mssecond isa Base.Callable) && (mslast isa MultiCol) && (mssecond isa typeof(splitter))
        _modify_multiple_out!(ds, _columns(ds)[msfirst], mslast.x)
    else
                # if something ends here, we should implement new functionality for it
        @error "not yet know how to handle the situation $(msfirst => mssecond => mslast)"
    end
end

function _modify_grouped(ds, ms, threads)
    needs_reset_grouping = false
    for i in 1:length(ms)
        _modify_grouped_f_barrier(ds, ms[i].first, ms[i].second.first, ms[i].second.second, threads)
    end
    return parent(ds)
end
