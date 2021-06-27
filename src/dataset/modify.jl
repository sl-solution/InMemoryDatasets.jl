struct MultiCol
    x
end
function byrow(@nospecialize(f); @nospecialize(args...))
    br = :($f, $args)
    br.head = :BYROW
    br
end

function _check_ind_and_add!(outidx::Index, val)
    if !haskey(outidx, val)
        push!(outidx, val)
    end
end

# col => fun => dst, the job is to create col => fun => :dst
function normalize_modify!(outidx::Index, idx::Index,
                            @nospecialize(sel::Pair{<:ColumnIndex,
                                                    <:Pair{<:Union{Base.Callable},
                                                        <:Union{Symbol, AbstractString}}})
                                                        )
    src, (fun, dst) = sel
    _check_ind_and_add!(outidx, Symbol(dst))
    return outidx[src] => fun => Symbol(dst)
end
# col => fun => dst, the job is to create col => fun => :dst
function normalize_modify!(outidx::Index, idx::Index,
                            @nospecialize(sel::Pair{<:ColumnIndex,
                                                    <:Pair{<:Union{Base.Callable},
                                                        <:Vector{<:Union{Symbol, AbstractString}}}})
                                                        )
    src, (fun, dst) = sel
    for i in 1:length(dst)
        _check_ind_and_add!(outidx, Symbol(dst[i]))
    end
    return outidx[src] => fun => MultiCol(Symbol.(dst))
end
# col => fun, the job is to create col => fun => :colname
function normalize_modify!(outidx::Index, idx::Index,
                            @nospecialize(sel::Pair{<:ColumnIndex,
                                                    <:Union{Base.Callable}}))

    src, fun = sel
    return outidx[src] => fun => _names(outidx)[outidx[src]]
end

# col => byrow
function normalize_modify!(outidx::Index, idx::Index,
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
function normalize_modify!(outidx::Index, idx::Index,
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
function normalize_modify!(outidx::Index, idx::Index,
                            @nospecialize(sel::Pair{<:MultiColumnIndex,
                                                    <:Union{Base.Callable,Expr}}))
    colsidx = outidx[sel.first]
    if sel.second isa Expr
        if sel.second.head == :BYROW
            # TODO needs a better name for destination
            _check_ind_and_add!(outidx, Symbol("row_", funname(sel.second.args[1])))
            return outidx[colsidx] => sel.second => Symbol("row_", funname(sel.second.args[1]))
        end
    end
    res = [normalize_modify!(outidx, idx, colsidx[1] => sel.second)]
    for i in 2:length(colsidx)
        push!(res, normalize_modify!(outidx, idx, colsidx[i] => sel.second))
    end
    return res
end

# special case cols => byrow(...) => :name
function normalize_modify!(outidx::Index, idx::Index,
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
function normalize_modify!(outidx::Index, idx::Index,
                            @nospecialize(sel::Pair{<:MultiColumnIndex,
                                                    <:Pair{<:Union{Base.Callable,Expr},
                                                        <:AbstractVector{<:Union{Symbol, AbstractString}}}}))
    colsidx = outidx[sel.first]
    if !(length(colsidx) == length(sel.second.second))
        throw(ArgumentError("The input number of columns and the length of the output names should match"))
    end
    res = [normalize_modify!(outidx, idx, colsidx[1] => sel.second.first => sel.second.second[1])]
    for i in 2:length(colsidx)
        push!(res, normalize_modify!(outidx, idx, colsidx[i] => sel.second.first => sel.second.second[i]))
    end
    return res
end

function normalize_modify_multiple!(outidx::Index, idx::Index, @nospecialize(args...))
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

modify(ds::Dataset) = copy(ds)
function modify(origninal_ds::Dataset, @nospecialize(args...))
    ds = copy(origninal_ds)
    idx_cpy::Index = Index(copy(index(ds).lookup), copy(index(ds).names), Dict{Int, Function}())
    if isgrouped(ds)
        norm_var = normalize_modify_multiple!(idx_cpy, index(ds), args...)
        all_new_var = map(x -> x.second.second, norm_var)
        var_index = idx_cpy[unique(all_new_var)]
        if any(index(ds).sortedcols .∈ Ref(var_index))
            throw(ArgumentError("the grouping variables cannot be modified, first use `ungroup!(ds)` to ungroup the data set"))
        end
        _modify_grouped(ds, norm_var)
    else
        _modify(ds, normalize_modify_multiple!(idx_cpy, index(ds), args...))
    end
end
modify!(ds::Dataset) = ds
function modify!(ds::Dataset, @nospecialize(args...))
    idx_cpy = Inde(copy(index(ds).lookup), copy(index(ds).names), copy(index(ds).format))
    if isgrouped(ds)
        norm_var = normalize_modify_multiple!(idx_cpy, index(ds), args...)
        all_new_var = map(x -> x.second.second, norm_var)
        var_index = idx_cpy[unique(all_new_var)]
        index(ds).sortedcols .∈ Ref(var_index) && throw(ArgumentError("the grouping variables cannot be modified, first use `ungroup!(ds)` to ungroup the data set"))
        _modify_grouped(ds, normalize_modify_multiple!(idx_cpy, index(ds), args...))
    else
        _modify(ds, normalize_modify_multiple!(idx_cpy, index(ds), args...))
    end
end

# size() is better option for checking if the result is scalar,
# it works for numbers and it won't work for strings and symbols
function _is_scalar(_res, sz)
     resize_col = false
    try
        size(_res)
        if size(_res) == () || size(_res,1) != sz
            # fill!(Tables.allocatecolumn(typeof(_res), nrow(ds)),
            #                           _res)
            # _res = repeat([_res], nrow(ds))
            resize_col = true
        end
    catch e
        if (e isa MethodError)
            # fill!(Tables.allocatecolumn(typeof(_res), nrow(ds)),
                                      # _res)
            # _res = repeat([_res], nrow(ds))
            resize_col = true
       end

    end
    return resize_col
end

function _resize_result!(ds, _res, newcol)
    resize_col = _is_scalar(_res, nrow(ds))
    if resize_col
        ds[!, newcol] = fill!(Tables.allocatecolumn(typeof(_res), nrow(ds)), _res)
    else
        ds[!, newcol] = _res
    end
end


function _modify(ds, ms)
    needs_reset_grouping = false
    for i in 1:length(ms)
        if (ms[i].second.first isa Base.Callable) && !(ms[i].second.second isa MultiCol)
            _res = ms[i].second.first(_columns(ds)[ms[i].first])
            _resize_result!(ds, _res, ms[i].second.second)
        elseif (ms[i].second.first isa Expr) && ms[i].second.first.head == :BYROW
            try
                ds[!, ms[i].second.second] = byrow(ds, ms[i].second.first.args[1], ms[i].first; ms[i].second.first.args[2]...)
            catch e
                if e isa MethodError
                    throw(ArgumentError("output of `byrow` operation must be a vector"))
                end
                rethrow(e)
            end
        elseif (ms[i].second.first isa Base.Callable) && (ms[i].second.second isa MultiCol)
            _res = ms[i].second.first(_columns(ds)[ms[i].first])
            if _res isa Tuple
                for j in 1:length(ms[i].second.second.x)
                    _resize_result!(ds, _res[j], ms[i].second.second.x[j])
                end
            else
                throw(ArgumentError("the function must return results as a tuple which each element of it corresponds to a new column"))
            end
        else
            @error "not yet know how to handle this situation $(ms[i])"
        end
    end
    return ds
end

# Checking the output type
# it uses two observations to determine the output type
# if the data structure is complicated or the function in ms ref to index beyond 2 it won't work
# function _check_the_output_type(ds, ms)
#     try
#         mingsize = 2
#         _tmpval = similar(_columns(ds)[ms.first], mingsize)
#         _first_nonmiss_val = _first_nonmiss(_columns(ds)[ms.first])
#         for i in 1:mingsize
#             _tmpval[i] = _first_nonmiss_val
#         end
#         _tmpval_fun = ms.second.first(_tmpval)
#         notvector = _is_scalar(_tmpval_fun, 2)
#         if notvector
#             T = typeof(_tmpval_fun)
#         else
#             T = eltype(_tmpval_fun)
#         end
#         if eltype(ds[!, ms.first]) >: Missing
#             for i in 1:mingsize
#                 _tmpval[i] = missing
#             end
#             if notvector
#                 T = Union{T, typeof(ms.second.first(_tmpval))}
#             else
#                 T = Union{T, eltype(ms.second.first(_tmpval))}
#             end
#         end
#         return T
#     catch
#         throw(ArgumentError("modify only uses two observations to assess the output type, if your modification function uses index beyond 2, modify it to be more generic, or try to apply the function row-wise, otherwise the output type is not predictable for the input data type"))
#     end
# end

function _check_the_output_type(ds::Dataset, ms)
    CT = return_type(ms.second.first, (typeof(ds[!, ms.first].val),))
    # TODO check other possibilities:
    # the result can be
    # * AbstractVector{T} where T
    # * Vector{T}
    # * not a Vector
    CT == Union{} && throw(ArgumentError("compiler cannot assess the return type of calling `$(ms.second.first)` on `:$(_names(ds)[ms.first])`, you may want to try using `byrow`"))
    if CT <: AbstractVector
        if hasproperty(CT, :var)
            T = CT.var.ub
        else
            T = eltype(CT)
        end
    else
        T = CT
    end
    T
end

# FIXME notyet complete
# fill _res for grouped data: col => f => :newcol
function _modify_grouped_fill_one_col!(_res, x, ms, starts, ngroups, nrows)
    Threads.@threads for g in 1:ngroups
        lo = starts[g]
        g == ngroups ? hi = nrows : hi = starts[g + 1] - 1
        _tmp_res = ms.second.first(view(x, lo:hi))
        resize_col = _is_scalar(_tmp_res, length(lo:hi))
        if resize_col
            fill!(view(_res, lo:hi), _tmp_res)
        else
            copy!(view(_res, lo:hi), _tmp_res)
        end
    end
    _res
end


function _modify_grouped(ds, ms)
    needs_reset_grouping = false
    for i in 1:length(ms)
            if (ms[i].second.first isa Base.Callable) && !(ms[i].second.second isa MultiCol)
                T = _check_the_output_type(ds, ms[i])
                _res = Tables.allocatecolumn(T, nrow(ds))
                _modify_grouped_fill_one_col!(_res, _columns(ds)[ms[i].first], ms[i], index(ds).starts, index(ds).ngroups[], nrow(ds))
                # Threads.@threads for g in 1:index(ds).ngroups[]
                #     lo = index(ds).starts[g]
                #     g == index(ds).ngroups[] ? hi = nrow(ds) : hi = index(ds).starts[g + 1] - 1
                #     _tmp_res = ms[i].second.first(view(_columns(ds)[ms[i].first], lo:hi))
                #     resize_col = _is_scalar(_tmp_res, length(lo:hi))
                #     if resize_col
                #         fill!(view(_res, lo:hi), _tmp_res)
                #     else
                #         copy!(view(_res, lo:hi), _tmp_res)
                #     end
                # end
                ds[!, ms[i].second.second] = _res
            elseif (ms[i].second.first isa Expr) && ms[i].second.first.head == :BYROW
                ds[!, ms[i].second.second] = byrow(ds, ms[i].second.first.args[1], ms[i].first; ms[i].second.first.args[2]...)
            elseif (ms[i].second.first isa Base.Callable) && (ms[i].second.second isa MultiCol)
                # _res = _allocate_for_groups(ms[i].second.first, coalesce(_columns(ds)[ms[i].first]), eltype(ds[!, ms[i].first]), nrow(ds))
                # if _res isa Tuple
                #     for g in 1:index(ds).ngroups[]
                #         lo = index(ds).starts[g]
                #         g == index(ds).ngroups[] ? hi = nrow(ds) : hi = index(ds).starts[g + 1] - 1
                #         for j in 1:length(ms[i].second.second.x)
                #             copy!(view(_res, lo:hi), ms[i].second.first(view(_columns(ds)[ms[i].first], lo:hi)))
                #         end
                #             # _resize_result!(ds, _res[j], ms[i].second.second.x[j])
                #     end
                # else
                #     throw(ArgumentError("the function must return results as a tuple which each element of it corresponds to a new column"))
                # end
                throw(ArgumentError("multi column output is not supported for grouped data set"))
            else
                # if something ends here, we should implement new functionality for it
                @error "not yet know how to handle the situation $(ms[i])"
            end
    end
    return ds
end
