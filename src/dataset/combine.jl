#  we assume
# * outidx already has grouping columns
# * any fun except byrow will get input from input data set, and byrow gets input from the output data set.
# * except Tuple every other column selector will assume funcions are univariate (byrow is exception)


# col => fun => dst, the job is to create col => fun => :dst
function normalize_combine!(outidx::Index, idx,
    @nospecialize(sel::Pair{<:ColumnIndex,
                            <:Pair{<:Union{Function},
                                <:Union{Symbol, AbstractString}}})
                                )
    src, (fun, dst) = sel
    _check_ind_and_add!(outidx, Symbol(dst))
    return _names(idx)[idx[src]] => fun => Symbol(dst)
end

# Tuple => fun => dst, the job is to create Tuple => fun => :dst
function normalize_combine!(outidx::Index, idx,
    @nospecialize(sel::Pair{<:NTuple{N, ColumnIndex},
                            <:Pair{<:Union{Function},
                                <:Union{Symbol, AbstractString}}})
                                ) where N
    src, (fun, dst) = sel
    N < 2 && throw(ArgumentError("For multivariate functions (Tuple of column names), the number of input columns must be greater than 1"))
    _check_ind_and_add!(outidx, Symbol(dst))
    return ntuple(i -> _names(idx)[idx[src[i]]], N) => fun => Symbol(dst)
end

# this is add to support byrow for multivariate functions
# (col1, col2) => byrow(fun) => dst, the job is to create (col1, col2) => byrow(fun) => :dst
function normalize_combine!(outidx::Index, idx,
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
function normalize_combine!(outidx::Index, idx,
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
function normalize_combine!(outidx::Index, idx,
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
function normalize_combine!(outidx::Index, idx,
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
function normalize_combine!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:ColumnIndex,
                                <:Union{Function}}))

    src, fun = sel
    nname = Symbol(funname(fun), "_", _names(idx)[idx[src]])
    _check_ind_and_add!(outidx, nname)
    return _names(idx)[idx[src]] => fun => nname
end

# Tuple => fun  normalise as Tuple => fun => :genname
function normalize_combine!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:NTuple{N, ColumnIndex},
                                <:Union{Function}})) where N

    src, fun = sel
    N < 2 && throw(ArgumentError("For multivariate functions (Tuple of column names), the number of input columns must be greater than 1"))
    col1, col2 = idx[src[1]], idx[src[2]]
    var1, var2 = _names(idx)[col1], _names(idx)[col2]
    if N > 2
        nname = Symbol(funname(fun), "_", var1, "_", var2, "_etc")
    else
        nname = Symbol(funname(fun), "_", var1, "_", var2)
    end


    _check_ind_and_add!(outidx, nname)
    return ntuple(i -> _names(idx)[idx[src[i]]], N) => fun => nname
end

# handling vector of tuples
function normalize_combine!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:Vector{<:NTuple{N, ColumnIndex}},
                                <:Union{Function}})) where N
    normalize_combine!(outidx, idx, sel.first .=> sel.second)
end
function normalize_combine!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:Vector{<:NTuple{N, ColumnIndex}},
                                <:Vector{<:Function}})) where N
    normalize_combine!(outidx, idx, Ref(sel.first) .=> sel.second)
end

# col => byrow
#TODO if we define byrow(fun) as a type rather than an Expr, we should modify this part
function normalize_combine!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:ColumnIndex,
                                <:Vector{Expr}}))
    if sel.second[1].head == :BYROW
        colsidx = outidx[sel.first]
        dsc_sym = Symbol(funname(sel.second[1].args[1]), "_", _names(outidx)[outidx[sel.first]])
        _check_ind_and_add!(outidx, dsc_sym )
        return _names(outidx)[outidx[colsidx]] => sel.second[1] => dsc_sym
    end
    throw(ArgumentError("only byrow is accepted when using expressions"))
end
function normalize_combine!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:ColumnIndex,
                                <:Expr}))
    if sel.second.head == :BYROW
        colsidx = outidx[sel.first]

        dsc_sym = Symbol(funname(sel.second.args[1]), "_", _names(outidx)[outidx[sel.first]])
        _check_ind_and_add!(outidx, dsc_sym )
        return _names(outidx)[outidx[colsidx]] => sel.second => dsc_sym
    end
    throw(ArgumentError("only byrow is accepted when using expressions"))
end

# col => byrow => dst
function normalize_combine!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:ColumnIndex,
                                <:Pair{<:Vector{Expr},
                                    <:Union{Symbol, AbstractString}}}))
    if sel.second.first[1].head == :BYROW
        colsidx = outidx[sel.first]
        _check_ind_and_add!(outidx, Symbol(sel.second.second))
        return _names(outidx)[outidx[colsidx]] => sel.second.first[1] => Symbol(sel.second.second)
    end
    throw(ArgumentError("only byrow is accepted when using expressions"))
end
function normalize_combine!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:ColumnIndex,
                                <:Pair{<:Expr,
                                    <:Union{Symbol, AbstractString}}}))
    if sel.second.first.head == :BYROW
        colsidx = outidx[sel.first]
        _check_ind_and_add!(outidx, Symbol(sel.second.second))
        return _names(outidx)[outidx[colsidx]] => sel.second.first => Symbol(sel.second.second)
    end
    throw(ArgumentError("only byrow is accepted when using expressions"))
end

# cols => byrow
function normalize_combine!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:MultiColumnIndex,
                                <:Vector{Expr}}))
    if sel.second[1] isa Expr
        if sel.second[1].head == :BYROW
            colsidx = outidx[sel.first]
        end
        _check_ind_and_add!(outidx, Symbol("row_", funname(sel.second[1].args[1])))
        return _names(outidx)[outidx[colsidx]] => sel.second[1] => Symbol("row_", funname(sel.second[1].args[1]))
    end
    throw(ArgumentError("only byrow is accepted when using expressions"))
end
# cols => fun/byrow normalise as cols .=> fun or cols => byrow
function normalize_combine!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:MultiColumnIndex,
                                <:Union{Function,Expr}}))
    if sel.second isa Expr
        if sel.second.head == :BYROW
            colsidx = outidx[sel.first]
        end
    # TODO needs a better name for destination
        _check_ind_and_add!(outidx, Symbol("row_", funname(sel.second.args[1])))
        return _names(outidx)[outidx[colsidx]] => sel.second => Symbol("row_", funname(sel.second.args[1]))
    end
    normalize_combine!(outidx, idx, idx[sel.first] .=> sel.second)
end

# cols => funs normalize cols .=> Ref(funs)
function normalize_combine!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:MultiColumnIndex,
                                <:Vector{<:Function}}))
    colsidx = idx[sel.first]
    normalize_combine!(outidx, idx, colsidx .=> Ref(sel.second))
end

# col => funs normalise as col .=> funs
function normalize_combine!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:ColumnIndex,
                                <:Vector{<:Function}}))
    colsidx = idx[sel.first]
    normalize_combine!(outidx, idx, colsidx .=> sel.second)
end

# special case cols => byrow(...) => :name
function normalize_combine!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:MultiColumnIndex,
                                <:Pair{<:Vector{Expr},
                                    <:Union{Symbol, AbstractString}}}))
    if sel.second.first[1].head == :BYROW
        colsidx = outidx[sel.first]
        _check_ind_and_add!(outidx, Symbol(sel.second.second))
        return _names(outidx)[outidx[colsidx]] => sel.second.first[1] => Symbol(sel.second.second)
    end
    throw(ArgumentError("only byrow operation is supported for cols => fun => :name"))
end
function normalize_combine!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:MultiColumnIndex,
                                <:Pair{<:Expr,
                                    <:Union{Symbol, AbstractString}}}))
    if sel.second.first.head == :BYROW
        colsidx = outidx[sel.first]
        _check_ind_and_add!(outidx, Symbol(sel.second.second))
        return _names(outidx)[outidx[colsidx]] => sel.second.first => Symbol(sel.second.second)
    end
    throw(ArgumentError("only byrow operation is supported for cols => fun => :name"))
end

# cols => fun => names normalise as cols .=> fun .=> names
function normalize_combine!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:MultiColumnIndex,
                                <:Pair{<:Union{Function},
                                    <:AbstractVector{<:Union{Symbol, AbstractString}}}}))
    colsidx = idx[sel.first]
    if !(length(colsidx) == length(sel.second.second))
        throw(ArgumentError("The input number of columns and the length of the output names should match"))
    end
    normalize_combine!(outidx, idx, colsidx .=> sel.second.first .=> sel.second.second)
end

# handling special case
function normalize_combine!(outidx::Index, idx,
                            @nospecialize(sel::Pair{<:Union{ColumnIndex, MultiColumnIndex}, <:AbstractVector}))
    normalize_combine!(outidx, idx, Ref(sel.first) .=> sel.second)
end

function normalize_combine!(outidx::Index, idx, arg::AbstractVector)
    res = Any[]
    for i in 1:length(arg)
        _res = normalize_combine!(outidx::Index, idx, arg[i])
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

function normalize_combine_multiple!(outidx::Index, idx, @nospecialize(args...))
    res = Any[]
    for i in 1:length(args)
        _res = normalize_combine!(outidx, idx, args[i])
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

function _is_byrow_valid(idx, ms)
    righthands = Int[]
    lookupdict = idx.lookup
    if ms[1].second.first isa Expr
        return false
    end
    for i in 1:length(ms)
        if (ms[i].second.first isa Expr) && ms[i].second.first.head == :BYROW
            # if the input vars are supposed to be used in a multivariate function
            if ms[i].first isa Tuple
                byrow_vars = [idx[ms[i].first[j]] for j in 1:length(ms[i].first)]
            else
                byrow_vars = idx[ms[i].first]
            end
            !all(byrow_vars .∈ Ref(righthands)) && return false
        end
        if haskey(idx, ms[i].second.second)
            push!(righthands, idx[ms[i].second.second])
        end
    end
    return true
end

function _check_mutliple_rows_for_each_group(ds, ms)
    for i in 1:length(ms)
        # byrow are not checked since they are not going to modify the number of rows
        if ms[i].first isa Tuple && !(ms[i].second.first isa Expr)
            T = return_type(ms[i].second.first, ntuple(j-> ds[!, ms[i].first[j]].val, length(ms[i].first)))
            if T <: AbstractVector && T !== Union{}
                return i
            end
        elseif !(ms[i].second.first isa Expr) &&
                 haskey(index(ds), ms[i].first) #&&
                    #!(ms[i].first ∈ map(x->x.second.second, view(ms, 1:(i-1)))) #TODO monitor this for any unseen problem
            T = return_type(ms[i].second.first, ds[!, ms[i].first].val)
            if T <: AbstractVector && T !== Union{}
                return i
            end
        end
    end
    return 0
end

function _is_groupingcols_modifed(ds, ms)
    groupcols::Vector{Int} = _groupcols(ds)
    idx = index(ds)
    all_names = _names(ds)
    for i in 1:length(ms)
        if (ms[i].second.second ∈ all_names) && (idx[ms[i].second.second] ∈ groupcols)
            return true
        end
    end
    return false
end


function _compute_the_mutli_row_trans!(special_res, new_lengths, x, nrows, _f, _first_vector_res, starts, ngroups, threads)
    @_threadsfor threads for g in 1:ngroups
        lo = starts[g]
        g == ngroups ? hi = nrows : hi = starts[g + 1] - 1
        special_res[g] = _f(view(x, lo:hi))
        new_lengths[g] = length(special_res[g])
    end
end
function _compute_the_mutli_row_trans_tuple!(special_res, new_lengths, x, nrows, _f, _first_vector_res, starts, ngroups, threads)
    @_threadsfor threads for g in 1:ngroups
        lo = starts[g]
        g == ngroups ? hi = nrows : hi = starts[g + 1] - 1
        special_res[g] = do_call(_f, x, lo:hi)
        new_lengths[g] = length(special_res[g])
    end
end

# this returns lookup dictionary and names for the new ds
function _create_index_for_newds(ds, ms, groupcols)
    all_names = _names(ds)
    nm = Symbol[]
    lookup = Dict{Symbol, Int}()
    cnt = 1
    for j in 1:length(groupcols)
        new_name = all_names[groupcols[j]]
        if !haskey(lookup, new_name)
            push!(nm, new_name)
            push!(lookup, new_name => cnt)
            cnt += 1
        end
    end
    for i in 1:length(ms)
        new_name = ms[i].second.second
         if !haskey(lookup, new_name)
            push!(nm, new_name)
            push!(lookup, new_name => cnt)
            cnt += 1
        end
    end
    return (lookup, nm)
end

function _push_groups_to_res_pa!(res, _tmpres, x, starts, new_lengths, total_lengths, j, groupcols, ngroups, threads)
    y = DataAPI.refarray(x)
    @_threadsfor threads for i in 1:ngroups
        counter::UnitRange{Int} = 1:1
        i == 1 ? (counter = 1:new_lengths[1]) : (counter = (new_lengths[i - 1] + 1):new_lengths[i])
        fill!(view(_tmpres.refs, (new_lengths[i] - length(counter) + 1):(new_lengths[i])),  y[starts[i]])
    end
    push!(res, _tmpres)
end

function _push_groups_to_res!(res, _tmpres, x, starts, new_lengths, total_lengths, j, groupcols, ngroups, threads)
    @_threadsfor threads for i in 1:ngroups
        counter::UnitRange{Int} = 1:1
        i == 1 ? (counter = 1:new_lengths[1]) : (counter = (new_lengths[i - 1] + 1):new_lengths[i])
        fill!(view(_tmpres, (new_lengths[i] - length(counter) + 1):(new_lengths[i])),  x[starts[i]])
    end
    push!(res, _tmpres)
end
function _check_the_output_type(x, mssecond)
    CT = return_type(mssecond, x)
    # TODO check other possibilities:
    # the result can be
    # * AbstractVector{T} where T
    # * Vector{T}
    # * not a Vector
    CT == Union{} && throw(ArgumentError("compiler cannot assess the return type of calling `$(mssecond)` on input, you may want to try using `byrow`."))
    if CT <: AbstractVector
        if hasproperty(CT, :var)
            T = Union{Missing, CT.var.ub}
        else
            T = Union{Missing, eltype(CT)}
        end
    else
        T = Union{Missing, CT}
    end
    return T
end

function _update_one_col_combine!(res, _res, x, _f, ngroups, new_lengths, total_lengths, col, threads)
    # make sure lo and hi are not defined any where outside the following loop
    @_threadsfor threads for g in 1:ngroups
        counter::UnitRange{Int} = 1:1
        g == 1 ? (counter = 1:new_lengths[1]) : (counter = (new_lengths[g - 1] + 1):new_lengths[g])
        lo = new_lengths[g] - length(counter) + 1
        hi = new_lengths[g]
        _tmp_res = _f(view(x, counter))
        check_scalar = _is_scalar(_tmp_res, length(lo:hi))
        if check_scalar
            fill!(view(_res,lo:hi), _tmp_res)
        else
            copy!(view(_res, lo:hi), _tmp_res)
        end
    end
    res[col] = _res
    return _res
end

function _add_one_col_combine!(res, _res, in_x, _f, starts, ngroups, new_lengths, total_lengths, nrows, threads)
    # make sure lo and hi are not defined any where outside the following loop
    @_threadsfor threads for g in 1:ngroups
        counter::UnitRange{Int} = 1:1
        g == 1 ? (counter = 1:new_lengths[1]) : (counter = (new_lengths[g - 1] + 1):new_lengths[g])
        lo = starts[g]
        g == ngroups ? hi = nrows : hi = starts[g + 1] - 1
        l1 = new_lengths[g] - length(counter) + 1
        h1 = new_lengths[g]
        _tmp_res = _f(view(in_x, lo:hi))
        check_scalar = _is_scalar(_tmp_res, length(l1:h1))
        if check_scalar
            fill!(view(_res,l1:h1), _tmp_res)
        else
            copy!(view(_res, l1:h1), _tmp_res)
        end
    end
    push!(res, _res)
    return _res
end
function _add_one_col_combine_tuple!(res, _res, in_x, _f, starts, ngroups, new_lengths, total_lengths, nrows, threads)
    # make sure lo and hi are not defined any where outside the following loop
    @_threadsfor threads for g in 1:ngroups
        counter::UnitRange{Int} = 1:1
        g == 1 ? (counter = 1:new_lengths[1]) : (counter = (new_lengths[g - 1] + 1):new_lengths[g])
        lo = starts[g]
        g == ngroups ? hi = nrows : hi = starts[g + 1] - 1
        l1 = new_lengths[g] - length(counter) + 1
        h1 = new_lengths[g]
        _tmp_res = do_call(_f, in_x, lo:hi)
        check_scalar = _is_scalar(_tmp_res, length(l1:h1))
        if check_scalar
            fill!(view(_res,l1:h1), _tmp_res)
        else
            copy!(view(_res, l1:h1), _tmp_res)
        end
    end
    push!(res, _res)
    return _res
end
function _update_one_col_combine!(res, _res, in_x, _f, starts, ngroups, new_lengths, total_lengths, nrows, col, threads)
    # make sure lo and hi are not defined any where outside the following loop
    @_threadsfor threads for g in 1:ngroups
        counter::UnitRange{Int} = 1:1
        g == 1 ? (counter = 1:new_lengths[1]) : (counter = (new_lengths[g - 1] + 1):new_lengths[g])
        lo = starts[g]
        g == ngroups ? hi = nrows : hi = starts[g + 1] - 1
        l1 = new_lengths[g] - length(counter) + 1
        h1 = new_lengths[g]
        _tmp_res = _f(view(in_x, lo:hi))
        check_scalar = _is_scalar(_tmp_res, length(l1:h1))
        if check_scalar
            fill!(view(_res,l1:h1), _tmp_res)
        else
            copy!(view(_res, l1:h1), _tmp_res)
        end
    end
    res[col] = _res
    return _res
end

function _special_res_fill_barrier!(_res, vals, nl_g, l_cnt)
    for k in 1:l_cnt
        _res[nl_g - l_cnt + k] = vals[k]
    end
end

# special_res cannot be based on previous columns of the combined data set
function _fill_res_with_special_res!(res, _res, special_res, ngroups, new_lengths, total_lengths, threads)
     @_threadsfor threads for g in 1:ngroups
        counter::UnitRange{Int} = 1:1
        g == 1 ? (counter = 1:new_lengths[1]) : (counter = (new_lengths[g - 1] + 1):new_lengths[g])
        # this is not optimized for pooled arrays
        # for k in 1:length(counter)
        #     _res[new_lengths[g] - length(counter) + k] = special_res[g][k]
        # end
        _special_res_fill_barrier!(_res, special_res[g], new_lengths[g], length(counter))
    end
    empty!(special_res)
    GC.safepoint()
    push!(res, _res)
end
# special_res cannot be based on previous columns of the combined data set
function _update_res_with_special_res!(res, _res, special_res, ngroups, new_lengths, total_lengths, col, threads)
     @_threadsfor threads for g in 1:ngroups
        counter::UnitRange{Int} = 1:1
        g == 1 ? (counter = 1:new_lengths[1]) : (counter = (new_lengths[g - 1] + 1):new_lengths[g])
        # this is not optimized for pooled arrays
        # for k in 1:length(counter)
        #     _res[new_lengths[g] - length(counter) + k] = special_res[g][k]
        # end
        _special_res_fill_barrier!(_res, special_res[g], new_lengths[g], length(counter))
    end
    res[col] = _res
    return _res
end

function _combine_f_barrier_special(special_res, fromds, newds, msfirst, mssecond, mslast, newds_lookup, _first_vector_res, ngroups, new_lengths, total_lengths, threads)
        T = _check_the_output_type(fromds, mssecond)
        _res = allocatecol(Union{Missing, T}, total_lengths)
        _fill_res_with_special_res!(_columns(newds), _res, special_res, ngroups, new_lengths, total_lengths, threads)
end
function _combine_f_barrier_special_tuple(special_res, fromds, newds, msfirst, mssecond, mslast, newds_lookup, _first_vector_res, ngroups, new_lengths, total_lengths, threads)
        T = _check_the_output_type(fromds, mssecond)
        _res = allocatecol(Union{Missing, T}, total_lengths)
        _fill_res_with_special_res!(_columns(newds), _res, special_res, ngroups, new_lengths, total_lengths, threads)
end

function _combine_f_barrier(fromds, newds, msfirst, mssecond, mslast, newds_lookup, starts, ngroups, new_lengths, total_lengths, threads)

    if !(mssecond isa Expr)
        if !haskey(newds_lookup, mslast)
            T = _check_the_output_type(fromds, mssecond)
            _res = allocatecol(Union{Missing, T}, total_lengths)
            _add_one_col_combine!(_columns(newds), _res, fromds, mssecond, starts, ngroups, new_lengths, total_lengths, length(fromds), threads)
        else
            T = _check_the_output_type(fromds, mssecond)
            _res = allocatecol(Union{Missing, T}, total_lengths)
            _update_one_col_combine!(_columns(newds), _res, fromds, mssecond, starts, ngroups, new_lengths, total_lengths, length(fromds), newds_lookup[mslast], threads)
            # _update_one_col_combine!(_columns(newds), _res, fromds, mssecond, ngroups, new_lengths, total_lengths, newds_lookup[mslast])
        end

    elseif (mssecond isa Expr) && mssecond.head == :BYROW
        push!(_columns(newds), byrow(newds, mssecond.args[1], msfirst; mssecond.args[2]...))
    else
        throw(ArgumentError("`combine` doesn't support $(msfirst=>mssecond=>mslast) combination"))
    end
end
function _combine_f_barrier_tuple(fromds, newds, msfirst, mssecond, mslast, newds_lookup, starts, ngroups, new_lengths, total_lengths, threads)
    T = _check_the_output_type(fromds, mssecond)
    _res = allocatecol(Union{Missing, T}, total_lengths)
    _add_one_col_combine_tuple!(_columns(newds), _res, fromds, mssecond, starts, ngroups, new_lengths, total_lengths, length(fromds[1]), threads)

end

"""
    combine(ds::AbstractDataset, args...; dropgroupcols = false, threads = true)

Create a new data set while the `args` aggregations has been applied on passed columns. The `args` argument must be in the form of `cols=>fun=>newname`, where `cols` refers to columns in the passed data set. `fun` assumes a single column as its input, thus, multiple columns will be broadcasted, i.e. `cols=>fun` will be tranlated as `col1=>fun`, `col2=>fun`, ..., and `col=>funs` will be translated as `col=>fun1`, `col=>fun2`, .... The `byrow` function can be passed as `fun`, however, its input must be referring to columns which already an operation has been done on them.

For using a multivate function the columns must be passed as tuple of column names or column indices.

For grouped data set the operations are done on each group of observations.

# Examples

```jldoctest
julia> ds = Dataset(g = [1,2,1,2,1,2], x = 1:6)
6×2 Dataset
 Row │ g         x
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────
   1 │        1         1
   2 │        2         2
   3 │        1         3
   4 │        2         4
   5 │        1         5
   6 │        2         6

julia> combine(groupby(ds, :g), :x=>[IMD.sum, mean])
2×3 Dataset
 Row │ g         sum_x     mean_x
     │ identity  identity  identity
     │ Int64?    Int64?    Float64?
─────┼──────────────────────────────
   1 │        1         9       3.0
   2 │        2        12       4.0

julia> combine(gatherby(ds, :g), :x => [IMD.maximum, IMD.minimum], 2:3 => byrow(-) => :range)
2×4 Dataset
 Row │ g         maximum_x  minimum_x  range
     │ identity  identity   identity   identity
     │ Int64?    Int64?     Int64?     Int64?
─────┼──────────────────────────────────────────
   1 │        1          5          1         4
   2 │        2          6          2         4

julia> ds = Dataset(g = [1,2,1,2,1,2], x = 1:6, y = 6:-1:1)
6×3 Dataset
 Row │ g         x         y
     │ identity  identity  identity
     │ Int64?    Int64?    Int64?
─────┼──────────────────────────────
   1 │        1         1         6
   2 │        2         2         5
   3 │        1         3         4
   4 │        2         4         3
   5 │        1         5         2
   6 │        2         6         1

julia> combine(groupby(ds,1), (:x, :y)=>(x1,x2)->IMD.maximum(x1)-IMD.minimum(x2))
2×2 Dataset
 Row │ g         function_x_y
     │ identity  identity
     │ Int64?    Int64?
─────┼────────────────────────
   1 │        1             3
   2 │        2             5

```
"""
function combine(ds::Dataset, @nospecialize(args...); dropgroupcols = false, threads = true)
    !isgrouped(ds) &&  return combine_ds(ds, args...)#throw(ArgumentError("`combine` is only for grouped data sets, use `modify` instead"))
    idx_cpy::Index = Index(Dict{Symbol, Int}(), Symbol[], Dict{Int, Function}())
    if !dropgroupcols
        for i in _sortedcols(ds)
            push!(idx_cpy, Symbol(names(ds)[i]))
        end
    end
    ms = normalize_combine_multiple!(idx_cpy, index(ds), args...)
    # the rule is that in combine, byrow must only be used for already aggregated columns
    # so, we should check every thing pass to byrow has been assigned in args before it
    # if this is not the case, throw ArgumentError and ask user to use modify instead
    newlookup, new_nm = _create_index_for_newds(ds, ms, index(ds).sortedcols)
    !(_is_byrow_valid(Index(newlookup, new_nm, Dict{Int, Function}()), ms)) && throw(ArgumentError("`byrow` must be used for aggregated columns, use `modify` otherwise"))
    # _check_mutliple_rows_for_each_group return the first transformation which causes multiple
    # rows or 0 if all transformations return scalar for each group
    # the transformation returning multiple rows must not be based on the previous columns in combine
    # result (which seems reasonable ??)
    _first_vector_res = _check_mutliple_rows_for_each_group(ds, ms)

    _is_groupingcols_modifed(ds, ms) && throw(ArgumentError("`combine` cannot modify the grouping or sorting columns, use a different name for the computed column"))

    groupcols = index(ds).sortedcols
    starts = index(ds).starts
    ngroups::Int = index(ds).ngroups[]

    # we will use new_lengths later for assigning the grouping info of the new ds
    if _first_vector_res == 0
        new_lengths = ones(Int, ngroups)
        our_cumsum!(new_lengths)
        total_lengths = ngroups
    else
        if ms[_first_vector_res].first isa Tuple
            CT = return_type(ms[_first_vector_res].second.first,
                 ntuple(i->_columns(ds)[index(ds)[ms[_first_vector_res].first[i]]], length(ms[_first_vector_res].first)))
        else
            CT = return_type(ms[_first_vector_res].second.first,
                 ds[!, ms[_first_vector_res].first].val)
        end
        special_res = _our_vect_alloc(CT, ngroups)
        new_lengths = _our_vect_alloc(Int, ngroups)
        # _columns(ds)[ms[_first_vector_res].first]
        if  ms[_first_vector_res].first isa Tuple
            _compute_the_mutli_row_trans_tuple!(special_res, new_lengths, ntuple(i->_columns(ds)[index(ds)[ms[_first_vector_res].first[i]]], length(ms[_first_vector_res].first)), nrow(ds), ms[_first_vector_res].second.first, _first_vector_res, starts, ngroups, threads)
        else
            _compute_the_mutli_row_trans!(special_res, new_lengths, _columns(ds)[index(ds)[ms[_first_vector_res].first]], nrow(ds), ms[_first_vector_res].second.first, _first_vector_res, starts, ngroups, threads)
        end
        # special_res, new_lengths = _compute_the_mutli_row_trans(ds, ms, _first_vector_res, starts, ngroups)
        our_cumsum!(new_lengths)
        total_lengths = new_lengths[end]
    end
    all_names = _names(ds)

    newds_idx = Index(Dict{Symbol, Int}(), Symbol[], Dict{Int, Function}(), Int[], Bool[], false, [], Int[], 1, false)

    newds = Dataset([], newds_idx)
    newds_lookup = index(newds).lookup
    var_cnt = 1
    if !dropgroupcols

        for j in 1:length(groupcols)
            _tmpres = allocatecol(ds[!, groupcols[j]].val, total_lengths)
            if DataAPI.refpool(_tmpres) !== nothing
                _push_groups_to_res_pa!(_columns(newds), _tmpres, _columns(ds)[groupcols[j]], starts, new_lengths, total_lengths, j, groupcols, ngroups, threads)
            else
                _push_groups_to_res!(_columns(newds), _tmpres, _columns(ds)[groupcols[j]], starts, new_lengths, total_lengths, j, groupcols, ngroups, threads)
            end
            push!(index(newds), new_nm[var_cnt])
            setformat!(newds, new_nm[var_cnt] => get(index(ds).format, groupcols[j], identity))
            var_cnt += 1

        end
    end
    for i in 1:length(ms)
        if i == _first_vector_res
            if ms[i].first isa Tuple
                _combine_f_barrier_special_tuple(special_res, ntuple(j->_columns(ds)[index(ds)[ms[i].first[j]]], length(ms[i].first)), newds, ms[i].first, ms[i].second.first, ms[i].second.second, newds_lookup, _first_vector_res,ngroups, new_lengths, total_lengths, threads)
            else
                _combine_f_barrier_special(special_res, ds[!, ms[i].first].val, newds, ms[i].first, ms[i].second.first, ms[i].second.second, newds_lookup, _first_vector_res,ngroups, new_lengths, total_lengths, threads)
            end
        else
            if ms[i].first isa Tuple && !(ms[i].second.first isa Expr)
                _combine_f_barrier_tuple(ntuple(j->_columns(ds)[index(ds)[ms[i].first[j]]], length(ms[i].first)), newds, ms[i].first, ms[i].second.first, ms[i].second.second, newds_lookup, starts, ngroups, new_lengths, total_lengths, threads)
            else
                _combine_f_barrier(haskey(index(ds).lookup, ms[i].first) ? _columns(ds)[index(ds)[ms[i].first]] : _columns(ds)[1], newds, ms[i].first, ms[i].second.first, ms[i].second.second, newds_lookup, starts, ngroups, new_lengths, total_lengths, threads)
            end
        end
        if !haskey(index(newds), ms[i].second.second)
            push!(index(newds), ms[i].second.second)
        end

    end
    # grouping information for the output dataset
    # append!(index(newds).sortedcols, index(newds)[index(ds).names[index(ds).sortedcols]])
    # append!(index(newds).rev, index(ds).rev)
    # append!(index(newds).perm, collect(1:total_lengths))
    # # index(newds).grouped[] = true
    # index(newds).ngroups[] = ngroups
    # append!(index(newds).starts, collect(1:total_lengths))
    # for i in 2:(length(new_lengths))
    #     index(newds).starts[i] = new_lengths[i - 1]+1
    # end
    newds
end


combine(ds::SubDataset, @nospecialize(args...); threads = true) = combine_ds(ds::AbstractDataset, args...; threads = threads)

function combine_ds(ds::AbstractDataset, @nospecialize(args...); threads = true)
    idx_cpy::Index = Index(Dict{Symbol, Int}(), Symbol[], Dict{Int, Function}())
    ms = normalize_combine_multiple!(idx_cpy, index(ds), args...)
    if ds isa SubDataset
        newlookup, new_nm = _create_index_for_newds(ds, ms, Int[])
    else
        newlookup, new_nm = _create_index_for_newds(ds, ms, index(ds).sortedcols)
    end
    !(_is_byrow_valid(Index(newlookup, new_nm, Dict{Int, Function}()), ms)) && throw(ArgumentError("`byrow` must be used for aggregated columns, use `modify` otherwise"))
    _first_vector_res = _check_mutliple_rows_for_each_group(ds, ms)


    starts = [1]
    ngroups::Int = 1

    # we will use new_lengths later for assigning the grouping info of the new ds
    if _first_vector_res == 0
        new_lengths = ones(Int, ngroups)
        our_cumsum!(new_lengths)
        total_lengths = ngroups
    else
        if ms[_first_vector_res].first isa Tuple
            CT = return_type(ms[_first_vector_res].second.first,
                 ntuple(i->_columns(ds)[index(ds)[ms[_first_vector_res].first[i]]], length(ms[_first_vector_res].first)))
        else
            CT = return_type(ms[_first_vector_res].second.first,
                 ds[!, ms[_first_vector_res].first].val)
        end
        special_res = _our_vect_alloc(CT, ngroups)
        new_lengths = _our_vect_alloc(Int, ngroups)
        # _columns(ds)[ms[_first_vector_res].first]
        if  ms[_first_vector_res].first isa Tuple
            _compute_the_mutli_row_trans_tuple!(special_res, new_lengths, ntuple(i->_columns(ds)[index(ds)[ms[_first_vector_res].first[i]]], length(ms[_first_vector_res].first)), nrow(ds), ms[_first_vector_res].second.first, _first_vector_res, starts, ngroups, threads)
        else
            _compute_the_mutli_row_trans!(special_res, new_lengths, _columns(ds)[index(ds)[ms[_first_vector_res].first]], nrow(ds), ms[_first_vector_res].second.first, _first_vector_res, starts, ngroups, threads)
        end
        # special_res, new_lengths = _compute_the_mutli_row_trans(ds, ms, _first_vector_res, starts, ngroups)
        our_cumsum!(new_lengths)
        total_lengths = new_lengths[end]
    end
    all_names = _names(ds)

    newds_idx = Index(Dict{Symbol, Int}(), Symbol[], Dict{Int, Function}(), Int[], Bool[], false, [], Int[], 1, false)

    newds = Dataset([], newds_idx)
    newds_lookup = index(newds).lookup
    var_cnt = 1

    for i in 1:length(ms)
        if i == _first_vector_res
            if ms[i].first isa Tuple
                _combine_f_barrier_special_tuple(special_res, ntuple(j->_columns(ds)[index(ds)[ms[i].first[j]]], length(ms[i].first)), newds, ms[i].first, ms[i].second.first, ms[i].second.second, newds_lookup, _first_vector_res,ngroups, new_lengths, total_lengths, threads)
            else
                _combine_f_barrier_special(special_res, ds[!, ms[i].first].val, newds, ms[i].first, ms[i].second.first, ms[i].second.second, newds_lookup, _first_vector_res,ngroups, new_lengths, total_lengths, threads)
            end
        else
            if ms[i].first isa Tuple && !(ms[i].second.first isa Expr)
                _combine_f_barrier_tuple(ntuple(j->_columns(ds)[index(ds)[ms[i].first[j]]], length(ms[i].first)), newds, ms[i].first, ms[i].second.first, ms[i].second.second, newds_lookup, starts, ngroups, new_lengths, total_lengths, threads)
            else
                _combine_f_barrier(haskey(index(ds), ms[i].first) ? _columns(ds)[index(ds)[ms[i].first]] : _columns(ds)[1], newds, ms[i].first, ms[i].second.first, ms[i].second.second, newds_lookup, starts, ngroups, new_lengths, total_lengths, threads)
            end
        end
        if !haskey(index(newds), ms[i].second.second)
            push!(index(newds), ms[i].second.second)
        end

    end
    newds
end
