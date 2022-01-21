function hp_row_sort!(ds::Dataset, cols = names(ds, Union{Missing, Number}); kwargs...)
    colsidx = index(ds)[cols]
    T = mapreduce(eltype, promote_type, eachcol(ds)[colsidx])
    m = Matrix{T}(ds[!, colsidx])
    Threads.@threads for i in 1:size(m, 1)
        @views sort!(m[i, :]; kwargs...)
    end
    # TODO no parallel is needed here to minimise memory
    for i in 1:length(colsidx)
        _columns(ds)[colsidx[i]] = m[:, i]
    end
    removeformat!(ds, cols)
    any(index(ds).sortedcols .∈ Ref(colsidx)) && _reset_grouping_info!(ds)
    _modified(_attributes(ds))
    ds
end

"""
    row_sort!(ds::AbstractDataset[, cols]; kwargs...)
    sort `cols` in each row.
"""
function hp_row_sort(ds::AbstractDataset, cols = names(ds, Union{Missing, Number}); kwargs...)
    dscopy = copy(ds)
    hp_row_sort!(dscopy, cols; kwargs...)
    dscopy
end

function hp_row_generic(ds::AbstractDataset, f::Function, cols::MultiColumnIndex)
    colsidx = multiple_getindex(index(ds), cols)
    if length(colsidx) == 2
        try
            allowmissing(f.(_columns(ds)[colsidx[1]], _columns(ds)[colsidx[2]]))
        catch e
            if e isa MethodError
                _hp_row_generic(ds, f, colsidx)
            else
                rethrow(e)
            end
        end
    else
        _hp_row_generic(ds, f, colsidx)
    end
end


function _hp_row_generic(ds::AbstractDataset, f::Function, colsidx)
    T = mapreduce(eltype, promote_type, view(_columns(ds),colsidx))
    inmat = Matrix{T}(undef, length(colsidx), min(1000, nrow(ds)))

    all_data = view(_columns(ds), colsidx)
    _fill_matrix!(inmat, all_data, 1:min(1000, nrow(ds)), colsidx)
    res_temp = allowmissing(f.(eachcol(inmat)))
    if !(typeof(res_temp) <:  AbstractVector)
        throw(ArgumentError("output of `f` must be a vector"))
    end

    # if length(res_temp[1]) > 1
    #     throw(ArgumentError("The matrix output is not supported"))
    #     res = similar(res_temp, nrow(ds), size(res_temp,2))
    # elseif length(res_temp[1]) == 1
    res = similar(res_temp, nrow(ds))
    # else
        # throw(ArgumentError("the result cannot be with zero dimension"))

    if nrow(ds)>1000
        if size(res, 2) == 1
            view(res, 1:1000) .= res_temp
            _hp_row_generic_vec!(res, ds, f, colsidx, Val(T))
        else
            view(res, 1:1000, :) .= res_temp
            # not implemented yet
            _hp_row_generic_mat!(res, ds, f, colsidx)
        end
    else
        return res_temp
    end
    return res
end

function _hp_row_generic_vec!(res, ds, f, colsidx, ::Val{T}) where T
    nt = Threads.nthreads()
    loopsize = div(length(res) - 1000, 1000)
    all_data = view(_columns(ds), colsidx)
    if loopsize == 0
        st = 1001
        en = length(res)
        inmat = Matrix{T}(undef, length(colsidx), en - st + 1)
        _fill_matrix!(inmat, all_data, st:en, colsidx)
        view(res, st:en) .= f.(eachcol(inmat))
        return
    end
    max_cz = length(res) - 1000 - (loopsize - 1)*1000
    inmat_all = [Matrix{T}(undef, length(colsidx), max_cz) for i in 1:nt]
    # make sure that the variable inside the loop are not the same as the out of scope one
    Threads.@threads for i in 1:loopsize
        t_st = i*1000 + 1
        i == loopsize ? t_en = length(res) : t_en = (i+1)*1000
        _fill_matrix!(inmat_all[Threads.threadid()], all_data, t_st:t_en, colsidx)
        for k in t_st:t_en
            res[k] = f(view(inmat_all[Threads.threadid()], :, k - t_st + 1))
        end
    end
end
