function hp_maximum(f, x::AbstractVector{T}) where {T}
    n = length(x)
    nt = Threads.nthreads()
    cz = div(n, nt)
    cz == 0 && return stat_maximum(f, x)
    CT = Core.Compiler.return_type(f, Tuple{our_nonmissingtype(eltype(x))})
    if T >: Missing
        CT = Union{Missing,CT}
    end
    res = Vector{CT}(undef, nt)
    Threads.@threads for i in 1:nt
        i == nt ? hi = n : hi = i * cz
        res[i] = stat_maximum(f, x, lo=(i - 1) * cz + 1, hi=hi)
    end
    stat_maximum(identity, res)
end
hp_maximum(x::AbstractVector{T}) where {T} = hp_maximum(identity, x)


function hp_minimum(f, x::AbstractVector{T}) where {T}
    n = length(x)
    nt = Threads.nthreads()
    cz = div(n, nt)
    cz == 0 && return stat_minimum(f, x)
    CT = Core.Compiler.return_type(f, Tuple{our_nonmissingtype(eltype(x))})
    if T >: Missing
        CT = Union{Missing,CT}
    end
    res = Vector{CT}(undef, nt)
    Threads.@threads for i in 1:nt
        i == nt ? hi = n : hi = i * cz
        res[i] = stat_minimum(f, x, lo=(i - 1) * cz + 1, hi=hi)
    end
    stat_minimum(identity, res)
end
hp_minimum(x::AbstractVector{T}) where {T} = hp_minimum(identity, x)


function hp_sum(f, x::AbstractVector{T}) where {T}
    n = length(x)
    nt = Threads.nthreads()
    cz = div(n, nt)
    cz == 0 && return stat_sum(f, x)
    CT = Core.Compiler.return_type(f, Tuple{our_nonmissingtype(eltype(x))})
    CT <: SMALLSIGNED ? CT = Int : nothing
    CT <: SMALLUNSIGNED ? CT = UInt : nothing
    CT <: Bool ? CT = Int : nothing
    if T >: Missing
        CT = Union{Missing,CT}
    end
    res = Vector{CT}(undef, nt)
    Threads.@threads for i in 1:nt
        i == nt ? hi = n : hi = i * cz
        res[i] = stat_sum(f, x, lo=(i - 1) * cz + 1, hi=hi)
    end
    stat_sum(identity, res)
end
hp_sum(x::AbstractVector{T}) where {T} = hp_sum(identity, x)

Base.@propagate_inbounds function hp_topk_vals(x::AbstractVector{T}, k::Int, lt_fun::F, by) where {T} where {F}
    k < 1 && throw(ArgumentError("k must be greater than 1"))
    all(ismissing, x) && return Union{Missing,T}[missing]
    nt = Threads.nthreads()
    res = Vector{T}(undef, k * nt)
    res_out = allowmissing(res)
    fill!(res_out, missing)
    cz = div(length(x), nt)
    Threads.@threads for i in 1:nt
        lo = (i - 1) * cz + 1
        i == nt ? hi = length(x) : hi = i * cz
        th_res = view(res, (i-1)*k+1:i*k)
        th_x = view(x, lo:hi)
        th_res_out = view(res_out, (i-1)*k+1:i*k)
        idx, cnt = initiate_topk_res!(th_res, th_x, by)
        topk_sort!(th_res, 1, cnt, lt_fun)
        for i in idx+1:length(th_x)
            if !ismissing(by(th_x[i]))
                insert_fixed_sorted!(th_res, th_x[i], lt_fun)
                cnt += 1
            end
        end
        if cnt < k
            view(th_res_out, 1:cnt) .= view(th_res, 1:cnt)
        else
            th_res_out .= th_res
        end
    end
    topk_vals(res_out, k, lt_fun, by)
end
Base.@propagate_inbounds function hp_topk_vals(x::Union{Vector{T}, SubArray{T, N, Vector{T}, Tuple{I}, L}}, k::Int, lt_fun::F, by) where {T<:Union{Missing, FLOATS, INTEGERS}} where {F} where N where I <: UnitRange{Int} where L
    k < 1 && throw(ArgumentError("k must be greater than 1"))
    all(ismissing, x) && return Union{Missing,T}[missing]
    nt = Threads.nthreads()
    res = Vector{T}(undef, k * nt)
    res_out = allowmissing(res)
    fill!(res_out, missing)
    cz = div(length(x), nt)
    Threads.@threads for i in 1:nt
        lo = (i - 1) * cz + 1
        i == nt ? hi = length(x) : hi = i * cz
        th_res = view(res, (i-1)*k+1:i*k)
        th_x = view(x, lo:hi)
        th_res_out = view(res_out, (i-1)*k+1:i*k)
        idx, cnt = initiate_topk_res!(th_res, th_x, by)
        topk_sort!(th_res, 1, cnt, lt_fun)
        if k < 21
            for i in idx+1:length(th_x)
                if !ismissing(by(th_x[i]))
                    insert_fixed_sorted!(th_res, th_x[i], lt_fun)
                    cnt += 1
                end
            end
        else
            for i in idx+1:length(th_x)
                if !ismissing(by(th_x[i]))
                    insert_fixed_sorted_binary!(th_res, th_x[i], lt_fun)
                    cnt += 1
                end
            end
        end
        if cnt < k
            view(th_res_out, 1:cnt) .= view(th_res, 1:cnt)
        else
            th_res_out .= th_res
        end
    end
    topk_vals(res_out, k, lt_fun, by)
end


Base.@propagate_inbounds function hp_topk_perm(x::AbstractVector{T}, k::Int, lt_fun::F, by) where {T} where {F}
    k < 1 && throw(ArgumentError("k must be greater than 1"))
    all(ismissing, x) && return Union{Missing,Int}[missing]
    nt = Threads.nthreads()
    res = Vector{T}(undef, k * nt)
    res_out = allowmissing(res)
    fill!(res_out, missing)
    perm = zeros(Int, k * nt)
    perm_out = allowmissing(perm)
    fill!(perm_out, missing)
    cz = div(length(x), nt)
    Threads.@threads for i in 1:nt
        lo = (i - 1) * cz + 1
        i == nt ? hi = length(x) : hi = i * cz
        th_res = view(res, (i-1)*k+1:i*k)
        th_perm = view(perm, (i-1)*k+1:i*k)
        th_x = view(x, lo:hi)
        th_res_out = view(res_out, (i-1)*k+1:i*k)
        th_perm_out = view(perm_out, (i-1)*k+1:i*k)
        idx, cnt = initiate_topk_res_perm!(th_perm, th_res, th_x, by, offset = lo - 1)
        topk_sort_permute!(th_res, th_perm, 1, cnt, lt_fun)
        for i in idx+1:length(th_x)
            if !ismissing(by(th_x[i]))
                insert_fixed_sorted_perm!(th_perm, th_res, i + lo - 1, th_x[i], lt_fun)
                cnt += 1
            end
        end
        if cnt < k
            view(th_res_out, 1:cnt) .= view(th_res, 1:cnt)
            view(th_perm_out, 1:cnt) .= view(th_perm, 1:cnt)
        else
            th_res_out .= th_res
            th_perm_out .= th_perm
        end
    end
    perm_out[topk_perm(res_out, k, lt_fun, by)]
end

Base.@propagate_inbounds function hp_topk_perm(x::Union{Vector{T}, SubArray{T, N, Vector{T}, Tuple{I}, L}}, k::Int, lt_fun::F, by) where {T<:Union{Missing, FLOATS, INTEGERS}} where {F} where N where I <: UnitRange{Int} where L
    k < 1 && throw(ArgumentError("k must be greater than 1"))
    all(ismissing, x) && return Union{Missing,Int}[missing]
    nt = Threads.nthreads()
    res = Vector{T}(undef, k * nt)
    res_out = allowmissing(res)
    fill!(res_out, missing)
    perm = zeros(Int, k * nt)
    perm_out = allowmissing(perm)
    fill!(perm_out, missing)
    cz = div(length(x), nt)
    Threads.@threads for i in 1:nt
        lo = (i - 1) * cz + 1
        i == nt ? hi = length(x) : hi = i * cz
        th_res = view(res, (i-1)*k+1:i*k)
        th_perm = view(perm, (i-1)*k+1:i*k)
        th_x = view(x, lo:hi)
        th_res_out = view(res_out, (i-1)*k+1:i*k)
        th_perm_out = view(perm_out, (i-1)*k+1:i*k)
        idx, cnt = initiate_topk_res_perm!(th_perm, th_res, th_x, by, offset = lo - 1)
        topk_sort_permute!(th_res, th_perm, 1, cnt, lt_fun)
        if k < 16
            for i in idx+1:length(th_x)
                if !ismissing(by(th_x[i]))
                    insert_fixed_sorted_perm!(th_perm, th_res, i + lo - 1, th_x[i], lt_fun)
                    cnt += 1
                end
            end
        else
            for i in idx+1:length(th_x)
                if !ismissing(by(th_x[i]))
                    insert_fixed_sorted_perm_binary!(th_perm, th_res, i + lo - 1, th_x[i], lt_fun)
                    cnt += 1
                end
            end
        end
        if cnt < k
            view(th_res_out, 1:cnt) .= view(th_res, 1:cnt)
            view(th_perm_out, 1:cnt) .= view(th_perm, 1:cnt)
        else
            th_res_out .= th_res
            th_perm_out .= th_perm
        end
    end
    perm_out[topk_perm(res_out, k, lt_fun, by)]
end