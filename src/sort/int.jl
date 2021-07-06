# missings go to the end
# fast algorithm for integers with few unique values forward direction
function ds_sort_int_missatright!(x, original_P, copy_P, where, lo, hi, rangelen, minval)
    offs = 1 - minval

    @inbounds for i in 1:rangelen+2
        where[i] = 0
    end

    # where = fill(0, rangelen+1)
    where[1] = 1
    @inbounds for i = lo:hi
        ismissing(x[i]) ? where[rangelen+2] += 1 : where[Int(x[i]) + offs + 1] += 1
    end

    #cumsum!(where, where)
    @inbounds for i = 2:rangelen+2
        where[i] += where[i-1]
    end

    @inbounds for i = lo:hi
        ismissing(x[i]) ? label = rangelen + 1 : label = Int(x[i]) + offs
        original_P[where[label] + lo - 1] = copy_P[i]
        where[label] += 1
    end
    # rearrange data
    @inbounds for i in 1:rangelen+1
        where[i] = 0
    end

    @inbounds for i = lo:hi
        ismissing(x[i]) ? where[rangelen+1] += 1 : where[Int(x[i]) + offs] += 1
        # where[x[i] + offs] += 1
    end
    f_indx = lo
    @inbounds for i = 1:rangelen
        l_indx = f_indx + where[i] - 1
        val = i-offs
        for j = f_indx:l_indx
            x[j] = val
        end
        f_indx = l_indx + 1
    end
    if f_indx <= hi
        for j = f_indx:hi
            x[j] = missing
        end
    end
end
function ds_sort_int_missatleft!(x, original_P, copy_P, where, lo, hi, rangelen, minval)
    offs = 1 - minval

    @inbounds for i in 1:rangelen+2
        where[i] = 0
    end

    # where = fill(0, rangelen+1)
    where[1] = 1
    @inbounds for i = lo:hi
        ismissing(x[i]) ? where[2] += 1 : where[Int(x[i]) + offs + 2] += 1
    end

    #cumsum!(where, where)
    @inbounds for i = 2:rangelen+2
        where[i] += where[i-1]
    end

    @inbounds for i = lo:hi
        ismissing(x[i]) ? label = 1 : label = Int(x[i]) + offs + 1
        original_P[where[label] + lo - 1] = copy_P[i]
        where[label] += 1
    end
    # rearrange data
    @inbounds for i in 1:rangelen+1
        where[i] = 0
    end

    @inbounds for i = lo:hi
        ismissing(x[i]) ? where[1] += 1 : where[Int(x[i]) + offs + 1] += 1
        # where[x[i] + offs] += 1
    end
    f_indx = lo + where[1]
    @inbounds for i = 2:rangelen+1
        l_indx = f_indx + where[i] - 1
        val = i - 1 - offs
        for j = f_indx:l_indx
            x[j] = val
        end
        f_indx = l_indx + 1
    end
    for j = lo:(lo + where[1] - 1)
        x[j] = missing
    end
end


# to simplify the problem we assume number_of_chunks is 2^n for some n
function _sort_chunks_int_right!(x, idx::Vector{<:Integer}, idx_cpy, where, number_of_chunks, rangelen, minval, o::Ordering)
    cz = div(length(x), number_of_chunks)
    en = length(x)
    Threads.@threads for i in 1:number_of_chunks
        ds_sort_int_missatright!(x, idx, idx_cpy, where[Threads.threadid()], (i-1)*cz+1,i*cz, rangelen, minval)
    end
    # take care of the last few observations
    if number_of_chunks*div(length(x), number_of_chunks) < en
        ds_sort_int_missatright!(x, idx, idx_cpy, where[Threads.threadid()],  number_of_chunks*div(length(x), number_of_chunks)+1, en, rangelen, minval)
    end
end

# to simplify the problem we assume number_of_chunks is 2^n for some n
function _sort_chunks_int_left!(x, idx::Vector{<:Integer}, idx_cpy, where, number_of_chunks, rangelen, minval, o::Ordering)
    cz = div(length(x), number_of_chunks)
    en = length(x)
    Threads.@threads for i in 1:number_of_chunks
        ds_sort_int_missatleft!(x, idx, idx_cpy, where[Threads.threadid()], (i-1)*cz+1,i*cz, rangelen, minval)
    end
    # take care of the last few observations
    if number_of_chunks*div(length(x), number_of_chunks) < en
        ds_sort_int_missatleft!(x, idx, idx_cpy, where[Threads.threadid()],  number_of_chunks*div(length(x), number_of_chunks)+1, en, rangelen, minval)
    end
end


# sorting a vector using parallel quick sort
# it uses a simple algorithm for doing this, and to make it even simpler the number of threads must be in the form of 2^n
function hp_ds_sort_int!(x, idx, idx_cpy, where, rangelen, minval, missatleft, a::QuickSortAlg, o::Ordering)
    cpucnt = Threads.nthreads()
    @assert cpucnt >= 2 "we need at least 2 cpus for parallel sorting"
    cpucnt = 2 ^ floor(Int, log2(cpucnt))
    if missatleft
        _sort_chunks_int_left!(x , idx, idx_cpy, where, cpucnt, rangelen, minval, o)
    else
        _sort_chunks_int_right!(x , idx, idx_cpy, where, cpucnt, rangelen, minval, o)
    end
    _sort_multi_sorted_chunk!(x, idx, cpucnt, a, o)
end
