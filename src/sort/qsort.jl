
midpoint(lo::T, hi::T) where T<:Integer = lo + ((hi - lo) >>> 0x01)
midpoint(lo::Integer, hi::Integer) = midpoint(promote(lo, hi)...)

const DEFAULT_UNSTABLE = QuickSort
# const DEFAULT_STABLE   = MergeSort
const SMALL_ALGORITHM  = InsertionSort
const SMALL_THRESHOLD  = 20

# Base on Julia sort

# Ordering in the following algorithms always is with by = identity
function ds_sort!(v, idx::Vector{<:Integer}, lo::Integer, hi::Integer, ::InsertionSortAlg, o::Ordering)
    @inbounds for i = lo+1:hi
        j = i
        x = v[i]
        y = idx[i]
        while j > lo
            if lt(o, x, v[j-1])
                v[j] = v[j-1]
                idx[j] = idx[j-1]
                j -= 1
                continue
            end
            break
        end
        v[j] = x
        idx[j] = y
    end
end


@inline function _selectpivot!(v, idx::Vector{<:Integer}, lo::Integer, hi::Integer, o::Ordering)
    @inbounds begin
        mi = midpoint(lo, hi)

        # sort v[mi] <= v[lo] <= v[hi] such that the pivot is immediately in place
        if lt(o, v[lo], v[mi])
            v[mi], v[lo] = v[lo], v[mi]
            idx[mi], idx[lo] = idx[lo], idx[mi]
        end

        if lt(o, v[hi], v[lo])
            if lt(o, v[hi], v[mi])
                v[hi], v[lo], v[mi] = v[lo], v[mi], v[hi]
                idx[hi], idx[lo], idx[mi] = idx[lo], idx[mi], idx[hi]
            else
                v[hi], v[lo] = v[lo], v[hi]
                idx[hi], idx[lo] = idx[lo], idx[hi]
            end
        end

        # return the location of pivot
        return lo
    end
end


function _partition!(v, idx::Vector{<:Integer}, lo::Integer, hi::Integer, o::Ordering)
    pv = _selectpivot!(v, idx, lo, hi, o)
    pivot = v[pv]
    pidx = idx[pv]
    # pivot == v[lo], v[hi] > pivot
    i, j = lo, hi
    @inbounds while true
        i += 1; j -= 1
        while lt(o, v[i], pivot); i += 1; end;
        while lt(o, pivot, v[j]); j -= 1; end;
        i >= j && break
        v[i], v[j] = v[j], v[i]
        idx[i], idx[j] = idx[j], idx[i]
    end
    v[j], v[lo] = pivot, v[j]
    idx[j], idx[lo] = pidx, idx[j]

    # v[j] == pivot
    # v[k] >= pivot for k > j
    # v[i] <= pivot for i < j
    return j
end

function ds_sort!(v, idx::Vector{<:Integer}, lo::Integer, hi::Integer, a::QuickSortAlg, o::Ordering)
    @inbounds while lo < hi
        hi-lo <= SMALL_THRESHOLD && return ds_sort!(v, idx, lo, hi, SMALL_ALGORITHM, o)
        j = _partition!(v, idx, lo, hi, o)
        if j-lo < hi-j
            # recurse on the smaller chunk
            # this is necessary to preserve O(log(n))
            # stack space in the worst case (rather than O(n))
            lo < (j-1) && ds_sort!(v, idx, lo, j-1, a, o)
            lo = j+1
        else
            j+1 < hi && ds_sort!(v, idx, j+1, hi, a, o)
            hi = j-1
        end
    end
end

# simple parallel implementation of QuickSort
# the assumption is that x[lo:mid] is sorted and x[mid+1:hi] is also sorted,
# the function uses this information to sort x[lo:hi]
# x_cpy is a copy of the x, idx_cpy is a copy of idx
function _sort_two_sorted_half!(x, x_cpy, idx::Vector{<:Integer}, idx_cpy, lo, mid, hi, o)
    st1 = lo
    en1 = mid
    st2 = mid+1
    en2 = hi
    cnt = lo
    while true
        if lt(o, x_cpy[st1], x_cpy[st2])
            x[cnt] = x_cpy[st1]
            idx[cnt] = idx_cpy[st1]
            st1 += 1
            cnt += 1
            st1 > en1 && break
        else
            x[cnt] = x_cpy[st2]
            idx[cnt] = idx_cpy[st2]
            st2 += 1
            cnt += 1
            st2 > en2 && break
        end
    end
    if st1 > en1
        while cnt <= hi
            x[cnt] = x_cpy[st2]
            idx[cnt] = idx_cpy[st2]
            st2 += 1
            cnt += 1
        end
    elseif st2 > en2
        while cnt <= hi
            x[cnt] = x_cpy[st1]
            idx[cnt] = idx_cpy[st1]
            st1 += 1
            cnt += 1
        end
    end
end

# to simplify the problem we assume number_of_chunks is 2^n for some n
function _sort_chunks!(x, idx::Vector{<:Integer}, number_of_chunks, a::QuickSortAlg, o::Ordering)
    cz = div(length(x), number_of_chunks)
    en = length(x)
    Threads.@threads for i in 1:number_of_chunks
        ds_sort!(x, idx, (i-1)*cz+1, i*cz, a, o)
    end
    # take care of the last few observations
    if number_of_chunks*div(length(x), number_of_chunks) < en
        ds_sort!(x, idx, number_of_chunks*div(length(x), number_of_chunks)+1, en, a, o)
    end
end

function _sort_multi_sorted_chunk!(x, idx::Vector{<:Integer}, number_of_chunks, a::QuickSortAlg, o::Ordering)
    cz = div(length(x), number_of_chunks)
    en = length(x)
    current_numberof_chunks = number_of_chunks
    x_cpy = copy(x)
    idx_cpy = copy(idx)
    while true
        Threads.@threads for i in 1:2:current_numberof_chunks
            _sort_two_sorted_half!(x, x_cpy, idx, idx_cpy, (i-1)*cz+1, i*cz, (i+1)*cz, o)
        end
        cz *= 2
        current_numberof_chunks = current_numberof_chunks  >> 1
        current_numberof_chunks < 2 && break
        copy!(x_cpy, x)
        copy!(idx_cpy, idx)
    end
    # take care of the last few (less than number_of_chunks) observations
    if number_of_chunks*div(length(x), number_of_chunks) < en
        copy!(x_cpy, x)
        copy!(idx_cpy, idx)
        _sort_two_sorted_half!(x, x_cpy, idx, idx_cpy, 1, number_of_chunks*div(length(x), number_of_chunks), en, o)
    end
end

# sorting a vector using parallel quick sort
# it uses a simple algorithm for doing this, and to make it even simpler the number of threads must be in the form of 2^n
function hp_ds_sort!(x, idx, a::QuickSortAlg, o::Ordering)
    cpucnt = Threads.nthreads()
    @assert cpucnt >= 2 "we need at least 2 cpus for parallel sorting"
    cpucnt = 2 ^ floor(Int, log2(cpucnt))
    _sort_chunks!(x , idx, cpucnt, a, o)
    _sort_multi_sorted_chunk!(x, idx, cpucnt, a, o)
end
