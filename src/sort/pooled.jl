# x contains the refs
# P is the index of permutation, where and counts are integers vector of size ngroups + 1, perm is the permutaion of sorted pool, iperm is the inverse of perm
function _group_indexer!(x::Vector, original_P, copy_P, where, counts, lo, hi, ngroups::Integer, perm, iperm)
    # from PooledArrays.jl
    for i in 1:ngroups+1
        where[i] = 0
    end

    @inbounds for i = lo:hi
        where[x[i] + 1] += 1
    end
    counts[1] = 0
    for i in 1:ngroups
        counts[i+1] = where[perm[i] + 1]
    end
    # mark the start of each contiguous group of like-indexed data
    where[1] = 1
    @inbounds for i = 2:ngroups+1
        where[i] = where[i - 1] + counts[i - 1]
    end

    # this is our indexer
    @inbounds for i = lo:hi
        label = iperm[x[i]] + 1
        original_P[where[label] + lo - 1] = copy_P[i]
        where[label] += 1
    end

    # rearrange data
    @inbounds for i in 1:ngroups
        where[i] = 0
    end

    @inbounds for i = lo:hi
        where[x[i]] += 1
    end
    for i in 1:ngroups
        counts[i] = where[perm[i]]
    end

    f_indx = lo
    @inbounds for i = 1:ngroups
        l_indx = f_indx + counts[i] - 1
        val = perm[i]
        for j = f_indx:l_indx
            x[j] = val
        end
        f_indx = l_indx + 1
    end
end
