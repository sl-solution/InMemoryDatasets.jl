using .Base.Order
import Base.Sort.QuickSortAlg
import Base.Sort.InsertionSortAlg
import Base.Sort.MergeSort
import Base.Order.lt

using .Base: sub_with_overflow, add_with_overflow

trunc2int(x) = unsafe_trunc(Int, x)
trunc2int(::Missing) = missing
_is_intable(x) = (typemin(Int) <= x <= typemax(Int)) && (round(x, RoundToZero) == x)
_is_intable(::Missing) = true
# x is sorted based on o
function _fill_starts!(ranges, x, rangescpy, last_valid_range, o::Ordering, ::Val{T}) where T

    cnt = 1
    st = 1
    lo::T = 0
    hi::T = 0
    @inbounds for j in 1:last_valid_range
        lo = rangescpy[j]
        j == last_valid_range ? hi = length(x) : hi = rangescpy[j+1] - 1
        cnt = _find_blocks_sorted!(ranges, x, lo, hi, cnt, o, Val(T))
    end
    @inbounds for j in 1:(cnt - 1)
        rangescpy[j] = ranges[j]
    end
    return cnt - 1
end

function _find_blocks_sorted!(ranges, x, lo, hi, cnt, o::Ordering, ::Val{T}) where T
    n = hi - lo + 1
    counter = 0
    st::T = lo
    ranges[cnt] = st
    cnt += 1
    @inbounds while true
        stopval::T = searchsortedlast(x, x[st], st, hi, o)
        # # the last obs in the current group
        st = stopval + 1
        st > hi && return cnt
        ranges[cnt] = st
        cnt += 1
        counter += 1
        #
        # # if too many levels switch strategy
        # #TODO supplied by should be take into account
        # # the decision is to always assume by = identity
        if counter > div(n, 2)
            # ranges[cnt] = st
            # cnt += 1
            for i in st:hi - 1
                if !isequal(x[i], x[i+1])
                # if lt(o, x[i], x[i+1])
                    ranges[cnt] = i + 1
                    cnt += 1
                end
            end
            return cnt
        end
    end
end
