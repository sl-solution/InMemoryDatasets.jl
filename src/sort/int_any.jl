
## EXPERIMENTAL
_b_ints = (511, 261632, 133955584, 68585259008, 35115652612096, 17979214137393152, 9205357638345293824) # for the last one the first bit is 0 (signbit)

@inline FLIPSIGN(x, y) = flipsign(x,y)
@inline FLIPSIGN(::Missing, ::Missing) = missing
@inline SHIFT(x,n) = x >> n
@inline SHIFT(::Missing, n) = missing

Base.@propagate_inbounds function _convert_for_countsort!(conv_x, x, bid, _b_ints, lo, hi)
    shift_val = (bid-1)*9
    for i in lo:hi
        conv_x[i] = SHIFT(abs(x[i]) & _b_ints[bid] , shift_val)
    end
    for i in lo:hi
        conv_x[i] = FLIPSIGN(conv_x[i], sign(x[i]))
    end
end

function _radixlike_sort_int_missatright!(x, conv_x, idx, cpy_idx, _b_ints, lo, hi)
    for j in 1:7
        cpy_idx .= idx
        _convert_for_countsort!(conv_x, x, j, _b_ints, lo, hi)
        _ds_sort_int_missatright_nopermx!(conv_x[idx], idx, cpy_idx, lo, hi, 1023, -512, Val(Int32))
    end
end
