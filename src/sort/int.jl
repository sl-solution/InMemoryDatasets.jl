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
