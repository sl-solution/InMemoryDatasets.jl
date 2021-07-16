function readcsvfile(path, types, n, header = true, dlm = ',')
    f = open(path, "r")
    res = [Vector{types[i]}(undef, n) for i in 1:length(types)]
    cnt = 1
    if header
        readline(f)
    end
    for l in eachline(f)
        cc = 1
        for j in 1:length(types)
            nd = findnext(dlm, l, cc)
            nd === nothing ? en = length(l)  : en = nd - 1
            types[j] <: AbstractString ? res[j][cnt] = l[cc:en] : res[j][cnt] = parse(types[j], l[cc:en])
            cc = en + 1 + length(dlm)
        end
        cnt += 1
        if cnt % 10000000 == 0
            @show cnt
        end
    end
    close(f)
    res
end
    
