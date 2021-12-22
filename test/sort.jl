using InMemoryDatasets, PooledArrays, Random, Test, CategoricalArrays

@testset "Basic usage" begin
    ds = Dataset(x1 = rand(1:1000,100))
    dss = sort(ds, 1)
    @test issorted(dss.x1)
    dss = sort(ds, 1, rev = true)
    @test issorted(dss.x1, rev = true)
    setformat!(ds, 1=>isodd)
    dss = sort(ds, 1)
    @test issorted(dss.x1, by = isodd)
    dss = sort(ds, 1, rev = true)
    @test issorted(dss.x1, by = isodd, rev = true)
    dss = sort(ds, 1, rev = true, mapformats = false)
    @test issorted(dss.x1, rev = true)
    ds = Dataset(x1 = rand(3333)*100)
    dss = sort(ds, 1)
    @test issorted(dss.x1)
    dss = sort(ds, 1, rev = true)
    @test issorted(dss.x1, rev = true)
    setformat!(ds, 1=>round)
    dss = sort(ds, 1)
    @test issorted(dss.x1, by = round)
    dss = sort(ds, 1, rev = true)
    @test issorted(dss.x1, by = round, rev = true)
    dss = sort(ds, 1, rev = true, mapformats = false)
    @test issorted(dss.x1, rev = true)
    x1 = Vector{Int}(undef, 5050)
    y1 = Vector{Float64}(undef, 5050)
    cnt = 1
    for i in 1:100
        for j in 1:i
            x1[cnt] = i
            y1[cnt] = j + rand()
            cnt += 1
        end
    end
    shuffle_row = shuffle(1:5050)
    ds = Dataset(x = x1[shuffle_row], y = y1[shuffle_row])
    sort!(ds, 1:2)
    @test ds.x == x1
    @test ds.y == y1
    sort!(ds,2)
    @test issorted(ds.y)
    ds2 = ds[shuffle_row, :]
    sort!(ds2, 1)
    ds3 = sort(ds2, 1:2)
    @test ds3 == sort(ds, :)
    ds4 = sort(ds, 1:2, rev = true)
    @test ds4.x == reverse(x1)
    @test ds4.y == reverse(y1)
    ds = Dataset(Float64.(rand(1:3, 100, 4)), :auto)
    map!(ds, x->rand()<.3 ? missing : x, :)
    ds2 = copy(ds)
    for j in ncol(ds2):-1:1
        r = sortperm(ds2[!, j].val)
        for i in 1:ncol(ds2)
            ds2[!, i] = ds2[r, i]
        end
    end
    sort!(ds, 1:2)
    @test ds.x1 == ds2.x1
    @test ds.x2 == ds2.x2
    @test sort(ds, 1:4) == ds2

    x1 = rand(1:3, 777)
    x2 = rand(Date(100):Day(1):Date(101), 777)
    x3 = PooledArray(rand([missing, 1.1, 20000.0, 123.0], 777))
    x4 = [randstring(2) for _ in 1:777]
    x5 = rand(1:2, 777)
    ds = Dataset(col1 = x3, col2 = x2, col3 = x1, col4 = x5, col5 = x4)

    ds2 = copy(ds)
    for j in ncol(ds2):-1:1
        r = sortperm(ds2[!, j].val)
        for i in 1:ncol(ds2)
            ds2[!, i] = ds2[r, i]
        end
    end
    @test sort(ds, 1:5) == ds2
    sort!(ds, :)

    @test sort(ds,1, stable = false).col1 == ds2.col1
    sort!(ds, 5)
    @test issorted(ds.col5)
    @test issorted(sort(ds, 5, rev = true).col5, rev = true)


    dv1 = [9, 1, 8, missing, 3, 3, 7, missing]
    dv2 = [9, 1, 8, missing, 3, 3, 7, missing]
    dv3 = Vector{Union{Int, Missing}}(1:8)
    cv1 = CategoricalArray(dv1, ordered=true)

    d = Dataset(dv1 = dv1, dv2 = dv2, dv3 = dv3, cv1 = cv1)

    @test sortperm(d, :) == sortperm(dv1)
    @test sortperm(d[:, [:dv3, :dv1]], :) == sortperm(dv3)
    @test sort(d, :dv1)[!, :dv3] == sort(d, "dv1")[!, "dv3"] == sortperm(dv1)
    @test sort(d, :dv2)[!, :dv3] == sortperm(dv1)
    @test sort(d, :cv1)[!, :dv3] == sortperm(dv1)
    @test sort(d, [:dv1, :cv1])[!, :dv3] == sortperm(dv1)
    @test sort(d, [:dv1, :dv3])[!, :dv3] == sortperm(dv1)


    x = CategoricalArray{Union{Characters{6, UInt8}, String}}(["Old", "Young", "Middle", "Young"])
    levels!(x, ["Young", "Middle", "Old"])
    ds = Dataset(x = x)
    ds_s = sort(ds, :x)
    @test ds_s.x == ["Young", "Young", "Middle", "Old"]
    ordered!(x, true)
    ds = Dataset(x = x)
    ds_s = sort(ds, :x)
    @test ds_s.x == ["Young", "Young", "Middle", "Old"]
    a = rand(1:3, 1000)
    c = rand(1:10, 1000)
    f = rand(1:2, 1000)
    pc = PooledArray(c)
    pa = PooledArray(a)
    pf = categorical(f)
    ds = Dataset(a = a, c = c, f = f)
    ds_pa = Dataset(a = pa, c = pc, f = pf)
    ds2 = copy(ds)
    for j in ncol(ds2):-1:1
        r = sortperm(ds2[!, j].val)
        for i in 1:ncol(ds2)
            ds2[!, i] = ds2[r, i]
        end
    end
    for i in 1:20
        @test sort(ds, :) == sort(ds_pa, :) == ds2
    end
    x1 = -rand(1:1000, 5000)
    x2 = -rand(1:100, 5000)
    dsl = Dataset(x1 = Characters{6, UInt8}.(c"id" .* string.(-x1)), x2 = Characters{5, UInt8}.(c"id" .* string.(-x2)))
    dsr = Dataset(x1 = x1, x2 = x2)
    for i in 1:2
        dsl[!, i] = PooledArray(dsl[!, i])
        dsr[!, i] = PooledArray(dsr[!, i])
    end
    fmtfun3(x) = @views -parse(Int, x[3:end])
    setformat!(dsl, 1:2=>fmtfun3)
    @test sortperm(dsl, :)==sortperm(dsr, :)
    @test sortperm(dsl, :, rev=true)==sortperm(dsr, :, rev=true)
end

@testset "_find_starts_of_groups" begin
    # Suppose there will be at least one row and one column.
    ds = Dataset(x1 = [1])
    colsidx = [1]
    T = nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64)
    ranges = [1]
    last_valid_index = 1
    r1, r2, r3 = IMD._find_starts_of_groups(ds, colsidx, T)
    @test r1 == colsidx
    @test r3 == last_valid_index
    @test r2 == ranges[1:r3]

    ds = Dataset(x1 = [1,1,1,3,3,1,1])
    colsidx = [1]
    T = nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64)
    ranges = [1, 4, 6]
    last_valid_index = 3
    r1, r2, r3 = IMD._find_starts_of_groups(ds, colsidx, T)
    @test r1 == colsidx
    @test r3 == last_valid_index
    @test r2[1:r3] == ranges

    ds = Dataset(x1 = [1], x2 = [1], x3 = [2], x4 = [4])
    colsidx = 1:3
    T = nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64)
    ranges = [1]
    last_valid_index = 1
    r1, r2, r3 = IMD._find_starts_of_groups(ds, colsidx, T)
    @test r1 == colsidx
    @test r3 == last_valid_index
    @test r2[1:r3] == ranges

    # Not only for Dataset, but also GroupBy.
    ds = Dataset(x1 = [1, 1, 1], x2 = [1, 2, 1])
    gb = groupby(ds, 2)
    colsidx = [2]
    T = nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64)
    ranges = [1, 3]
    last_valid_index = 2
    r1, r2, r3 = IMD._find_starts_of_groups(gb, colsidx, T)
    @test r1 == colsidx
    @test r3 == last_valid_index
    @test r2[1:r3] == ranges

    # Use formats for some columns.
    format1(x) = isodd(x)
    ds = Dataset(x1 = [1, 1, 1, 1, 3, 3, 3], x2 = [1, 6, 5, 5, 5, 5, 2], x3 = [2, 2, 4, 4, 4, 4, 1], x4 = [4, 1, 1, 4, 4, 4, 1])
    setformat!(ds, [1, 3, 4] => format1)
    colsidx = 1:3
    T = nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64)
    ranges = [1, 2, 3, 5, 7]
    last_valid_index = 5
    r1, r2, r3 = IMD._find_starts_of_groups(ds, colsidx, T, mapformats = false) # Do not use formatted values.
    @test r1 == colsidx
    @test r3 == last_valid_index
    @test r2[1:r3] == ranges

    ranges = [1, 2, 3, 7]
    last_valid_index = 4
    r1, r2, r3 = IMD._find_starts_of_groups(ds, colsidx, T) # Use formatted values.
    @test r1 == colsidx
    @test r3 == last_valid_index
    @test r2[1:r3] == ranges

    # Consider whether ds is sorted using formatted values.
    # If ds is sorted using formatted values.
    format2(x) = abs(2.5 - x)
    ds = Dataset(x1 = [1, 1, 1, 1, 3, 3, 3], x2 = [1, 6, 5, 5, 5, 5, 2], x3 = [2, 1, 4, 4, 4, 4, 1], x4 = [4, 1, 1, 4, 4, 4, 1])
    setformat!(ds, 3:4 => format2)
    dss = sort(ds, 3, mapformats = true) # Use formatted values for sorting.
    colsidx = [1, 3, 4]
    T = nrow(dss) < typemax(Int32) ? Val(Int32) : Val(Int64)
    ranges = [1, 2, 3, 4, 5, 7]
    last_valid_index = 6
    r1, r2, r3 = IMD._find_starts_of_groups(dss, colsidx, T, mapformats = false) # Do not use formatted values.
    @test r1 == colsidx
    @test r3 == last_valid_index
    @test r2[1:r3] == ranges

    ranges = [1, 2, 5]
    last_valid_index = 3
    r1, r2, r3 = IMD._find_starts_of_groups(dss, colsidx, T) # Use formatted values.
    @test r1 == colsidx
    @test r3 == last_valid_index
    @test r2[1:r3] == ranges

    # If ds is sorted using unformatted values.
    ds = Dataset(x1 = [1, 1, 1, 1, 3, 3, 3], x2 = [1, 6, 5, 5, 5, 5, 2], x3 = [2, 1, 4, 4, 4, 4, 1], x4 = [4, 1, 1, 4, 4, 4, 1])
    setformat!(ds, 3:4 => format2)
    dss = sort(ds, 3, mapformats = false) # Use unformatted values for sorting.
    colsidx = [1, 3, 4]
    T = nrow(dss) < typemax(Int32) ? Val(Int32) : Val(Int64)
    ranges = [1, 2, 3, 4, 5, 6]
    last_valid_index = 6
    r1, r2, r3 = IMD._find_starts_of_groups(dss, colsidx, T, mapformats = false) # Do not use formatted values.
    @test r1 == colsidx
    @test r3 == last_valid_index
    @test r2[1:r3] == ranges

    ranges = [1, 2, 3, 4, 6]
    last_valid_index = 5
    r1, r2, r3 = IMD._find_starts_of_groups(dss, colsidx, T) # Use formatted values.
    @test r1 == colsidx
    @test r3 == last_valid_index
    @test r2[1:r3] == ranges

    # Consider ds with multiple types.
    c1 = PooledArray(["string", "string", 1.1, 1.1, 1.1, 20000.0, 123.0])
    c2 = PooledArray(["string", missing, 1.1, 1.1, 'a', 'a', 'b'])
    c3 = PooledArray([missing, missing, missing, 1.1, 1.1, 20000.0, 123.0])
    c4 = CategoricalArray{Union{Characters{6, UInt8}, String}}(["Old", "Young", "Young", "Young", "Old", "Young", "Middle"])
    levels!(c4, ["Young", "Middle", "Old"])
    ds = Dataset(x1 = c1, x2 = c2, x3 = c3, x4 = c4)
    colsidx = [1, 2]
    T = nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64)
    ranges = [1, 2, 3, 5, 6, 7]
    last_valid_index = 6
    r1, r2, r3 = IMD._find_starts_of_groups(ds, colsidx, T)
    @test r1 == colsidx
    @test r3 == last_valid_index
    @test r2[1:r3] == ranges

    # Simple functions to test large data set.
    function _zero_or_one(ds, colsidx)
        re = zeros(Bool, nrow(ds))
        re[1] = true
        re[2:end] = byrow(ds[2:end, colsidx] .!== ds[1:(end-1), colsidx], any)
        re
    end

    function _get_starts(ds, colsidx, ::Val{T}) where T
        zero_or_one = _zero_or_one(ds, colsidx)
        cols = IMD.index(ds)[colsidx]
        last_valid_index = 1
        ranges = Vector{T}(undef, length(zero_or_one))
        @inbounds for i in 1:length(zero_or_one)
            if zero_or_one[i] == true
                ranges[last_valid_index] = i
                last_valid_index += 1
            end
        end
        return cols, ranges, (last_valid_index - 1)
    end

    # Test for large data set with few levels.
    c1 = rand(1:3, 10^6)
    c2 = PooledArray(rand([missing, 1.1, 20000.0, 123.0], 10^6))
    c3 = PooledArray(rand([missing, 1.1, 20000.0, 123.0], 10^6))
    c4 = PooledArray(rand([missing, 1.1, 20000.0, 123.0], 10^6))
    c5 = rand(1:8, 10^6)
    ds = Dataset(x1 = c1, x2 = c2, x3 = c3, x4 = c4, x5 = c5)
    colsidx = [1, 2, 4, 5]
    T = nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64)
    ra1, ra2, ra3 = IMD._find_starts_of_groups(ds, colsidx, T)
    rb1, rb2, rb3 = _get_starts(ds, colsidx, T)
    @test ra1 == rb1
    @test ra3 == rb3
    @test ra2[1:ra3] == rb2[1:rb3]

    # Test for large data set with many levels.
    c1 = rand(1:3, 10^6)
    c2 = rand(Date(100):Day(1):Date(101), 10^6)
    c3 = PooledArray(rand([missing, 1.1, 20000.0, 123.0], 10^6))
    c4 = [randstring(2) for _ in 1:10^6]
    c5 = rand(1:2, 10^6)
    ds = Dataset(x1 = c1, x2 = c2, x3 = c3, x4 = c4, x5 = c5)
    colsidx = [1, 2, 4, 5]
    T = nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64)
    ra1, ra2, ra3 = IMD._find_starts_of_groups(ds, colsidx, T)
    rb1, rb2, rb3 = _get_starts(ds, colsidx, T)
    @test ra1 == rb1
    @test ra3 == rb3
    @test ra2[1:ra3] == rb2[1:rb3]

end

@testset "bigInt test set" begin
    ds = Dataset(x = big.([1,4,-1,1,100]), x2 = [45,3,98,100,10])
    @test sortperm(ds, 1) == [3,1,4,2,5]
    @test sortperm(ds, 1, rev = true) == [5,2,1,4,3]
    @test sortperm(ds, 1:2) == [3, 1, 4, 2, 5]
    @test sortperm(ds, 1:2, rev = [false, true]) == [3,4,1,2,5]
    ds[2,1]=missing
    @test sortperm(ds, 1) == [3,1,4,5,2]
    @test sortperm(ds, 1, rev = true) == [2,5,1,4,3]

    x = rand(big(1):big(100), 10000)
    y = rand(1:200, 10000)
    ds = Dataset(x = x, y = y)
    @test sortperm(ds, 1) == sortperm(x)
    @test sortperm(ds, 1, rev=true) == sortperm(x, rev = true)

    x = rand(Int128, 1000)
    y = rand(1:100, 1000)
    ds = Dataset(x = x, y = y)
    @test sortperm(ds, 1) == sortperm(x)
    @test sortperm(ds, 1, rev = true) == sortperm(x, rev = true)
    setformat!(ds, 1=>isodd)
    @test sortperm(ds, 1) == sortperm(x, by = isodd)
    @test sortperm(ds, 1, rev = true) == sortperm(x, by = isodd, rev = true)

    ds = Dataset(x = [0xfffffffffffffff3, 0xfffffffffffffff2, 0xfffffffffffffff4, 0xfffffffffffffff1], y = [1,1,2,2])
    @test sort(ds,1) == ds[[4,2,1,3],:]
    @test sort(ds,1, rev = true) == ds[[3,1,2,4],:]
    setformat!(ds, 1=>isodd)
    @test sort(ds,1) == ds[[2,3,1,4],:]
    @test sort(ds,1, rev=true) == ds[[1,4,2,3],:]
    @test sort(ds, 1:2) == ds[[2,3,1,4], :]
    @test sort(ds, 1:2, rev = [false, true]) == ds[[3,2,4,1], :]
end

@testset "sort views" begin
    ds = Dataset(x = [missing, 1.0, 5.0, 3.2, missing], y = [2, 1, 7, 4, 1])
    sds = view(ds, [2,1,2,2,2,1,3,4,5], [2,1])
    @test sort(ds, 1) == sort(ds[!, [1,2]], 1)
    @test sort(ds, 1) == sort(ds[!, [2,1]], 2)[:, [2,1]]
    @test sortperm(sds, 2) == [1,3,4,5,8, 7, 2,6,9]
    @test sortperm(sds, [2,1]) == [1,3,4,5,8,7, 9, 2,6]
    if !Base.Sys.iswindows()
        for i = 1:100
            ds = Dataset(rand(1:10, 1000, 3), :auto)
            @test sort(ds, :) == sort(ds[!, 1:3], :)
            ds = Dataset(rand(1:1000000, 1000, 3), :auto)
            @test sort(ds, :) == sort(ds[!, 1:3], :)
            ds = Dataset(rand(1000, 2), :auto)
            @test sort(ds, :) == sort(ds[!, 1:2], :)
        end
    end
end

@testset "issorted/issorted!" begin
    dv1 = [9, 1, 8, missing, 3, 3, 7, missing]
    dv2 = [9, 1, 8, missing, 3, 3, 7, missing]
    dv3 = Vector{Union{Int, Missing}}(1:8)
    cv1 = CategoricalArray(dv1, ordered=true)

    d = Dataset(dv1=dv1, dv2=dv2, dv3=dv3, cv1=cv1)

    @test !issorted(d, :cv1)
    @test issorted(d, :dv3)
    @test !issorted(d, :dv1)

    dv1 = [1,3,3,7,8,9, missing, missing]
    dv2 = [9, 1, 8, missing, 3, 3, 7, missing]
    dv3 = Vector{Union{Int, Missing}}(1:8)
    cv1 = CategoricalArray(dv1, ordered=true)

    d = Dataset(dv1=dv1, dv2=dv2, dv3=dv3, cv1=cv1)
    @test issorted(d, :cv1)
    @test issorted(d, :dv1)
    @test !issorted(d, :dv2)

    ds = Dataset(x = [0xfffffffffffffff3, 0xfffffffffffffff2, 0xfffffffffffffff4, 0xfffffffffffffff1], y = [1,1,2,2])
    @test issorted(ds[[4,2,1,3],:],1)
    @test issorted(view(ds, [4,2,1,3], :), 1)
    @test issorted(ds[[3,1,2,4],:],1, rev = true)
    setformat!(ds, 1=>isodd)
    @test issorted(ds[[2,3,1,4],:],1)
    @test issorted(view(ds, [2,3,1,4], :), 1)
    @test issorted(ds[[1,4,2,3],:],1, rev=true)
    @test issorted(ds[[2,3,1,4], :], 1:2)
    @test issorted(view(ds, [2,3,1,4], :), 1:2)
    @test issorted(ds[[3,2,4,1], :], 1:2, rev = [false, true])
    @test issorted(view(ds, [3,2,4,1], :), 1:2, rev = [false, true])


    x = rand(Int128, 1000)
    y = rand(1:100, 1000)
    ds = Dataset(x = x, y = y)
    @test issorted(sort(ds, 1),1)
    @test issorted(sort(ds, 1, rev = true), 1, rev=true)
    setformat!(ds, 1=>isodd)
    @test issorted(sort(ds, 1),1)
    @test issorted(sort(ds, 1, rev = true), 1, rev = true)

    ds = Dataset(x = big.([1,4,-1,1,100]), x2 = [45,3,98,100,10])
    @test !issorted(ds, 1)
    @test issorted(ds[[3,1,4,2,5], 1:1], 1)
    @test issorted(view(ds, [5,2,1,4,3], [2,1]), 2, rev = true)
    @test issorted(ds[[3, 1, 4, 2, 5], :], 1:2)
    @test issorted(ds[[3,4,1,2,5],:], 1:2, rev = [false, true])
    ds[2,1]=missing
    @test !issorted(ds, 1)
    @test issorted(ds[[3,1,4,5,2], :], 1)
    @test issorted(view(ds, [2,5,1,4,3], :), 1, rev = true)
    if !Base.Sys.iswindows()
        for i in 1:100
            ds = Dataset(rand(1:10, 1000, 10), :auto)
            for j in 1:10
                @test issorted(sort(ds, 1:j), 1:j)
                @test issorted(sort(ds, 1:j, rev = true), 1:j, rev = true)
                setformat!(ds, 1:10=>isodd)
                @test issorted(sort(ds, 1:j), 1:j)
                @test issorted(sort(ds, 1:j, rev = true), 1:j, rev = true)
            end
            ds = Dataset(rand(1:10., 1000, 10), :auto)
            map!(ds, x->rand()<.1 ? missing : x, :)
            for j in 1:10
                @test issorted(sort(ds, 1:j), 1:j)
                @test issorted(sort(ds, 1:j, rev = true), 1:j, rev = true)
                setformat!(ds, 1:10=>sign)
                @test issorted(sort(ds, 1:j), 1:j)
                @test issorted(sort(ds, 1:j, rev = true), 1:j, rev = true)
            end
            ds = Dataset(rand(1:10., 1000, 10), :auto)
            map!(ds, x->rand()<.1 ? missing : x, :)
            for j in 1:10
                ds[!, j] = PooledArray(ds[!, j])
            end
            for j in 1:10
                @test issorted(sort(ds, 1:j), 1:j)
                @test issorted(sort(ds, 1:j, rev = true), 1:j, rev = true)
                setformat!(ds, 1:10=>sign)
                @test issorted(sort(ds, 1:j), 1:j)
                @test issorted(sort(ds, 1:j, rev = true), 1:j, rev = true)
            end
        end
        for i in 1:100
            ds = Dataset(rand(1:10, 1000, 10), :auto)
            for j in 1:10
                sort!(ds, 1:j)
                issorted!(ds, 1:j)
                @test IMD._sortedcols(ds) == 1:j
                @test issorted(ds, 1:j)

                setformat!(ds, 1:10=>isodd)
                sort!(ds, 1:j, rev = true)
                issorted!(ds, 1:j, rev = true)
                @test IMD._sortedcols(ds) == 1:j
                @test issorted(ds, 1:j, rev = true)
            end
            ds = Dataset(rand(1:10., 1000, 10), :auto)
            map!(ds, x->rand()<.1 ? missing : x, :)
            for j in 1:10
                sort!(ds, 1:2:j)
                issorted!(ds, 1:2:j)
                @test IMD._sortedcols(ds) == collect(1:2:j)
                @test issorted(ds, 1:2:j)

                setformat!(ds, 1:10=>sign)
                sort!(ds, 1:2:j, rev = true)
                issorted!(ds, 1:2:j, rev = true)
                @test IMD._sortedcols(ds) == collect(1:2:j)
                @test issorted(ds, 1:2:j, rev = true)
            end
            ds = Dataset(rand(1:10., 1000, 10), :auto)
            map!(ds, x->rand()<.1 ? missing : x, :)
            for j in 1:10
                ds[!, j] = PooledArray(ds[!, j])
            end
            for j in 1:10
                sort!(ds, 1:2:j)
                issorted!(ds, 1:2:j)
                @test IMD._sortedcols(ds) == collect(1:2:j)
                @test issorted(ds, 1:2:j)

                setformat!(ds, 1:10=>sign)
                sort!(ds, 1:2:j, rev = true)
                issorted!(ds, 1:2:j, rev = true)
                @test IMD._sortedcols(ds) == collect(1:2:j)
                @test issorted(ds, 1:2:j, rev = true)
            end
        end
    end

end
