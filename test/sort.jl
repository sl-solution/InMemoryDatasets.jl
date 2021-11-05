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
    r1, r2, r3 = _find_starts_of_groups(ds, colsidx, T)
    @test r1 == colsidx
    @test r3 == last_valid_index
    @test r2 == ranges[1:r3]

    ds = Dataset(x1 = [1,1,1,3,3,1,1])
    colsidx = [1]
    T = nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64)
    ranges = [1, 4, 6]
    last_valid_index = 3
    r1, r2, r3 = _find_starts_of_groups(ds, colsidx, T)
    @test r1 == colsidx
    @test r3 == last_valid_index
    @test r2[1:r3] == ranges

    ds = Dataset(x1 = [1], x2 = [1], x3 = [2], x4 = [4])
    colsidx = 1:3
    T = nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64)
    ranges = [1]
    last_valid_index = 1
    r1, r2, r3 = _find_starts_of_groups(ds, colsidx, T)
    @test r1 == colsidx
    @test r3 == last_valid_index
    @test r2[1:r3] == ranges

    ds = Dataset(x1 = [1, 1, 1, 1, 3, 3, 3], x2 = [1, 6, 5, 5, 5, 5, 2], x3 = [2, 2, 4, 4, 4, 4, 1], x4 = [4, 1, 1, 4, 4, 4, 1])
    colsidx = 1:3
    T = nrow(ds) < typemax(Int32) ? Val(Int32) : Val(Int64)
    ranges = [1, 2, 3, 5, 7]
    last_valid_index = 5
    r1, r2, r3 = _find_starts_of_groups(ds, colsidx, T)
    @test r1 == colsidx
    @test r3 == last_valid_index
    @test r2[1:r3] == ranges
end