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
end
