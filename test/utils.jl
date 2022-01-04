using Test, InMemoryDatasets, PooledArrays, Random, CategoricalArrays

@testset "make_unique - from DataFrames"  begin
    @test IMD.make_unique([:x, :x, :x_1, :x2], makeunique=true) == [:x, :x_2, :x_1, :x2]
    @test_throws ArgumentError IMD.make_unique([:x, :x, :x_1, :x2], makeunique=false)
    @test IMD.make_unique([:x, :x_1, :x2], makeunique=false) == [:x, :x_1, :x2]
    @test IMD.make_unique([:x, :x, :x_3, :x, :y, :x, :y, :x_1], makeunique = true) == [:x, :x_2, :x_3, :x_4, :y, :x_5, :y_1, :x_1]
end

@testset "repeat count- from DataFrames" begin
    ds = Dataset(a=1:2, b=3:4)
    ref = Dataset(a=repeat(1:2, 2),
                    b=repeat(3:4, 2))
    @test repeat(ds, 2) == ref
    @test repeat(view(ds, 1:2, :), 2) == ref

    @test_throws ArgumentError repeat(ds, 0)
    @test_throws ArgumentError repeat(ds, -1)
end

@testset "repeat inner_outer- from DataFrames" begin
    ds = Dataset(a=1:2, b=3:4)
    ref = Dataset(a=repeat(1:2, inner=2, outer=3),
                    b=repeat(3:4, inner=2, outer=3))
    @test repeat(ds, inner=2, outer=3) == ref
    @test repeat(view(ds, 1:2, :), inner=2, outer=3) == ref

    @test_throws ArgumentError  repeat(ds, inner=2, outer=0)
    @test_throws ArgumentError repeat(ds, inner=0, outer=3)
    @test_throws ArgumentError repeat(ds, inner=2, outer=false)
    @test_throws ArgumentError repeat(ds, inner=false, outer=3)
    @test_throws ArgumentError repeat(ds, inner=2, outer=-1)
    @test_throws ArgumentError repeat(ds, inner=-1, outer=3)
end

@testset "repeat! count- from DataFrames" begin
    ds = Dataset(a=1:2, b=3:4)
    ref = Dataset(a=repeat(1:2, 2),
                    b=repeat(3:4, 2))
    a = ds.a
    b = ds.b
    repeat!(ds, 2)
    @test ds == ref
    @test a == 1:2
    @test b == 3:4

    for v in (0, false)
        ds = Dataset(a=1:2, b=3:4)
        @test_throws ArgumentError repeat!(ds, v)

    end

    ds = Dataset(a=1:2, b=3:4)
    @test_throws ArgumentError repeat(ds, -1)
    @test ds == Dataset(a=1:2, b=3:4)

    @test_throws MethodError repeat!(view(ds, 1:2, :), 2)
end

@testset "repeat! inner_outer- from DataFrames" begin
    ds = Dataset(a=1:2, b=3:4)
    ref = Dataset(a=repeat(1:2, inner=2, outer=3),
                    b=repeat(3:4, inner=2, outer=3))
    a = ds.a
    b = ds.b
    repeat!(ds, inner = 2, outer = 3)
    @test ds == ref
    @test a == 1:2
    @test b == 3:4

    for v in (0, false)
        ds = Dataset(a=1:2, b=3:4)
        @test_throws ArgumentError repeat!(ds, inner=2, outer=v)

        ds = Dataset(a=1:2, b=3:4)
        @test_throws ArgumentError repeat!(ds, inner=v, outer=3)
    end

    ds = Dataset(a=1:2, b=3:4)
    @test_throws ArgumentError repeat(ds, inner = 2, outer = -1)
    @test_throws ArgumentError repeat(ds, inner = -1, outer = 3)
    @test ds == Dataset(a=1:2, b=3:4)

    @test_throws MethodError repeat!(view(ds, 1:2, :), inner = 2, outer = 3)
end

@testset "funname- from DataFrames" begin
    @test IMD.funname(sum ∘ skipmissing ∘ Base.div12) ==
          :sum_skipmissing_div12
end

@testset "repeat - passing `freq`" begin
    ds = Dataset(x1 = [1,2, missing, 0], x2 = PooledArray([1,3,1,2]), x3 = [1.2,1.2,1.1,-10.0], x4 = [2,1,1,1])
    @test repeat(ds, freq = 2) == ds[[1,2,2,2,3,4,4], :]
    @test repeat(ds, freq = :x2) == ds[[1,2,2,2,3,4,4], :]
    @test repeat(ds, freq = :x4) == ds[[1,1,2,3,4], :]
    @test repeat(ds, freq = ds[!, :x4]) == ds[[1,1,2,3,4], :]
    @test repeat(view(ds, :, :), freq = :x4) == ds[[1,1,2,3,4], :]
    @test repeat(view(ds, :, :), freq = 4) == ds[[1,1,2,3,4], :]
    @test_throws ArgumentError repeat(ds, freq = :x1)
    @test_throws ArgumentError repeat(ds, freq = 3)

    repeat!(ds, 1000)
    @test repeat(ds, freq = 2) == ds[repeat([1,2,2,2,3,4,4], 1000), :]
    @test repeat(ds, freq = :x2) == ds[repeat([1,2,2,2,3,4,4], 1000), :]
    @test repeat(ds, freq = :x4) == ds[repeat([1,1,2,3,4],1000), :]
    @test repeat(ds, freq = ds[!, :x4]) == ds[repeat([1,1,2,3,4],1000), :]

     ds = Dataset(a = 0:2, b = 2:4)
     @test repeat(ds, freq = :a) == Dataset(a = [1,2,2], b = [3,4,4])
     @test repeat(ds, freq = [2,0,2]) == Dataset(a = [0,0,2,2], b=[2,2,4,4])

     @test repeat(ds, freq = [1000, 1000, 0]) == Dataset(a = [fill(0,1000);fill(1, 1000)], b=[fill(2,1000);fill(3, 1000)])
     @test repeat(view(ds,[1,2,3], [1,2]), freq = [1000, 1000, 0]) == Dataset(a = [fill(0,1000);fill(1, 1000)], b=[fill(2,1000);fill(3, 1000)])
     @test repeat(view(ds,[1,2,3], [1,2]), freq = [1000, 1000, 0], view  = true) == Dataset(a = [fill(0,1000);fill(1, 1000)], b=[fill(2,1000);fill(3, 1000)])
end
