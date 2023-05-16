using Test, InMemoryDatasets, PooledArrays, Random, CategoricalArrays
const ≅ = isequal

# from DataFrames.jl
refds = Dataset(reshape(1.5:15.5, (3, 5)), :auto)

@testset "CartesianIndex" begin
    ds = Dataset(rand(2, 3), :auto)
    for i in axes(ds, 1), j in axes(ds, 2)
        @test ds[i, j] == ds[CartesianIndex(i, j)]
        r = rand()
        ds[CartesianIndex(i, j)] = r
        @test ds[i, j] == r
    end
    @test_throws BoundsError ds[CartesianIndex(0, 1)]
    @test_throws BoundsError ds[CartesianIndex(0, 0)]
    @test_throws BoundsError ds[CartesianIndex(1, 0)]
    @test_throws BoundsError ds[CartesianIndex(5, 1)]
    @test_throws BoundsError ds[CartesianIndex(5, 5)]
    @test_throws BoundsError ds[CartesianIndex(1, 5)]

    @test_throws BoundsError ds[CartesianIndex(0, 1)] = 1
    @test_throws ArgumentError ds[CartesianIndex(0, 0)] = 1
    @test_throws ArgumentError ds[CartesianIndex(1, 0)] = 1
    @test_throws BoundsError ds[CartesianIndex(5, 1)] = 1
    @test_throws ArgumentError ds[CartesianIndex(5, 5)] = 1
    @test_throws ArgumentError ds[CartesianIndex(1, 5)] = 1
end

@testset "broadcasting of AbstractDataset objects" begin
    for ds in (copy(refds), view(copy(refds), :, :))
        @test identity.(ds) == refds
        @test identity.(ds) !== ds
        @test (x->x).(ds) == refds
        @test (x->x).(ds) !== ds
        @test (ds .+ ds) ./ 2 == refds
        @test (ds .+ ds) ./ 2 !== ds
        @test ds .+ Matrix(ds) == 2 .* ds
        @test Matrix(ds) .+ ds == 2 .* ds
        @test (Matrix(ds) .+ ds .== 2 .* ds) == Dataset(trues(size(ds)), names(ds))
        @test ds .+ 1 == ds .+ ones(size(ds))
        @test ds .+ axes(ds, 1) == Dataset(Matrix(ds) .+ axes(ds, 1), names(ds))
        @test ds .+ permutedims(axes(ds, 2)) == Dataset(Matrix(ds) .+ permutedims(axes(ds, 2)), names(ds))
    end

    ds1 = copy(refds)
    ds2 = view(copy(refds), :, :)
    @test (ds1 .+ ds2) ./ 2 == refds
    @test (ds1 .- ds2) == Dataset(zeros(size(refds)), names(refds))
    @test (ds1 .* ds2) == refds .^ 2
    @test (ds1 ./ ds2) == Dataset(ones(size(refds)), names(refds))
end

@testset "broadcasting of AbstractDataset objects thrown exceptions" begin
    ds = copy(refds)
    dsv = view(ds, :, 2:ncol(ds))

    @test_throws DimensionMismatch ds .+ dsv
    @test_throws DimensionMismatch ds .+ ds[2:end, :]

    @test_throws DimensionMismatch ds .+ [1, 2]
    @test_throws DimensionMismatch ds .+ [1 2]
    @test_throws DimensionMismatch ds .+ rand(2, 2)
    @test_throws DimensionMismatch dsv .+ [1, 2]
    @test_throws DimensionMismatch dsv .+ [1 2]
    @test_throws DimensionMismatch dsv .+ rand(2, 2)

    ds2 = copy(ds)
    rename!(ds2, [:x1, :x2, :x3, :x4, :y])
    @test_throws ArgumentError ds .+ ds2
    @test_throws ArgumentError ds .+ 1 .+ ds2
end

@testset "broadcasting data sets" begin
    ds1 = Dataset(x=1, y=2)
    ds2 = Dataset(x=[1, 11], y=[2, 12])
    @test ds1 .+ ds2 == Dataset(x=[2, 12], y=[4, 14])

    ds1 = Dataset(x=1, y=2)
    ds2 = Dataset(x=[1, 11], y=[2, 12])
    x = ds2.x
    y = ds2.y
    t1 = IMD._get_lastmodified(IMD._attributes(ds2))
    sleep(.1)
    ds2 .+= ds1
    @test IMD._get_lastmodified(IMD._attributes(ds2)) != t1
    @test ds2.x === x
    @test ds2.y === y
    @test ds2 == Dataset(x=[2, 12], y=[4, 14])

    ds = Dataset(x=[1, 11], y=[2, 12])
    dsv = view(ds, 1:1, 1:2)
    t1 = IMD._get_lastmodified(IMD._attributes(ds))
    sleep(.1)
    ds .-= dsv
    @test IMD._get_lastmodified(IMD._attributes(ds)) != t1
    @test ds == Dataset(x=[0, 10], y=[0, 10])

    @test Dataset() .+ Dataset() == Dataset()
    @test_throws ArgumentError Dataset(a=1, b=1) .+ Dataset(b=1, a=1)

    ds = Dataset(a=1, b=2)
    @test_throws ArgumentError ds .= Dataset(b=1, a=2)
    @test_throws ArgumentError ds .= Dataset(a=1, c=2)
    @test_throws ArgumentError ds[!, [:a, :b]] .= Dataset(b=1, a=2)
    @test_throws ArgumentError ds[!, [:a, :b]] .= Dataset(a=1, c=2)
end

@testset "broadcasting of AbstractDataset objects corner cases" begin
    ds = Dataset(c11=categorical(["a", "b"]), c12=categorical([missing, "b"]), c13=categorical(["a", missing]),
                   c21=categorical([1, 2]), c22=categorical([missing, 2]), c23=categorical([1, missing]),
                   p11=PooledArray(["a", "b"]), p12=PooledArray([missing, "b"]), p13=PooledArray(["a", missing]),
                   p21=PooledArray([1, 2]), p22=PooledArray([missing, 2]), p23=PooledArray([1, missing]),
                   b1=[true, false], b2=[missing, false], b3=[true, missing],
                   f1=[1.0, 2.0], f2=[missing, 2.0], f3=[1.0, missing],
                   s1=["a", "b"], s2=[missing, "b"], s3=["a", missing])

    ds2 = Dataset(c11=categorical(["a", "b"]), c12=[nothing, "b"], c13=["a", nothing],
                    c21=categorical([1, 2]), c22=[nothing, 2], c23=[1, nothing],
                    p11=["a", "b"], p12=[nothing, "b"], p13=["a", nothing],
                    p21=[1, 2], p22=[nothing, 2], p23=[1, nothing],
                    b1=[true, false], b2=[nothing, false], b3=[true, nothing],
                    f1=[1.0, 2.0], f2=[nothing, 2.0], f3=[1.0, nothing],
                    s1=["a", "b"], s2=[nothing, "b"], s3=["a", nothing])

    @test ds ≅ identity.(ds)
    @test ds ≅ (x->x).(ds)
    ds3 = coalesce.(ds, nothing)
    @test ds2 == ds3
    @test eltype.(eachcol(ds2)) == eltype.(eachcol(ds3))
    for i in axes(ds, 2)
        @test typeof(ds2[!, i]) == typeof(ds3[!, i])
    end
    ds4 = (x -> ds[1, 1]).(ds)
    @test names(ds4) == names(ds)
    @test all(isa.(eachcol(ds4), DatasetColumn{Dataset, CategoricalVector{Union{Missing, String}, UInt32, String, CategoricalValue{String, UInt32}, Missing}}))
    @test all(eachcol(ds4) .== Ref(categorical(["a", "a"])))

    ds5 = Dataset(x=Any[1, 2, 3], y=Any[1, 2.0, big(3)])
    @test identity.(ds5) == ds5
    @test (x->x).(ds5) == ds5
    @test ds5 .+ 1 == Dataset(Matrix(ds5) .+ 1, names(ds5))
    @test eltype.(eachcol(identity.(ds5))) == [Union{Missing, Int}, Union{Missing, BigFloat}]
    @test eltype.(eachcol((x->x).(ds5))) == [Union{Missing, Int}, Union{Missing, BigFloat}]
    @test eltype.(eachcol(ds5 .+ 1)) == [Union{Missing, Int}, Union{Missing, BigFloat}]
end

@testset "normal data set and data set row in broadcasted assignment - one column" begin
    ds = copy(refds)
    ds[!, 1] .+= 1
    @test ds.x1 == [2.5, 3.5, 4.5]
    @test ds[:, 2:end] == refds[:, 2:end]
    t1 = IMD._get_lastmodified(IMD._attributes(ds))
    sleep(.1)

    dsv = @view ds[1:2, 2:end]
    dsv[!, 1] .+= 100
    @test IMD._get_lastmodified(IMD._attributes(ds)) != t1
    @test ds.x2 == [104.5, 105.5, 6.5]
    # reverse the performed operations
    ds.x1 -= [1, 1, 1]
    ds.x2 -= [100, 100, 0]
    @test ds == Dataset(reshape(1.5:15.5, (3, 5)), :auto)

    ds = copy(refds)
    ds[:, 1] .+= 1
    @test ds.x1 == [2.5, 3.5, 4.5]
    @test ds[:, 2:end] == refds[:, 2:end]

    dsv = @view ds[1:2, 2:end]
    dsv.x2 .+= 1
    @test dsv.x2 == [5.5, 6.5]
    @test dsv[:, 2:end] == refds[1:2, 3:end]
    @test Matrix(ds) == [2.5  5.5  7.5  10.5  13.5
                         3.5  6.5  8.5  11.5  14.5
                         4.5  6.5  9.5  12.5  15.5]

    dsr = ds[1, 3:end]
    t1 = IMD._get_lastmodified(IMD._attributes(ds))
    sleep(.1)

    dsr[end-1:end] .= 10
    @test t1 != IMD._get_lastmodified(IMD._attributes(ds))

    @test Vector(dsr) == [7.5, 10.0, 10.0]
    @test Matrix(ds) == [2.5  5.5  7.5  10.0  10.0
                         3.5  6.5  8.5  11.5  14.5
                         4.5  6.5  9.5  12.5  15.5]

    ds = copy(refds)
    ds[!, 1] .+= [1, 2, 3]
    @test ds.x1 == [2.5, 4.5, 6.5]
    @test ds[:, 2:end] == refds[:, 2:end]

    ds = copy(refds)
    ds[!, :x1] .+= [1, 2, 3]
    @test ds.x1.val == [2.5, 4.5, 6.5]
    @test ds[:, 2:end] == refds[:, 2:end]

    dsv = @view ds[1:2, 2:end]
    dsv[!, :x2] .+= [1, 2]
    @test dsv.x2 == [5.5, 7.5]
    @test dsv[:, 2:end] == refds[1:2, 3:end]
    @test Matrix(ds) == [2.5  5.5  7.5  10.5  13.5
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    dsr = ds[1, 3:end]
    dsr[end-1:end] .= [10, 11]
    @test Vector(dsr) == [7.5, 10.0, 11.0]
    @test Matrix(ds) == [2.5  5.5  7.5  10.0  11.0
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    ds = copy(refds)
    ds[:, 1] .+= [1, 2, 3]
    @test ds.x1 == [2.5, 4.5, 6.5]
    @test ds[:, 2:end] == refds[:, 2:end]
    t1 = IMD._get_lastmodified(IMD._attributes(ds))
    sleep(.1)
    dsv = @view ds[1:2, 2:end]
    dsv[:, 1] .+= [1, 2]
    @test t1 != IMD._get_lastmodified(IMD._attributes(ds))
    @test dsv.x2 == [5.5, 7.5]
    @test dsv[:, 2:end] == refds[1:2, 3:end]
    @test Matrix(ds) == [2.5  5.5  7.5  10.5  13.5
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    # test a more complex broadcasting pattern
    ds = copy(refds)
    ds[!, 1] .+= [0, 1, 2] .+ 1
    @test ds.x1 == ds[!, 1] == [2.5, 4.5, 6.5]
    @test ds[:, 2:end] == refds[:, 2:end]

    ds = copy(refds)
    ds[!, "x1"] .+= [0, 1, 2] .+ 1
    @test ds."x1" == ds[!, 1] == [2.5, 4.5, 6.5]
    @test ds[:, Not("x1")] == refds[:, 2:end]

    ds = copy(refds)
    dsv = @view ds[1:2, 2:end]
    dsv[!, 1] .+= [0, 1] .+ 1
    @test ds == Dataset([1.5  5.5  7.5  10.5  13.5
                           2.5  7.5  8.5  11.5  14.5
                           3.5  6.5  9.5  12.5  15.5], :auto)

    dsv = @view ds[1:2, 2:end]
    @test_throws ArgumentError dsv[!, "x1"] .+= [0, 1] .+ 1
    @test ds == Dataset([1.5  5.5  7.5  10.5  13.5
                           2.5  7.5  8.5  11.5  14.5
                           3.5  6.5  9.5  12.5  15.5], :auto)


    ds = copy(refds)
    t1 = IMD._get_lastmodified(IMD._attributes(ds))
    sleep(.1)
    ds[!, :x1] .+= [0, 1, 2] .+ 1
    @test t1 != IMD._get_lastmodified(IMD._attributes(ds))
    @test ds.x1 == [2.5, 4.5, 6.5]
    @test ds[:, 2:end] == refds[:, 2:end]

    dsv = @view ds[1:2, 2:end]
    dsv[!, :x2] .+= [0, 1] .+ 1
    @test dsv.x2 == [5.5, 7.5]
    @test dsv[:, 2:end] == refds[1:2, 3:end]
    @test Matrix(ds) == [2.5  5.5  7.5  10.5  13.5
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    ds = copy(refds)
    ds[!, "x1"] .+= [0, 1, 2] .+ 1
    @test ds."x1" == [2.5, 4.5, 6.5]
    @test ds[:, 2:end] == refds[:, 2:end]

    dsv = @view ds[1:2, 2:end]
    dsv[!, "x2"] .+= [0, 1] .+ 1
    @test dsv."x2" == [5.5, 7.5]
    @test dsv[:, 2:end] == refds[1:2, 3:end]
    @test Matrix(ds) == [2.5  5.5  7.5  10.5  13.5
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    dsr = ds[1, 3:end]
    dsr[end-1:end] .= [9, 10] .+ 1
    @test Vector(dsr) == [7.5, 10.0, 11.0]
    @test Matrix(ds) == [2.5  5.5  7.5  10.0  11.0
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    ds = copy(refds)
    ds[:, 1] .+= [0, 1, 2] .+ 1
    @test ds.x1 == [2.5, 4.5, 6.5]
    @test ds[:, 2:end] == refds[:, 2:end]

    dsv = @view ds[1:2, 2:end]
    dsv[:, 1] .+= [0, 1] .+ 1
    @test dsv.x2 == [5.5, 7.5]
    @test dsv[:, 2:end] == refds[1:2, 3:end]
    @test Matrix(ds) == [2.5  5.5  7.5  10.5  13.5
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    ds = copy(refds)
    ds[:, "x1"] .+= [0, 1, 2] .+ 1
    @test ds."x1" == [2.5, 4.5, 6.5]
    @test ds[:, 2:end] == refds[:, 2:end]

    dsv = @view ds[1:2, 2:end]
    dsv[:, "x2"] .+= [0, 1] .+ 1
    @test dsv."x2" == [5.5, 7.5]
    @test dsv[:, 2:end] == refds[1:2, 3:end]
    @test Matrix(ds) == [2.5  5.5  7.5  10.5  13.5
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    ds = copy(refds)
    dsv = @view ds[1:2, 2:end]
    dsr = ds[1, 3:end]
    @test_throws DimensionMismatch dsv[!, 1] .= fill(100, 2, 2)
    @test_throws DimensionMismatch dsv[!, 1] .= reshape(fill(200, 4), :, 2)
    @test_throws DimensionMismatch ds[!, 1] .= rand(1, 2)
    @test_throws DimensionMismatch dsr[end-1:end] .= rand(3, 1)
    @test_throws DimensionMismatch ds[:, 1] .= rand(1, 3)
    @test_throws DimensionMismatch dsv[:, 1] .= rand(1, 2)
    @test_throws DimensionMismatch ds[!, 1] .= reshape(rand(3), 1, :)
    @test_throws DimensionMismatch dsr[end-1:end] .= reshape(rand(3), :, 1)
    @test_throws DimensionMismatch ds[:, 1] .= reshape(rand(3), 1, :, 1)
    @test_throws DimensionMismatch dsv[:, 1] .= reshape(rand(2), 1, :, 1)

    ds = copy(refds)
    ds[!, :x1] .+= 1
    @test ds.x1 == [2.5, 3.5, 4.5]
    @test ds[:, 2:end] == refds[:, 2:end]

    dsv = @view ds[1:2, 2:end]
    t1 = IMD._get_lastmodified(IMD._attributes(ds))
    sleep(.1)
    dsv[!, :x2] .+= 1
    @test t1 != IMD._get_lastmodified(IMD._attributes(ds))
    dsr = ds[1, 3:end]
    dsr[[:x4, :x5]] .= 10
    @test Vector(dsr) == [7.5, 10.0, 10.0]
    @test Matrix(ds) == [2.5  5.5  7.5  10.0  10.0
                         3.5  6.5  8.5  11.5  14.5
                         4.5  6.5  9.5  12.5  15.5]

    ds = copy(refds)
    ds[!, "x1"] .+= 1
    @test ds."x1" == [2.5, 3.5, 4.5]
    @test ds[:, 2:end] == refds[:, 2:end]

    dsv = @view ds[1:2, 2:end]
    dsv[!, "x2"] .+= 1

    dsr = ds[1, 3:end]
    dsr[["x4", "x5"]] .= 10
    @test Vector(dsr) == [7.5, 10.0, 10.0]
    @test Matrix(ds) == [2.5  5.5  7.5  10.0  10.0
                         3.5  6.5  8.5  11.5  14.5
                         4.5  6.5  9.5  12.5  15.5]

    ds = copy(refds)
    ds[:, :x1] .+= 1
    @test ds.x1 == [2.5, 3.5, 4.5]
    @test ds[:, 2:end] == refds[:, 2:end]

    dsv = @view ds[1:2, 2:end]
    dsv[:, :x2] .+= 1
    @test dsv.x2 == [5.5, 6.5]
    @test dsv[:, 2:end] == refds[1:2, 3:end]
    @test Matrix(ds) == [2.5  5.5  7.5  10.5  13.5
                         3.5  6.5  8.5  11.5  14.5
                         4.5  6.5  9.5  12.5  15.5]

    ds = copy(refds)
    ds[:, "x1"] .+= 1
    @test ds."x1" == [2.5, 3.5, 4.5]
    @test ds[:, 2:end] == refds[:, 2:end]

    dsv = @view ds[1:2, 2:end]
    t1 = IMD._get_lastmodified(IMD._attributes(ds))
    sleep(.1)
    dsv[:, "x2"] .+= 1
    @test t1 != IMD._get_lastmodified(IMD._attributes(ds))
    @test dsv."x2" == [5.5, 6.5]
    @test dsv[:, 2:end] == refds[1:2, 3:end]
    @test Matrix(ds) == [2.5  5.5  7.5  10.5  13.5
                         3.5  6.5  8.5  11.5  14.5
                         4.5  6.5  9.5  12.5  15.5]

    ds = copy(refds)
    ds[!, :x1] .+= [1, 2, 3]
    @test ds.x1 == [2.5, 4.5, 6.5]
    @test ds[:, 2:end] == refds[:, 2:end]

    dsv = @view ds[1:2, 2:end]
    dsv[!, :x2] .+= [1, 2]

    dsr = ds[1, 3:end]
    dsr[[:x4, :x5]] .= [10, 11]
    @test Vector(dsr) == [7.5, 10.0, 11.0]
    @test Matrix(ds) == [2.5  5.5  7.5  10.0  11.0
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    ds = copy(refds)
    ds[!, "x1"] .+= [1, 2, 3]
    @test ds."x1" == [2.5, 4.5, 6.5]
    @test ds[:, 2:end] == refds[:, 2:end]

    dsv = @view ds[1:2, 2:end]
    dsv[!, :x2] .+= [1, 2]

    dsr = ds[1, 3:end]
    dsr[["x4", "x5"]] .= [10, 11]
    @test Vector(dsr) == [7.5, 10.0, 11.0]
    @test Matrix(ds) == [2.5  5.5  7.5  10.0  11.0
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    ds = copy(refds)
    ds[:, :x1] .+= [1, 2, 3]
    @test ds.x1 == [2.5, 4.5, 6.5]
    @test ds[:, 2:end] == refds[:, 2:end]

    dsv = @view ds[1:2, 2:end]
    dsv[:, :x2] .+= [1, 2]
    @test dsv.x2 == [5.5, 7.5]
    @test dsv[:, 2:end] == refds[1:2, 3:end]
    @test Matrix(ds) == [2.5  5.5  7.5  10.5  13.5
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    ds = copy(refds)
    ds[:, "x1"] .+= [1, 2, 3]
    @test ds."x1" == [2.5, 4.5, 6.5]
    @test ds[:, 2:end] == refds[:, 2:end]

    dsv = @view ds[1:2, 2:end]
    dsv[:, "x2"] .+= [1, 2]
    @test dsv."x2" == [5.5, 7.5]
    @test dsv[:, 2:end] == refds[1:2, 3:end]
    @test Matrix(ds) == [2.5  5.5  7.5  10.5  13.5
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    ds = copy(refds)
    dsv = @view ds[1:2, 2:end]
    dsr = ds[1, 3:end]

    @test_throws DimensionMismatch dsv[!, :x2] .= fill(100, 2, 2)
    @test_throws DimensionMismatch dsv[!, 1] .= reshape(fill(200, 4), :, 2)
    @test_throws DimensionMismatch dsv[!, "x2"] .= fill(100, 2, 2)
    @test_throws DimensionMismatch ds[!, :x1] .= rand(1, 3)
    @test_throws DimensionMismatch dsr[[:x4, :x5]] .= rand(3, 1)
    @test_throws DimensionMismatch ds[:, :x1] .= rand(1, 3)
    @test_throws DimensionMismatch dsv[:, :x2] .= rand(1, 2)
    @test_throws DimensionMismatch ds[!, 1] .= reshape(rand(3), 1, :)
    @test_throws DimensionMismatch dsr[end-1:end] .= reshape(rand(3), :, 1)
    @test_throws DimensionMismatch ds[:, 1] .= reshape(rand(3), 1, :)
    @test_throws DimensionMismatch dsv[:, 1] .= reshape(rand(2), 1, :)
    @test_throws DimensionMismatch ds[!, "x1"] .= rand(1, 3)
    @test_throws DimensionMismatch dsr[["x4", "x5"]] .= rand(3, 1)
    @test_throws DimensionMismatch ds[:, "x1"] .= rand(1, 3)
    @test_throws DimensionMismatch dsv[:, "x2"] .= rand(1, 2)
end

@testset "normal data set and data set view in broadcasted assignment - two columns" begin
    ds = copy(refds)
    ds[:, [1, 2]] .= Matrix(ds[:, [1, 2]]) .+ 1
    @test ds.x1 == [2.5, 3.5, 4.5]
    @test ds.x2 == [5.5, 6.5, 7.5]
    @test ds[:, 3:end] == refds[:, 3:end]

    dsv = @view ds[1:2, 3:end]
    t1 = IMD._get_lastmodified(IMD._attributes(ds))
    sleep(.1)
    dsv[:, [1, 2]] .= Matrix(dsv[:, [1, 2]]) .+ 1
    @test t1 != IMD._get_lastmodified(IMD._attributes(ds))

    @test dsv.x3 == [8.5, 9.5]
    @test dsv.x4 == [11.5, 12.5]
    @test dsv[:, 3:end] == refds[1:2, 5:end]
    @test Matrix(ds) == [2.5  5.5  8.5  11.5  13.5
                         3.5  6.5  9.5  12.5  14.5
                         4.5  7.5  9.5  12.5  15.5]

    ds = copy(refds)
    ds[:, [1, 2]] .= Matrix(ds[:, [1, 2]]) .+ 1
    @test ds.x1 == [2.5, 3.5, 4.5]
    @test ds.x2 == [5.5, 6.5, 7.5]
    @test ds[:, 3:end] == refds[:, 3:end]

    dsv = @view ds[1:2, 3:end]
    dsv[:, [1, 2]] .= Matrix(dsv[:, [1, 2]]) .+ 1
    @test dsv.x3 == [8.5, 9.5]
    @test dsv.x4 == [11.5, 12.5]
    @test dsv[:, 3:end] == refds[1:2, 5:end]
    @test Matrix(ds) == [2.5  5.5  8.5  11.5  13.5
                         3.5  6.5  9.5  12.5  14.5
                         4.5  7.5  9.5  12.5  15.5]

    ds = copy(refds)
    t1 = IMD._get_lastmodified(IMD._attributes(ds))
    sleep(.1)

    ds[:, [1, 2]] .= Matrix(ds[:, [1, 2]]) .+ [1 4
                                               2 5
                                               3 6]
   @test t1 != IMD._get_lastmodified(IMD._attributes(ds))

    @test ds.x1 == [2.5, 4.5, 6.5]
    @test ds.x2 == [8.5, 10.5, 12.5]
    @test ds[:, 3:end] == refds[:, 3:end]

    dsv = @view ds[1:2, 3:end]
    dsv[:, [1, 2]] .= Matrix(dsv[:, [1, 2]]) .+ [1 3
                                               2 4]
    @test dsv.x3 == [8.5, 10.5]
    @test dsv.x4 == [13.5, 15.5]
    @test dsv[:, 3:end] == refds[1:2, 5:end]
    @test Matrix(ds) == [2.5   8.5   8.5  13.5  13.5
                         4.5  10.5  10.5  15.5  14.5
                         6.5  12.5   9.5  12.5  15.5]

    ds = copy(refds)
    ds[:, [1, 2]] .= Matrix(ds[:, [1, 2]]) .+ [1 4
                                               2 5
                                               3 6]
    @test ds.x1 == [2.5, 4.5, 6.5]
    @test ds.x2 == [8.5, 10.5, 12.5]
    @test ds[:, 3:end] == refds[:, 3:end]

    dsv = @view ds[1:2, 3:end]
    dsv[:, [1, 2]] .= Matrix(dsv[:, [1, 2]]) .+ [1 3
                                               2 4]
    @test dsv.x3 == [8.5, 10.5]
    @test dsv.x4 == [13.5, 15.5]
    @test dsv[:, 3:end] == refds[1:2, 5:end]
    @test Matrix(ds) == [2.5   8.5   8.5  13.5  13.5
                         4.5  10.5  10.5  15.5  14.5
                         6.5  12.5   9.5  12.5  15.5]

    ds = copy(refds)
    dsv = @view ds[1:2, 2:end]
    @test_throws DimensionMismatch ds[:, [1, 2]] .= rand(3, 10)
    @test_throws DimensionMismatch dsv[:, [1, 2]] .= rand(2, 10)
    @test_throws DimensionMismatch ds[:, [1, 2]] .= rand(3, 10)
    @test_throws DimensionMismatch dsv[:, [1, 2]] .= rand(2, 10)

    ds = copy(refds)
    ds[:, [:x1, :x2]] .= Matrix(ds[:, [:x1, :x2]]) .+ 1
    @test ds.x1 == [2.5, 3.5, 4.5]
    @test ds.x2 == [5.5, 6.5, 7.5]
    @test ds[:, 3:end] == refds[:, 3:end]

    dsv = @view ds[1:2, 3:end]
    dsv[:, [:x3, :x4]] .= Matrix(dsv[:, [:x3, :x4]]) .+ 1
    @test dsv.x3 == [8.5, 9.5]
    @test dsv.x4 == [11.5, 12.5]
    @test dsv[:, 3:end] == refds[1:2, 5:end]
    @test Matrix(ds) == [2.5  5.5  8.5  11.5  13.5
                         3.5  6.5  9.5  12.5  14.5
                         4.5  7.5  9.5  12.5  15.5]

    ds = copy(refds)
    ds[:, ["x1", "x2"]] .= Matrix(ds[:, [:x1, :x2]]) .+ 1
    @test ds.x1 == [2.5, 3.5, 4.5]
    @test ds.x2 == [5.5, 6.5, 7.5]
    @test ds[:, 3:end] == refds[:, 3:end]

    dsv = @view ds[1:2, 3:end]
    t1 = IMD._get_lastmodified(IMD._attributes(ds))
    sleep(.1)

    dsv[:, ["x3", "x4"]] .= Matrix(dsv[:, [:x3, :x4]]) .+ 1
    @test t1 != IMD._get_lastmodified(IMD._attributes(ds))

    @test dsv.x3 == [8.5, 9.5]
    @test dsv.x4 == [11.5, 12.5]
    @test dsv[:, 3:end] == refds[1:2, 5:end]
    @test Matrix(ds) == [2.5  5.5  8.5  11.5  13.5
                         3.5  6.5  9.5  12.5  14.5
                         4.5  7.5  9.5  12.5  15.5]

    ds = copy(refds)
    t1 = IMD._get_lastmodified(IMD._attributes(ds))
    sleep(.1)

    ds[:, [:x1, :x2]] .= Matrix(ds[:, [:x1, :x2]]) .+ 1
    @test t1 != IMD._get_lastmodified(IMD._attributes(ds))

    @test ds.x1 == [2.5, 3.5, 4.5]
    @test ds.x2 == [5.5, 6.5, 7.5]
    @test ds[:, 3:end] == refds[:, 3:end]

    dsv = @view ds[1:2, 3:end]
    t1 = IMD._get_lastmodified(IMD._attributes(ds))
    sleep(.1)

    dsv[:, [:x3, :x4]] .= Matrix(dsv[:, [:x3, :x4]]) .+ 1
    @test t1 != IMD._get_lastmodified(IMD._attributes(ds))

    @test dsv.x3 == [8.5, 9.5]
    @test dsv.x4 == [11.5, 12.5]
    @test dsv[:, 3:end] == refds[1:2, 5:end]
    @test Matrix(ds) == [2.5  5.5  8.5  11.5  13.5
                         3.5  6.5  9.5  12.5  14.5
                         4.5  7.5  9.5  12.5  15.5]

    ds = copy(refds)
    ds[:, ["x1", "x2"]] .= Matrix(ds[:, [:x1, :x2]]) .+ 1
    @test ds.x1 == [2.5, 3.5, 4.5]
    @test ds.x2 == [5.5, 6.5, 7.5]
    @test ds[:, 3:end] == refds[:, 3:end]

    dsv = @view ds[1:2, 3:end]
    t1 = IMD._get_lastmodified(IMD._attributes(ds))
    sleep(.1)

    dsv[:, ["x3", "x4"]] .= Matrix(dsv[:, [:x3, :x4]]) .+ 1
    @test t1 != IMD._get_lastmodified(IMD._attributes(ds))

    @test dsv.x3 == [8.5, 9.5]
    @test dsv.x4 == [11.5, 12.5]
    @test dsv[:, 3:end] == refds[1:2, 5:end]
    @test Matrix(ds) == [2.5  5.5  8.5  11.5  13.5
                         3.5  6.5  9.5  12.5  14.5
                         4.5  7.5  9.5  12.5  15.5]

    ds = copy(refds)
    ds[:, [:x1, :x2]] .= Matrix(ds[:, [:x1, :x2]]) .+ [1 4
                                                       2 5
                                                       3 6]
    @test ds.x1 == [2.5, 4.5, 6.5]
    @test ds.x2 == [8.5, 10.5, 12.5]
    @test ds[:, 3:end] == refds[:, 3:end]

    dsv = @view ds[1:2, 3:end]
    dsv[:, [:x3, :x4]] .= Matrix(dsv[:, [:x3, :x4]]) .+ [1 3
                                                         2 4]
    @test dsv.x3 == [8.5, 10.5]
    @test dsv.x4 == [13.5, 15.5]
    @test dsv[:, 3:end] == refds[1:2, 5:end]
    @test Matrix(ds) == [2.5   8.5   8.5  13.5  13.5
                         4.5  10.5  10.5  15.5  14.5
                         6.5  12.5   9.5  12.5  15.5]

    ds = copy(refds)
    ds[:, ["x1", "x2"]] .= Matrix(ds[:, ["x1", "x2"]]) .+ [1 4
                                                           2 5
                                                           3 6]
    @test ds.x1 == [2.5, 4.5, 6.5]
    @test ds.x2 == [8.5, 10.5, 12.5]
    @test ds[:, 3:end] == refds[:, 3:end]

    dsv = @view ds[1:2, 3:end]
    dsv[:, ["x3", "x4"]] .= Matrix(dsv[:, ["x3", "x4"]]) .+ [1 3
                                                             2 4]
    @test dsv.x3 == [8.5, 10.5]
    @test dsv.x4 == [13.5, 15.5]
    @test dsv[:, 3:end] == refds[1:2, 5:end]
    @test Matrix(ds) == [2.5   8.5   8.5  13.5  13.5
                         4.5  10.5  10.5  15.5  14.5
                         6.5  12.5   9.5  12.5  15.5]

    ds = copy(refds)
    ds[:, [:x1, :x2]] .= Matrix(ds[:, [:x1, :x2]]) .+ [1 4
                                                       2 5
                                                       3 6]
    @test ds.x1 == [2.5, 4.5, 6.5]
    @test ds.x2 == [8.5, 10.5, 12.5]
    @test ds[:, 3:end] == refds[:, 3:end]

    dsv = @view ds[1:2, 3:end]
    dsv[:, [:x3, :x4]] .= Matrix(dsv[:, [:x3, :x4]]) .+ [1 3
                                                         2 4]
    @test dsv.x3 == [8.5, 10.5]
    @test dsv.x4 == [13.5, 15.5]
    @test dsv[:, 3:end] == refds[1:2, 5:end]
    @test Matrix(ds) == [2.5   8.5   8.5  13.5  13.5
                         4.5  10.5  10.5  15.5  14.5
                         6.5  12.5   9.5  12.5  15.5]

    ds = copy(refds)
    ds[:, ["x1", "x2"]] .= Matrix(ds[:, ["x1", "x2"]]) .+ [1 4
                                                           2 5
                                                           3 6]
    @test ds.x1 == [2.5, 4.5, 6.5]
    @test ds.x2 == [8.5, 10.5, 12.5]
    @test ds[:, 3:end] == refds[:, 3:end]

    dsv = @view ds[1:2, 3:end]
    dsv[:, ["x3", "x4"]] .= Matrix(dsv[:, ["x3", "x4"]]) .+ [1 3
                                                             2 4]
    @test dsv.x3 == [8.5, 10.5]
    @test dsv.x4 == [13.5, 15.5]
    @test dsv[:, 3:end] == refds[1:2, 5:end]
    @test Matrix(ds) == [2.5   8.5   8.5  13.5  13.5
                         4.5  10.5  10.5  15.5  14.5
                         6.5  12.5   9.5  12.5  15.5]

    ds = copy(refds)
    dsv = @view ds[1:2, 2:end]
    @test_throws DimensionMismatch ds[:, [:x1, :x2]] .= rand(3, 10)
    @test_throws DimensionMismatch dsv[:, [:x3, :x4]] .= rand(2, 10)
    @test_throws DimensionMismatch ds[:, [:x1, :x2]] .= rand(3, 10)
    @test_throws DimensionMismatch dsv[:, [:x3, :x4]] .= rand(2, 10)
    @test_throws DimensionMismatch ds[:, ["x1", "x2"]] .= rand(3, 10)
    @test_throws DimensionMismatch dsv[:, ["x3", "x4"]] .= rand(2, 10)
    @test_throws DimensionMismatch ds[:, ["x1", "x2"]] .= rand(3, 10)
    @test_throws DimensionMismatch dsv[:, ["x3", "x4"]] .= rand(2, 10)

    ds = copy(refds)
    ds[:, [1, 2]] .= [1 2
                      3 4
                      5 6]
    @test Matrix(ds) == [1.0  2.0  7.5  10.5  13.5
                         3.0  4.0  8.5  11.5  14.5
                         5.0  6.0  9.5  12.5  15.5]

    ds = copy(refds)
    ds[:, [1, 2]] .= [1, 3, 5]
    @test Matrix(ds) == [1.0  1.0  7.5  10.5  13.5
                         3.0  3.0  8.5  11.5  14.5
                         5.0  5.0  9.5  12.5  15.5]

    ds = copy(refds)
    ds[:, [1, 2]] .= reshape([1, 3, 5], 3, 1)
    @test Matrix(ds) == [1.0  1.0  7.5  10.5  13.5
                         3.0  3.0  8.5  11.5  14.5
                         5.0  5.0  9.5  12.5  15.5]

    ds = copy(refds)
    ds[:, [1, 2]] .= 1
    @test Matrix(ds) == [1.0  1.0  7.5  10.5  13.5
                         1.0  1.0  8.5  11.5  14.5
                         1.0  1.0  9.5  12.5  15.5]

    ds = copy(refds)
    dsv = view(ds, 2:3, 2:4)
    dsv[:, [1, 2]] .= [1 2
                       3 4]
    @test Matrix(ds) == [1.5  4.5  7.5  10.5  13.5
                         2.5  1.0  2.0  11.5  14.5
                         3.5  3.0  4.0  12.5  15.5]

    ds = copy(refds)
    dsv = view(ds, 2:3, 2:4)
    dsv[:, [1, 2]] .= [1, 3]
    @test Matrix(ds) == [1.5  4.5  7.5  10.5  13.5
                         2.5  1.0  1.0  11.5  14.5
                         3.5  3.0  3.0  12.5  15.5]

    ds = copy(refds)
    dsv = view(ds, 2:3, 2:4)
    dsv[:, [1, 2]] .= reshape([1, 3], 2, 1)
    @test Matrix(ds) == [1.5  4.5  7.5  10.5  13.5
                         2.5  1.0  1.0  11.5  14.5
                         3.5  3.0  3.0  12.5  15.5]

    ds = copy(refds)
    dsv = view(ds, 2:3, 2:4)
    dsv[:, [1, 2]] .= 1
    @test Matrix(ds) == [1.5  4.5  7.5  10.5  13.5
                         2.5  1.0  1.0  11.5  14.5
                         3.5  1.0  1.0  12.5  15.5]
end

@testset "assignment to a whole data set and data set row" begin
    ds = copy(refds)
    t1 = IMD._get_lastmodified(IMD._attributes(ds))
    sleep(.1)

    ds .= 10
    @test t1 != IMD._get_lastmodified(IMD._attributes(ds))

    @test all(Matrix(ds) .== 10)
    dsv = view(ds, 1:2, 1:4)
    t1 = IMD._get_lastmodified(IMD._attributes(ds))
    sleep(.1)

    dsv .= 100
    @test t1 != IMD._get_lastmodified(IMD._attributes(ds))

    @test Matrix(ds) == [100.0  100.0  100.0  100.0  10.0
                        100.0  100.0  100.0  100.0  10.0
                         10.0   10.0   10.0   10.0  10.0]
    dsr = ds[1, 1:2]
    dsr .= 1000
    @test Matrix(ds) == [1000.0  1000.0  100.0  100.0  10.0
                         100.0   100.0  100.0  100.0  10.0
                         10.0    10.0   10.0   10.0  10.0]

    ds = copy(refds)
    t1 = IMD._get_lastmodified(IMD._attributes(ds))
    sleep(.1)

    ds[:, :] .= 10
    @test t1 != IMD._get_lastmodified(IMD._attributes(ds))

    @test all(Matrix(ds) .== 10)
    dsv = view(ds, 1:2, 1:4)
    dsv[:, :] .= 100
    @test Matrix(ds) == [100.0  100.0  100.0  100.0  10.0
                         100.0  100.0  100.0  100.0  10.0
                         10.0   10.0   10.0   10.0  10.0]
    dsr = ds[1, 1:2]
    dsr[:] .= 1000
    @test Matrix(ds) == [1000.0  1000.0  100.0  100.0  10.0
                         100.0   100.0   100.0  100.0  10.0
                         10.0    10.0    10.0   10.0   10.0]

    ds = copy(refds)
    ds[:, :] .= 10
    @test all(Matrix(ds) .== 10)
    dsv = view(ds, 1:2, 1:4)
    dsv[:, :] .= 100
    @test Matrix(ds) == [100.0  100.0  100.0  100.0  10.0
                         100.0  100.0  100.0  100.0  10.0
                         10.0   10.0   10.0   10.0  10.0]
end

@testset "extending data set in broadcasted assignment - one column" begin
    ds = copy(refds)
    t1 = IMD._get_lastmodified(IMD._attributes(ds))
    sleep(.1)

    ds[!, :a] .= 1
    @test t1 != IMD._get_lastmodified(IMD._attributes(ds))

    @test Matrix(ds) == [1.5  4.5  7.5  10.5  13.5  1.0
                         2.5  5.5  8.5  11.5  14.5  1.0
                         3.5  6.5  9.5  12.5  15.5  1.0]
    @test names(ds)[end] == "a"
    @test ds[:, 1:end-1] == refds
    ds[!, :b] .= [1, 2, 3]
    @test Matrix(ds) == [1.5  4.5  7.5  10.5  13.5  1.0 1.0
                         2.5  5.5  8.5  11.5  14.5  1.0 2.0
                         3.5  6.5  9.5  12.5  15.5  1.0 3.0]
    @test names(ds)[end] == "b"
    @test ds[:, 1:end-2] == refds
    cds = copy(ds)
    @test_throws DimensionMismatch ds[!, :c] .= ones(1, 3)
    @test ds == cds
    @test_throws DimensionMismatch ds[!, :x] .= ones(4)
    @test ds == cds
    @test_throws ArgumentError ds[!, 10] .= ones(3)
    @test ds == cds

    dsv = @view ds[1:2, 2:end]
    @test_throws BoundsError dsv[!, 10] .= ones(3)
    @test_throws ArgumentError dsv[!, :z] .= ones(3)
    @test ds == cds
    dsr = ds[1, 3:end]
    @test_throws BoundsError dsr[10] .= ones(3)
    @test_throws ArgumentError dsr[:z] .= ones(3)
    @test ds == cds

    ds = Dataset()
    t1 = IMD._get_lastmodified(IMD._attributes(ds))
    sleep(.1)

    @test_throws DimensionMismatch ds[!, :a] .= sin.(1:3)
    ds[!, :b] .= sin.(1)
    ds[!, :c] .= sin(1) .+ 1
    @test t1 != IMD._get_lastmodified(IMD._attributes(ds))

    @test ds == Dataset(b=Float64[], c=Float64[])

    ds = copy(refds)
    ds[!, "a"] .= 1
    @test Matrix(ds) == [1.5  4.5  7.5  10.5  13.5  1.0
                         2.5  5.5  8.5  11.5  14.5  1.0
                         3.5  6.5  9.5  12.5  15.5  1.0]
    @test names(ds)[end] == "a"
    @test ds[:, 1:end-1] == refds
    ds[!, "b"] .= [1, 2, 3]
    @test Matrix(ds) == [1.5  4.5  7.5  10.5  13.5  1.0 1.0
                         2.5  5.5  8.5  11.5  14.5  1.0 2.0
                         3.5  6.5  9.5  12.5  15.5  1.0 3.0]
    @test names(ds)[end] == "b"
    @test ds[:, 1:end-2] == refds
    cds = copy(ds)
    @test_throws DimensionMismatch ds[!, "c"] .= ones(1, 3)
    @test ds == cds
    @test_throws DimensionMismatch ds[!, "x"] .= ones(4)
    @test ds == cds
    @test_throws ArgumentError ds[!, 10] .= ones(3)
    @test ds == cds

    dsv = @view ds[1:2, 2:end]
    @test_throws BoundsError dsv[!, 10] .= ones(3)
    @test_throws ArgumentError dsv[!, "z"] .= ones(3)
    @test ds == cds
    dsr = ds[1, 3:end]
    @test_throws BoundsError dsr[10] .= ones(3)
    @test_throws ArgumentError dsr["z"] .= ones(3)
    @test ds == cds

    ds = Dataset()
    @test_throws DimensionMismatch ds[!, "a"] .= sin.(1:3)
    ds[!, "b"] .= sin.(1)
    ds[!, "c"] .= sin(1) .+ 1
    @test ds == Dataset(b=Float64[], c=Float64[])
end

@testset "empty data frame corner case" begin
    ds = Dataset()
    @test_throws ArgumentError ds[!, 1] .= 1
    @test_throws ArgumentError ds[!, 2] .= 1
    @test_throws ArgumentError ds[!, [:a, :b]] .= [1]
    @test_throws ArgumentError ds[!, [:a, :b]] .= 1
    @test_throws DimensionMismatch ds[!, :a] .= [1 2]
    @test_throws DimensionMismatch ds[!, :a] .= [1, 2]
    @test_throws DimensionMismatch ds[!, :a] .= sin.(1) .+ [1, 2]
    @test_throws ArgumentError ds[!, ["a", "b"]] .= [1]
    @test_throws ArgumentError ds[!, ["a", "b"]] .= 1
    @test_throws DimensionMismatch ds[!, "a"] .= [1 2]
    @test_throws DimensionMismatch ds[!, "a"] .= [1, 2]
    @test_throws DimensionMismatch ds[!, "a"] .= sin.(1) .+ [1, 2]

    for rhs in [1, [1], Int[], "abc", ["abc"]]
        ds = Dataset()
        t1 = IMD._get_lastmodified(IMD._attributes(ds))
        sleep(.1)

        ds[!, :a] .= rhs
        @test size(ds) == (0, 1)
        @test nonmissingtype(eltype(ds[!, 1])) == (rhs isa AbstractVector ? eltype(rhs) : nonmissingtype(typeof(rhs)))
        @test t1 != IMD._get_lastmodified(IMD._attributes(ds))

        ds = Dataset()
        t1 = IMD._get_lastmodified(IMD._attributes(ds))
        sleep(.1)

        ds[!, :a] .= length.(rhs)
        @test size(ds) == (0, 1)
        @test eltype(ds[!, 1]) == Union{Missing, Int}
        @test t1 != IMD._get_lastmodified(IMD._attributes(ds))

        ds = Dataset()
        ds[!, :a] .= length.(rhs) .+ 1
        @test size(ds) == (0, 1)
        @test eltype(ds[!, 1]) == Union{Missing, Int}

        ds = Dataset()
        @. ds[!, :a] = length(rhs) + 1
        @test size(ds) == (0, 1)
        @test eltype(ds[!, 1]) == Union{Missing, Int}

        ds = Dataset(x=Int[])
        ds[!, :a] .= rhs
        @test size(ds) == (0, 2)
        @test nonmissingtype(eltype(ds[!, 2])) == (rhs isa AbstractVector ? eltype(rhs) : nonmissingtype(typeof(rhs)))

        ds = Dataset(x=Int[])
        ds[!, :a] .= length.(rhs)
        @test size(ds) == (0, 2)
        @test eltype(ds[!, 2]) == Union{Missing, Int}

        ds = Dataset(x=Int[])
        ds[!, :a] .= length.(rhs) .+ 1
        @test size(ds) == (0, 2)
        @test eltype(ds[!, 2]) == Union{Missing, Int}

        ds = Dataset(x=Int[])
        @. ds[!, :a] = length(rhs) + 1
        @test size(ds) == (0, 2)
        @test eltype(ds[!, 2]) == Union{Missing, Int}

        ds = Dataset()
        ds[!, "a"] .= rhs
        @test size(ds) == (0, 1)
        @test nonmissingtype(eltype(ds[!, 1])) == (rhs isa AbstractVector ? eltype(rhs) : nonmissingtype(typeof(rhs)))

        ds = Dataset()
        ds[!, "a"] .= length.(rhs)
        @test size(ds) == (0, 1)
        @test eltype(ds[!, 1]) == Union{Missing, Int}

        ds = Dataset()
        ds[!, "a"] .= length.(rhs) .+ 1
        @test size(ds) == (0, 1)
        @test eltype(ds[!, 1]) == Union{Missing, Int}

        ds = Dataset()
        @. ds[!, "a"] = length(rhs) + 1
        @test size(ds) == (0, 1)
        @test eltype(ds[!, 1]) == Union{Missing, Int}

        ds = Dataset(x=Int[])
        ds[!, "a"] .= rhs
        @test size(ds) == (0, 2)
        @test nonmissingtype(eltype(ds[!, 2])) == (rhs isa AbstractVector ? eltype(rhs) : nonmissingtype(typeof(rhs)))

        ds = Dataset(x=Int[])
        ds[!, "a"] .= length.(rhs)
        @test size(ds) == (0, 2)
        @test eltype(ds[!, 2]) == Union{Missing, Int}

        ds = Dataset(x=Int[])
        ds[!, "a"] .= length.(rhs) .+ 1
        @test size(ds) == (0, 2)
        @test eltype(ds[!, 2]) == Union{Missing, Int}

        ds = Dataset(x=Int[])
        @. ds[!, "a"] = length(rhs) + 1
        @test size(ds) == (0, 2)
        @test eltype(ds[!, 2]) == Union{Missing, Int}
    end

    ds = Dataset()
    t1 = IMD._get_lastmodified(IMD._attributes(ds))
    sleep(.1)

    ds .= 1
    @test t1 != IMD._get_lastmodified(IMD._attributes(ds))

    @test ds == Dataset()
    ds .= [1]
    @test ds == Dataset()
    ds .= ones(1, 1)
    @test ds == Dataset()
    @test_throws DimensionMismatch ds .= ones(1, 2)
    @test_throws DimensionMismatch ds .= ones(1, 2, 1)

    ds = Dataset(a=[])
    ds[!, :b] .= sin.(1)
    @test eltype(ds.b) == Union{Missing, Float64}
    ds[!, :b] .= [1]
    @test eltype(ds.b) == Union{Missing, Int}
    ds[!, :b] .= 'a'
    @test eltype(ds.b) == Union{Missing, Char}
    @test names(ds) == ["a", "b"]

    c = categorical(["a", "b", "c"])
    ds = Dataset()
    @test_throws DimensionMismatch ds[!, :a] .= c

    ds[!, :b] .= c[1]
    @test nrow(ds) == 0
    @test ds.b.val isa CategoricalVector{Union{Missing, String}, UInt32, String, CategoricalValue{String, UInt32}, Missing}

    ds = Dataset(a=[])
    t1 = IMD._get_lastmodified(IMD._attributes(ds))
    sleep(.1)

    ds[!, "b"] .= sin.(1)
    @test eltype(ds."b") == Union{Missing, Float64}
    @test t1 != IMD._get_lastmodified(IMD._attributes(ds))

    ds[!, "b"] .= [1]
    @test eltype(ds."b") == Union{Int, Missing}
    ds[!, "b"] .= 'a'
    @test eltype(ds."b") == Union{Char, Missing}
    @test names(ds) == ["a", "b"]

    c = categorical(["a", "b", "c"])
    ds = Dataset()
    @test_throws DimensionMismatch ds[!, "a"] .= c

    ds[!, "b"] .= c[1]
    @test nrow(ds) == 0
    @test ds."b".val isa CategoricalVector{Union{Missing, String}}
end

@testset "test categorical values" begin
    for v in Any[categorical([1, 2, 3]), categorical([1, 2, missing]),
              categorical([missing, 1, 2]),
              categorical(["1", "2", "3"]), categorical(["1", "2", missing]),
              categorical([missing, "1", "2"])]
        ds = copy(refds)
        t1 = IMD._get_lastmodified(IMD._attributes(ds))
        sleep(.1)

        ds[!, :c1] .= v
        @test t1 != IMD._get_lastmodified(IMD._attributes(ds))

        @test ds.c1 ≅ v
        @test ds.c1.val !== v
        @test ds.c1.val isa CategoricalVector
        @test levels(ds.c1.val) == levels(v)
        @test levels(ds.c1.val) !== levels(v)
        ds[!, :c2] .= v[2]
        @test ds.c2.val == fill(v[2], 3)
        @test ds.c2.val isa CategoricalVector
        @test levels(ds.c2.val) == levels(v)
        ds[!, :c3] .= (x->x).(v)
        @test ds.c3.val ≅ v
        @test ds.c3.val !== v
        @test ds.c3.val isa CategoricalVector
        @test levels(ds.c3.val) == levels(v)
        @test levels(ds.c3.val) !== levels(v)
        ds[!, :c4] .= identity.(v)
        @test ds.c4.val ≅ v
        @test ds.c4.val !== v
        @test ds.c4.val isa CategoricalVector
        @test levels(ds.c4.val) == levels(v)
        @test levels(ds.c4.val) !== levels(v)
        ds[!, :c5] .= (x->v[2]).(v)
        @test unique(ds.c5.val) == [unwrap(v[2])]
        @test ds.c5.val isa CategoricalVector
        @test levels(ds.c5.val) == levels(v)
    end
end

@testset "scalar broadcasting" begin
    a = Dataset(x=zeros(2))
    a .= 1 ./ (1 + 2)
    @test a.x == [1/3, 1/3]
    a .= 1 ./ (1 .+ 3)
    @test a.x == [1/4, 1/4]
    a .= sqrt.(1 ./ 2)
    @test a.x == [sqrt(1/2), sqrt(1/2)]
end

@testset "tuple broadcasting" begin
    X = Dataset(zeros(2, 3), :auto)
    X .= (1, 2)
    @test X == Dataset([1 1 1; 2 2 2], :auto)

    X = Dataset(zeros(2, 3), :auto)
    X .= (1, 2) .+ 10 .- X
    @test X == Dataset([11 11 11; 12 12 12], :auto)

    X = Dataset(zeros(2, 3), :auto)
    X .+= (1, 2) .+ 10
    @test X == Dataset([11 11 11; 12 12 12], :auto)

    ds = Dataset(rand(2, 3), :auto)
    @test floor.(Int, ds ./ (1,)) == Dataset(zeros(Int, 2, 3), :auto)
    ds .= floor.(Int, ds ./ (1,))
    @test ds == Dataset(zeros(2, 3), :auto)

    ds = Dataset(rand(2, 3), :auto)
    @test_throws InexactError convert.(Int, ds)
    ds2 = convert.(Int, floor.(ds))
    @test ds2 == Dataset(zeros(Int, 2, 3), :auto)
    @test eltype.(eachcol(ds2)) == [Union{Missing, Int}, Union{Missing, Int}, Union{Missing, Int}]
end

@testset "scalar on assignment side" begin
    ds = Dataset(rand(2, 3), :auto)
    @test_throws MethodError ds[1, 1] .= ds[1, 1] .- ds[1, 1]
    ds[1, 1:1] .= ds[1, 1] .- ds[1, 1]
    @test ds[1, 1] == 0
    @test_throws MethodError ds[1, 2] .-= ds[1, 2]
    ds[1:1, 2] .-= ds[1, 2]
    @test ds[1, 2] == 0
end

@testset "nothing test" begin
    X = Dataset(Any[1 2; 3 4], :auto)
    X .= nothing
    @test (X .== nothing) == Dataset(trues(2, 2), :auto)

    X = Dataset([1 2; 3 4], :auto)
    @test_throws MethodError X .= nothing
    @test X == Dataset([1 2; 3 4], :auto)

    X = Dataset([1 2; 3 4], :auto)
    foreach(i -> X[!, i] .= nothing, axes(X, 2))
    @test (X .== nothing) == Dataset(trues(2, 2), :auto)
end


######
@testset "aliasing test" begin
    ds = Dataset(x=[1, 2])
    t1 = IMD._get_lastmodified(IMD._attributes(ds))
    sleep(.1)

    y = view(ds.x, [2, 1])
    ds .= y
    @test t1 != IMD._get_lastmodified(IMD._attributes(ds))

    @test ds[!, :x].val == [2, 1]

    ds = Dataset(x=[1, 2])
    y = view(ds.x, [2, 1])
    dsv = view(ds, :, :)
    dsv .= y
    @test ds.x == [2, 1]

    ds = Dataset(x=2, y=1, z=1)
    dsr = ds[1, :]
    y = view(ds.x.val, 1)
    dsr .= 2 .* y
    @test Vector(dsr) == [4, 4, 4]

    ds = Dataset(x=[1, 2], y=[11, 12])
    ds2 = Dataset()
    ds2.x = [-1, -2]
    ds2.y = ds.x
    ds3 = copy(ds2)
    ds .= ds2
    @test ds == ds3

    Random.seed!(1234)
    for i in 1:10
        ds1 = Dataset(rand(100, 100), :auto)
        ds2 = copy(ds1)
        for i in 1:100
            ds2[!, rand(1:100)] = ds1[!, i]
        end
        ds3 = copy(ds2)
        ds1 .= ds2
        @test ds1 == ds3
        @test ds2 != ds3
    end

    for i in 1:10
        ds1 = Dataset(rand(100, 100), :auto)
        ds2 = copy(ds1)
        for i in 1:100
            ds2[!, rand(1:100)] = ds1[!, i]
        end
        ds3 = copy(ds2)
        ds1 .= view(ds2, :, :)
        @test ds1 == ds3
        @test ds2 != ds3
    end

    for i in 1:10
        ds1 = Dataset(rand(100, 100), :auto)
        ds2 = copy(ds1)
        for i in 1:100
            ds2[!, rand(1:100)] = ds1[!, i]
        end
        ds3 = copy(ds2)
        view(ds1, :, :) .= ds2
        @test ds1 == ds3
        @test ds2 != ds3
    end

    for i in 1:10
        ds1 = Dataset(rand(100, 100), :auto)
        ds2 = copy(ds1)
        ds3 = copy(ds1)
        for i in 1:100
            ds2[!, rand(1:100)] = ds1[!, i]
            ds3[!, rand(1:100)] = ds1[!, i]
        end
        ds6 = copy(ds2)
        ds7 = copy(ds3)
        ds4 = Dataset(sin.(ds1[1, 1] .+ copy(ds1[!, 1]) .+ Matrix(ds2) ./ Matrix(ds3)), names(ds3))
        ds5 = sin.(view(ds1, 1, 1) .+ ds1[!, 1] .+ ds2 ./ ds3)
        ds1 .= sin.(view(ds1, 1, 1) .+ ds1[!, 1] .+ ds2 ./ ds3)
        @test ds1 == ds4 == ds5
        @test ds2 != ds6
        @test ds3 != ds7
    end

    for i in 1:10
        ds1 = Dataset(rand(100, 100), :auto)
        ds2 = copy(ds1)
        ds3 = copy(ds1)
        for i in 1:100
            ds2[!, rand(1:100)] = ds1[!, i]
            ds3[!, rand(1:100)] = ds1[!, i]
        end
        ds6 = copy(ds2)
        ds7 = copy(ds3)
        ds4 = Dataset(sin.(ds1[1, 1] .+ copy(ds1[!, 1]) .+ Matrix(ds2) ./ Matrix(ds3)), names(ds3))
        ds5 = sin.(view(ds1, 1, 1) .+ ds1[!, 1] .+ view(ds2, :, :) ./ ds3)
        ds1 .= sin.(view(ds1[!, 1], 1) .+ view(ds1[!, 1], :) .+ ds2 ./ view(ds3, :, :))
        @test ds1 == ds4 == ds5
        @test ds2 != ds6
        @test ds3 != ds7
    end

    for i in 1:10
        ds1 = Dataset(rand(100, 100), :auto)
        t1 = IMD._get_lastmodified(IMD._attributes(ds1))
        sleep(.1)

        ds2 = copy(ds1)
        ds3 = copy(ds1)
        for i in 1:100
            ds2[!, rand(1:100)] = ds1[!, i]
            ds3[!, rand(1:100)] = ds1[!, i]
        end
        ds6 = copy(ds2)
        ds7 = copy(ds3)
        ds4 = Dataset(sin.(ds1[1, 1] .+ copy(ds1[!, 1]) .+ Matrix(ds2) ./ Matrix(ds3)), names(ds3))
        ds5 = sin.(view(ds1, 1, 1) .+ ds1[!, 1] .+ view(ds2, :, :) ./ ds3)
        view(ds1, :, :) .= sin.(view(ds1[!, 1], 1) .+ view(ds1[!, 1], :) .+ ds2 ./ view(ds3, :, :))
        @test t1 != IMD._get_lastmodified(IMD._attributes(ds1))

        @test ds1 == ds4 == ds5
        @test ds2 != ds6
        @test ds3 != ds7
    end
end

@testset "@. test" begin
    ds = Dataset(rand(2, 3), :auto)
    sds = view(ds, 1:1, :)
    dsm = Matrix(ds)
    sdsm = Matrix(sds)

    r1 = @. (ds + sds + 5) / sds
    @test r1 isa Dataset

    @. ds = sin(sds / (ds + 1))
    @. dsm = sin(sdsm / (dsm + 1))
    @test ds == Dataset(dsm, names(ds))
end

@testset "test common cases" begin
    m = rand(1000, 10)
    ds = Dataset(m, :auto)
    t1 = IMD._get_lastmodified(IMD._attributes(ds))
    sleep(.1)

    @test ds .+ 1 == Dataset(m .+ 1, names(ds))
    @test ds .+ transpose(1:10) == Dataset(m .+ transpose(1:10), names(ds))
    @test ds .+ (1:1000) == Dataset(m .+ (1:1000), names(ds))
    @test ds .+ m == Dataset(m .+ m, names(ds))
    @test m .+ ds == Dataset(m .+ m, names(ds))
    @test ds .+ ds == Dataset(m .+ m, names(ds))

    ds .+= 1
    @test t1 != IMD._get_lastmodified(IMD._attributes(ds))

    m .+= 1
    @test ds == Dataset(m, names(ds))
    ds .+= transpose(1:10)
    m .+= transpose(1:10)
    @test ds == Dataset(m, names(ds))
    ds .+= (1:1000)
    m .+= (1:1000)
    @test ds == Dataset(m, names(ds))
    ds .+= ds
    m .+= m
    @test ds == Dataset(m, names(ds))
    ds2 = copy(ds)
    m2 = copy(m)
    ds .+= ds .+ ds2 .+ m2 .+ 1
    m .+= m .+ ds2 .+ m2 .+ 1
    @test ds == Dataset(m, names(ds))
end

@testset "data set only on left hand side broadcasting assignment" begin
    Random.seed!(1234)

    m = rand(3, 4);
    m = allowmissing(m)
    m2 = copy(m);
    m3 = copy(m);
    ds = Dataset(a=view(m, :, 1), b=view(m, :, 1),
                   c=view(m, :, 1), d=view(m, :, 1), copycols=false);
    ds2 = copy(ds)
    mds = Matrix(ds)

    @test m .+ ds == m2 .+ ds
    @test Matrix(m .+ ds) == m .+ mds
    @test sin.(m .+ ds) .+ 1 .+ m2 == sin.(m2 .+ ds) .+ 1 .+ m
    @test Matrix(m .+ ds ./ 2 .* ds2) == m .+ mds ./ 2 .* mds

    m2 .+= ds .+ 1 ./ ds2
    m .+= ds .+ 1 ./ ds2
    @test m2 == m
    for col in eachcol(ds)
        @test col.val == m[:, 1]
    end
    for col in eachcol(ds2)
        @test col.val == m3[:, 1]
    end

    m = rand(3, 4);
    m = allowmissing(m)
    m2 = copy(m);
    m3 = copy(m);
    ds = view(Dataset(a=view(m, :, 1), b=view(m, :, 1),
                        c=view(m, :, 1), d=view(m, :, 1), copycols=false),
              [3, 2, 1], :)
    ds2 = copy(ds)
    mds = Matrix(ds)

    @test m .+ ds == m2 .+ ds
    @test Matrix(m .+ ds) == m .+ mds
    @test sin.(m .+ ds) .+ 1 .+ m2 == sin.(m2 .+ ds) .+ 1 .+ m
    @test Matrix(m .+ ds ./ 2 .* ds2) == m .+ mds ./ 2 .* mds

    m2 .+= ds .+ 1 ./ ds2
    m .+= ds .+ 1 ./ ds2
    @test m2 == m
    for col in eachcol(ds)
        @test IMD.__!(col) == m[3:-1:1, 1]
    end
    for col in eachcol(ds2)
        @test IMD.__!(col) == m3[3:-1:1, 1]
    end
end

@testset "broadcasting with 3-dimensional object" begin
    y = zeros(4, 3, 2)
    ds = Dataset(ones(4, 3), :auto)
    @test_throws DimensionMismatch ds .+ y
    @test_throws DimensionMismatch y .+ ds
    @test_throws DimensionMismatch ds .+= y
    y .+= ds
    @test y == ones(4, 3, 2)
end

@testset "additional checks of post-! broadcasting rules" begin
    ds = copy(refds)
    v1 = ds[!, 1]
    @test_throws MethodError ds[CartesianIndex(1, 1)] .= 1
    @test_throws MethodError ds[CartesianIndex(1, 1)] .= "d"
    @test_throws DimensionMismatch ds[CartesianIndex(1, 1)] .= [1, 2]

    ds = copy(refds)
    v1 = ds[!, 1]
    @test_throws MethodError ds[1, 1] .= 1
    @test_throws MethodError ds[1, 1] .= "d"
    @test_throws DimensionMismatch ds[1, 1] .= [1, 2]

    ds = copy(refds)
    v1 = ds[!, 1]
    @test_throws MethodError ds[1, :x1] .= 1
    @test_throws MethodError ds[1, :x1] .= "d"
    @test_throws DimensionMismatch ds[1, :x1] .= [1, 2]

    ds = copy(refds)
    v1 = ds[!, 1]
    @test_throws MethodError ds[1, "x1"] .= 1
    @test_throws MethodError ds[1, "x1"] .= "d"
    @test_throws DimensionMismatch ds[1, "x1"] .= [1, 2]

    ds = copy(refds)
    v1 = ds[!, 1]
    v2 = ds[!, 2]
    ds[1, 1:2] .= 'd'
    @test v1 == [100.0, 2.5, 3.5]
    @test v2 == [100.0, 5.5, 6.5]
    @test_throws MethodError ds[1, 1:2] .= "d"
    @test v1 == [100.0, 2.5, 3.5]
    @test v2 == [100.0, 5.5, 6.5]
    ds[1, 1:2] .= 'e':'f'
    @test v1 == [101.0, 2.5, 3.5]
    @test v2 == [102.0, 5.5, 6.5]
    @test_throws DimensionMismatch ds[1, 1:2] .= ['d' 'd']
    @test v1 == [101.0, 2.5, 3.5]
    @test v2 == [102.0, 5.5, 6.5]

    ds = copy(refds)
    v1 = ds[!, 1]
    ds[:, 1] .= 'd'
    @test v1 == [100.0, 100.0, 100.0]
    @test_throws MethodError ds[:, 1] .= "d"
    @test v1 == [100.0, 100.0, 100.0]
    @test_throws DimensionMismatch ds[:, 1] .= [1 2 3]
    @test v1 == [100.0, 100.0, 100.0]

    ds = copy(refds)
    v1 = ds[!, 1]
    ds[:, :x1] .= 'd'
    @test v1 == [100.0, 100.0, 100.0]
    @test_throws MethodError ds[:, :x1] .= "d"
    @test v1 == [100.0, 100.0, 100.0]
    @test_throws DimensionMismatch ds[:, :x1] .= [1 2 3]
    @test v1 == [100.0, 100.0, 100.0]

    ds = copy(refds)
    v1 = ds[!, 1]
    ds[:, 1] .= 'd':'f'
    @test v1 == [100.0, 101.0, 102.0]
    @test_throws MethodError ds[:, 1] .= ["d", "e", "f"]
    @test v1 == [100.0, 101.0, 102.0]

    ds = copy(refds)
    t1 = IMD._get_lastmodified(IMD._attributes(ds))
    sleep(.1)

    v1 = ds[!, 1]
    v2 = ds[!, 2]
    ds[:, 1:2] .= 'd'
    @test t1 != IMD._get_lastmodified(IMD._attributes(ds))

    @test v1 == [100.0, 100.0, 100.0]
    @test v2 == [100.0, 100.0, 100.0]
    @test_throws MethodError ds[:, 1:2] .= "d"
    @test v1 == [100.0, 100.0, 100.0]
    @test v2 == [100.0, 100.0, 100.0]
    @test_throws DimensionMismatch ds[:, 1:2] .= [1 2 3]
    @test v1 == [100.0, 100.0, 100.0]
    @test v2 == [100.0, 100.0, 100.0]

    ds = copy(refds)
    v1 = ds[!, 1]
    v2 = ds[!, 2]
    ds[:, 1:2] .= 'd':'f'
    @test v1 == [100.0, 101.0, 102.0]
    @test v2 == [100.0, 101.0, 102.0]
    @test_throws MethodError ds[:, 1:2] .= ["d", "e", "f"]
    @test v1 == [100.0, 101.0, 102.0]
    @test v2 == [100.0, 101.0, 102.0]

    ds = copy(refds)
    v1 = ds[!, 1]
    v2 = ds[!, 2]
    ds[:, 1:2] .= permutedims('d':'e')
    @test v1 == [100.0, 100.0, 100.0]
    @test v2 == [101.0, 101.0, 101.0]

    ds = copy(refds)
    v1 = ds[!, 1]
    v2 = ds[!, 2]
    ds[:, 1:2] .= reshape('d':'i', 3, :)
    @test v1 == [100.0, 101.0, 102.0]
    @test v2 == [103.0, 104.0, 105.0]
    @test_throws DimensionMismatch ds[:, 1:2] .= reshape('d':'i', 1, :, 3)
    @test v1 == [100.0, 101.0, 102.0]
    @test v2 == [103.0, 104.0, 105.0]

    ds = copy(refds)
    v1 = ds[!, 1]
    v1′ = ds[:, 1]
    ds[!, 1] .= 100.0
    @test ds.x1 == [100.0, 100.0, 100.0]
    @test v1 == v1′
    ds[!, 1] .= 'd'
    @test ds.x1 == ['d', 'd', 'd']
    @test v1 == v1′
    @test_throws DimensionMismatch ds[!, 1] .= [1 2 3]
    @test ds.x1 == ['d', 'd', 'd']
    @test v1 == v1′

    ds = copy(refds)
    v1 = ds[!, 1]
    v1′ = ds[:, 1]
    ds[!, :x1] .= 100.0
    @test ds.x1 == [100.0, 100.0, 100.0]
    @test v1 == v1′
    ds[!, :x1] .= 'd'
    @test ds.x1 == ['d', 'd', 'd']
    @test v1 == v1′
    @test_throws DimensionMismatch ds[!, :x1] .= [1 2 3]
    @test ds.x1 == ['d', 'd', 'd']
    @test v1 == v1′

    ds = copy(refds)
    ds[!, :newcol] .= 100.0
    @test ds.newcol == [100.0, 100.0, 100.0]
    @test ds[:, 1:end-1] == refds

    ds = copy(refds)
    ds[!, "newcol"] .= 100.0
    @test ds.newcol == [100.0, 100.0, 100.0]
    @test ds[:, 1:end-1] == refds

    ds = copy(refds)
    ds[!, :newcol] .= 'd'
    @test ds.newcol == ['d', 'd', 'd']
    @test ds[:, 1:end-1] == refds

    ds = copy(refds)
    ds[!, "newcol"] .= 'd'
    @test ds.newcol == ['d', 'd', 'd']
    @test ds[:, 1:end-1] == refds

    ds = copy(refds)
    @test_throws DimensionMismatch ds[!, :newcol] .= [1 2 3]
    @test ds == refds

    ds = copy(refds)
    @test_throws DimensionMismatch ds[!, "newcol"] .= [1 2 3]
    @test ds == refds

    ds = copy(refds)
    @test_throws ArgumentError ds[!, 10] .= 'a'
    @test ds == refds
    @test_throws ArgumentError ds[!, 10] .= [1, 2, 3]
    @test ds == refds
    @test_throws ArgumentError ds[!, 10] .= [1 2 3]
    @test ds == refds

    ds = copy(refds)
    ds[!, 1:2] .= 'a'
    @test Matrix(ds) == ['a'  'a'  7.5  10.5  13.5
                         'a'  'a'  8.5  11.5  14.5
                         'a'  'a'  9.5  12.5  15.5]

    ds = copy(refds)
    v1 = ds[!, 1]
    ds[:, :x1] .= 'd'
    @test v1.val == [100.0, 100.0, 100.0]
    @test_throws MethodError ds[:, 1] .= "d"
    @test v1.val == [100.0, 100.0, 100.0]
    @test_throws DimensionMismatch ds[:, 1] .= [1 2 3]
    @test v1.val == [100.0, 100.0, 100.0]

    ds = copy(refds)
    if isdefined(Base, :dotgetproperty)
        ds.newcol .= 'd'
        @test ds == [refds Dataset(newcol=fill('d', 3))]
    else
        @test_throws ArgumentError ds.newcol .= 'd'
        @test ds == refds
    end

    ds = view(copy(refds), :, :)
    v1 = ds[!, 1]
    @test_throws MethodError ds[CartesianIndex(1, 1)] .= 1
    @test_throws MethodError ds[CartesianIndex(1, 1)] .= "d"
    @test_throws DimensionMismatch ds[CartesianIndex(1, 1)] .= [1, 2]

    ds = view(copy(refds), :, :)
    v1 = ds[!, 1]
    @test_throws MethodError ds[1, 1] .= 1
    @test_throws MethodError ds[1, 1] .= "d"
    @test_throws DimensionMismatch ds[1, 1] .= [1, 2]

    ds = view(copy(refds), :, :)
    v1 = ds[!, 1]
    @test_throws MethodError ds[1, :x1] .= 1
    @test_throws MethodError ds[1, :x1] .= "d"
    @test_throws DimensionMismatch ds[1, :x1] .= [1, 2]

    ds = view(copy(refds), :, :)
    v1 = ds[!, 1]
    v2 = ds[!, 2]
    ds[1, 1:2] .= 'd'
    @test v1 == [100.0, 2.5, 3.5]
    @test v2 == [100.0, 5.5, 6.5]
    @test_throws MethodError ds[1, 1:2] .= "d"
    @test v1 == [100.0, 2.5, 3.5]
    @test v2 == [100.0, 5.5, 6.5]
    ds[1, 1:2] .= 'e':'f'
    @test v1 == [101.0, 2.5, 3.5]
    @test v2 == [102.0, 5.5, 6.5]
    @test_throws DimensionMismatch ds[1, 1:2] .= ['d' 'd']
    @test v1 == [101.0, 2.5, 3.5]
    @test v2 == [102.0, 5.5, 6.5]

    ds = view(copy(refds), :, :)
    v1 = ds[!, 1]
    ds[:, 1] .= 'd'
    @test v1 == [100.0, 100.0, 100.0]
    @test_throws MethodError ds[:, 1] .= "d"
    @test v1 == [100.0, 100.0, 100.0]
    @test_throws DimensionMismatch ds[:, 1] .= [1 2 3]
    @test v1 == [100.0, 100.0, 100.0]

    ds = view(copy(refds), :, :)
    v1 = ds[!, 1]
    ds[:, :x1] .= 'd'
    @test v1 == [100.0, 100.0, 100.0]
    @test_throws MethodError ds[:, :x1] .= "d"
    @test v1 == [100.0, 100.0, 100.0]
    @test_throws DimensionMismatch ds[:, :x1] .= [1 2 3]
    @test v1 == [100.0, 100.0, 100.0]

    ds = view(copy(refds), :, :)
    v1 = ds[!, 1]
    ds[:, 1] .= 'd':'f'
    @test v1 == [100.0, 101.0, 102.0]
    @test_throws MethodError ds[:, 1] .= ["d", "e", "f"]
    @test v1 == [100.0, 101.0, 102.0]

    ds = view(copy(refds), :, :)
    v1 = ds[!, 1]
    v2 = ds[!, 2]
    ds[:, 1:2] .= 'd'
    @test v1 == [100.0, 100.0, 100.0]
    @test v2 == [100.0, 100.0, 100.0]
    @test_throws MethodError ds[:, 1:2] .= "d"
    @test v1 == [100.0, 100.0, 100.0]
    @test v2 == [100.0, 100.0, 100.0]
    @test_throws DimensionMismatch ds[:, 1:2] .= [1 2 3]
    @test v1 == [100.0, 100.0, 100.0]
    @test v2 == [100.0, 100.0, 100.0]

    ds = view(copy(refds), :, :)
    v1 = ds[!, 1]
    v2 = ds[!, 2]
    ds[:, 1:2] .= 'd':'f'
    @test v1 == [100.0, 101.0, 102.0]
    @test v2 == [100.0, 101.0, 102.0]
    @test_throws MethodError ds[:, 1:2] .= ["d", "e", "f"]
    @test v1 == [100.0, 101.0, 102.0]
    @test v2 == [100.0, 101.0, 102.0]

    ds = view(copy(refds), :, :)
    v1 = ds[!, 1]
    v2 = ds[!, 2]
    ds[:, 1:2] .= permutedims('d':'e')
    @test v1 == [100.0, 100.0, 100.0]
    @test v2 == [101.0, 101.0, 101.0]

    ds = view(copy(refds), :, :)
    v1 = ds[!, 1]
    v2 = ds[!, 2]
    ds[:, 1:2] .= reshape('d':'i', 3, :)
    @test v1 == [100.0, 101.0, 102.0]
    @test v2 == [103.0, 104.0, 105.0]
    @test_throws DimensionMismatch ds[:, 1:2] .= reshape('d':'i', 1, :, 3)
    @test v1 == [100.0, 101.0, 102.0]
    @test v2 == [103.0, 104.0, 105.0]

    ds = view(copy(refds), :, :)
    ds[!, 1] .= 100
    @test parent(ds).x1 == [100, 100, 100]
    @test eltype(parent(ds).x1) == Union{Missing, Float64}

    ds = view(copy(refds), :, :)
    ds[!, :x1] .= 100.0
    @test parent(ds).x1 == [100, 100, 100]
    @test eltype(parent(ds).x1) == Union{Missing, Float64}

    # we don't support adding :newcol to views
    # ds = view(copy(refds), :, :)
    # ds[!, :newcol] .= 100.0
    # @test parent(ds).newcol == [100, 100, 100]
    # @test eltype(parent(ds).newcol) == Union{Float64, Missing}

    ds = view(copy(refds), :, :)
    @test_throws BoundsError ds[!, 10] .= 'a'
    @test ds == refds
    @test_throws BoundsError ds[!, 10] .= [1, 2, 3]
    @test ds == refds
    @test_throws BoundsError ds[!, 10] .= [1 2 3]
    @test ds == refds

    ds = view(copy(refds), :, :)
    ds[!, 1:2] .= 'a'
    @test parent(ds).x1.val == parent(ds).x2.val == [97.0,97,97]

    ds = view(copy(refds), :, :)
    v1 = ds[!, 1]
    ds.x1 .= 'd'
    @test v1 == [100.0, 100.0, 100.0]
    @test_throws MethodError ds[:, 1] .= "d"
    @test v1 == [100.0, 100.0, 100.0]
    @test_throws DimensionMismatch ds[:, 1] .= [1 2 3]
    @test v1 == [100.0, 100.0, 100.0]

    # ds = view(copy(refds), :, :)
    # if VERSION >= v"1.7"
    #     ds.newcol .= 'd'
    #     @test ds.newcol == fill('d', 3)
    # else
    #     @test_throws ArgumentError ds.newcol .= 'd'
    #     @test ds == refds
    # end
end

@testset "DatasetRow getproperty broadcasted assignment" begin
    ds = Dataset(a=[[1, 2], [3, 4]], b=[[5, 6], [7, 8]])
    dsr = ds[1, :]
    dsr.a .= 10
    @test ds == Dataset(a=[[10, 10], [3, 4]], b=[[5, 6], [7, 8]])
    @test_throws MethodError dsr.a .= ["a", "b"]

    ds = Dataset(a=[[1, 2], [3, 4]], b=[[5, 6], [7, 8]])
    dsr = ds[1, 1:1]
    dsr.a .= 10
    @test ds == Dataset(a=[[10, 10], [3, 4]], b=[[5, 6], [7, 8]])
    @test_throws MethodError dsr.a .= ["a", "b"]

    ds = Dataset(a=[[1, 2], [3, 4]], b=[[5, 6], [7, 8]])
    dsr = ds[1, :]
    dsr."a" .= 10
    @test ds == Dataset(a=[[10, 10], [3, 4]], b=[[5, 6], [7, 8]])
    @test_throws MethodError dsr."a" .= ["a", "b"]

    ds = Dataset(a=[[1, 2], [3, 4]], b=[[5, 6], [7, 8]])
    dsr = ds[1, 1:1]
    dsr."a" .= 10
    @test ds == Dataset(a=[[10, 10], [3, 4]], b=[[5, 6], [7, 8]])
    @test_throws MethodError dsr."a" .= ["a", "b"]
end

@testset "make sure that : is in place and ! allocates" begin
    ds = Dataset(a=[1, 2, 3])
    a = ds.a
    ds[:, :a] .+= 1
    @test a == [2, 3, 4]
    @test ds.a === a
    ds[!, :a] .+= 1
    @test a == [2, 3, 4]
    @test ds.a == [3, 4, 5]
    @test ds.a !== a

    ds = Dataset(a=[1, 2, 3])
    a = ds.a
    ds[:, "a"] .+= 1
    @test a == [2, 3, 4]
    @test ds.a === a
    ds[!, "a"] .+= 1
    @test a == [2, 3, 4]
    @test ds.a == [3, 4, 5]
    @test ds.a !== a
end

@testset "add new correct rules for ds[row, col] .= v broadcasting" begin
    for v in [:a, "a"]
        ds = Dataset(a=1)
        @test_throws MethodError ds[1, 1] .= 10
        @test_throws MethodError ds[1, v] .= 10
        @test_throws MethodError ds[CartesianIndex(1, 1)] .= 10
        ds = Dataset(a=[[1, 2, 3]])
        ds[1, 1] .= 10
        @test ds == Dataset(a=[[10, 10, 10]])
        ds[1, v] .= 100
        @test ds == Dataset(a=[[100, 100, 100]])
        ds[CartesianIndex(1, 1)] .= 1000
        @test ds == Dataset(a=[[1000, 1000, 1000]])
    end
end

@testset "broadcasting into ds[!, cols]" begin
    for selector in [1:2, Between(:x1, :x2), Not(r"x3"), [:x1, :x2],
                     ["x1", "x2"], Between("x1", "x2")]
        ds = Dataset(x1=1:3, x2=4:6)
        ds[!, selector] .= "a"
        @test ds == Dataset(fill("a", 3, 2), :auto)
        @test ds.x1 !== ds.x2

        ds = Dataset(x1=1:3, x2=4:6)
        ds[!, selector] .= Ref((a=1, b=2))
        @test ds == Dataset(fill((a=1, b=2), 3, 2), :auto)
        @test ds.x1 !== ds.x2

        ds = Dataset(x1=1:3, x2=4:6)
        ds[!, selector] .= ["a" "b"]
        @test ds == Dataset(["a" "b"
                               "a" "b"
                               "a" "b"], :auto)
        @test ds.x1 !== ds.x2

        ds = Dataset(x1=1:3, x2=4:6)
        ds[!, selector] .= ["a", "b", "c"]
        @test ds == Dataset(["a" "a"
                               "b" "b"
                               "c" "c"], :auto)
        @test ds.x1 !== ds.x2

        ds = Dataset(x1=1:3, x2=4:6)
        ds[!, selector] .= categorical(["a"])
        @test ds == Dataset(["a" "a"
                               "a" "a"
                               "a" "a"], :auto)
        @test ds.x1.val isa CategoricalVector
        @test ds.x2.val isa CategoricalVector
        @test ds.x1 !== ds.x2

        ds = Dataset(x1=1:3, x2=4:6)
        ds[!, selector] .= Dataset(["a" "b"], :auto)
        @test ds == Dataset(["a" "b"
                               "a" "b"
                               "a" "b"], :auto)
        @test ds.x1 !== ds.x2

        ds = Dataset(x1=1:3, x2=4:6)
        ds[!, selector] .= Dataset(["a" "d"
                                      "b" "e"
                                      "c" "f"], :auto)
        @test ds == Dataset(["a" "d"
                               "b" "e"
                               "c" "f"], :auto)
        @test ds.x1 !== ds.x2

        ds = Dataset(x1=1:3, x2=4:6)
        ds[!, selector] .= ["a" "d"
                            "b" "e"
                            "c" "f"]
        @test ds == Dataset(["a" "d"
                               "b" "e"
                               "c" "f"], :auto)
        @test ds.x1 !== ds.x2

        ds = Dataset(x1=1:3, x2=4:6, x3=1)
        ds[!, selector] .= "a"
        @test ds == Dataset(["a" "a" 1
                               "a" "a" 1
                               "a" "a" 1], :auto)
        @test ds.x1 !== ds.x2

        ds = Dataset(x1=1:3, x2=4:6, x3=1)
        ds[!, selector] .= Ref((a=1, b=2))
        @test ds[:, 1:2] == Dataset(fill((a=1, b=2), 3, 2), :auto)
        @test ds[:, 3] == [1, 1, 1]
        @test ds.x1 !== ds.x2

        ds = Dataset(x1=1:3, x2=4:6, x3=1)
        ds[!, selector] .= ["a" "b"]
        @test ds == Dataset(["a" "b" 1
                               "a" "b" 1
                               "a" "b" 1], :auto)
        @test ds.x1 !== ds.x2

        ds = Dataset(x1=1:3, x2=4:6, x3=1)
        ds[!, selector] .= ["a", "b", "c"]
        @test ds == Dataset(["a" "a" 1
                               "b" "b" 1
                               "c" "c" 1], :auto)
        @test ds.x1 !== ds.x2

        ds = Dataset(x1=1:3, x2=4:6, x3=1)
        ds[!, selector] .= categorical(["a"])
        @test ds == Dataset(["a" "a" 1
                               "a" "a" 1
                               "a" "a" 1], :auto)
        @test ds.x1.val isa CategoricalVector
        @test ds.x2.val isa CategoricalVector
        @test ds.x1 !== ds.x2

        ds = Dataset(x1=1:3, x2=4:6, x3=1)
        ds[!, selector] .= Dataset(["a" "b"], :auto)
        @test ds == Dataset(["a" "b" 1
                               "a" "b" 1
                               "a" "b" 1], :auto)
        @test ds.x1 !== ds.x2

        ds = Dataset(x1=1:3, x2=4:6, x3=1)
        ds[!, selector] .= Dataset(["a" "d"
                                      "b" "e"
                                      "c" "f"], :auto)
        @test ds == Dataset(["a" "d" 1
                               "b" "e" 1
                               "c" "f" 1], :auto)
        @test ds.x1 !== ds.x2

        ds = Dataset(x1=1:3, x2=4:6, x3=1)
        ds[!, selector] .= ["a" "d"
                            "b" "e"
                            "c" "f"]
        @test ds == Dataset(["a" "d" 1
                               "b" "e" 1
                               "c" "f" 1], :auto)
        @test ds.x1 !== ds.x2
    end

    ds = Dataset(x1=1:3, x2=4:6)
    @test_throws ArgumentError ds[!, [:x1, :x3]] .= "a"
end

@testset "broadcasting over heterogenous columns" begin
    ds = Dataset(x=[1, 1.0, big(1), "1"])
    f_identity(x) = x
    @test ds == f_identity.(ds)
end

@testset "@views on ds[!, col]" begin
    ds = Dataset(ones(3, 4), :auto)
    @views ds[!, 1] .+= 1
    @test ds[!, 1] == [2.0, 2.0, 2.0]
    @views ds[:, 2] .= ds[!, 4] .+ ds[!, 3]
    @test ds[!, 2] == [2.0, 2.0, 2.0]

    # make sure we do not mess with maybeview
    @test @views typeof(ds[!, 1:2]) <: SubDataset
end

@testset "broadcasting of ds[:, col] = value" begin
    ds = Dataset(ones(3, 4), :auto)
    z = ["a", "b", "c"]
    ds[:, :z] .= z
    @test ds.z == z
    @test ds.z !== z
    @test_throws ArgumentError ds[:, 6] .= z
    @test_throws MethodError ds[:, 1] .= z

    ds = Dataset(ones(3, 4), :auto)
    z = "abc"
    ds[:, :z] .= z
    @test ds.z == fill("abc", 3)
    @test_throws ArgumentError ds[:, 6] .= z
    @test_throws MethodError ds[:, 1] .= z

    ds = Dataset(ones(3, 4), :auto)
    z = fill("abc", 1, 1, 2)
    @test_throws DimensionMismatch ds[:, :z] .= z

    ds = Dataset(ones(3, 4), :auto)
    z = ["a", "b", "c"]
    ds[:, "z"] .= z
    @test ds.z == z
    @test ds.z !== z

    ds = Dataset(ones(3, 4), :auto)
    z = "abc"
    ds[:, "z"] .= z
    @test ds.z == fill("abc", 3)

    ds = Dataset(ones(3, 4), :auto)
    z = fill("abc", 1, 1, 2)
    @test_throws DimensionMismatch ds[:, "z"] .= z
end

@testset "broadcasting of getproperty" begin
    if isdefined(Base, :dotgetproperty)
        ds = Dataset(a=1:4)
        ds.b .= 1
        ds.c .= 4:-1:1
        # TODO: enable this in the future when the deprecation period is finished
        # ds.a .= 'a':'d'
        # @test ds.a isa Vector{Char}
        # @test ds == Dataset(a='a':'d', b=1, c=4:-1:1)
        # dsv = view(ds, 2:3, 2:3)
        # @test_throws ArgumentError dsv.b .= 0
    end
end
