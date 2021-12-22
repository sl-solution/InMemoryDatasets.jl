using InMemoryDatasets, Random, Test

# From DataFrames.jl

@testset "copy - SubDataset" begin
    ds = Dataset(x=1:10, y=1.0:10.0)
    sds = view(ds, 1:2, 1:1)
    @test sds isa SubDataset
    @test copy(sds) isa Dataset
    @test sds == copy(sds)
    @test view(sds, :, :) === sds
    @test view(sds, :, r"") == sds
end

@testset "view -- Dataset" begin
    ds = Dataset(x=1:10, y=1.0:10.0)
    @test view(ds, 1, :) == IMD.DatasetRow(ds, 1, :)
    @test view(ds, UInt(1), :) == IMD.DatasetRow(ds, 1, :)
    @test view(ds, BigInt(1), :) == IMD.DatasetRow(ds, 1, :)
    @test view(ds, UInt(1):UInt(1), :) == SubDataset(ds, 1:1, :)
    @test view(ds, BigInt(1):BigInt(1), :) == SubDataset(ds, 1:1, :)
    @test view(ds, 1:2, :) == first(ds, 2)
    @test view(ds, vcat(trues(2), falses(8)), :) == first(ds, 2)
    @test view(ds, [1, 2], :) == first(ds, 2)

    @test view(ds, 1, r"") == IMD.DatasetRow(ds, 1, :)
    @test view(ds, UInt(1), r"") == IMD.DatasetRow(ds, 1, :)
    @test view(ds, BigInt(1), r"") == IMD.DatasetRow(ds, 1, :)
    @test view(ds, UInt(1):UInt(1), r"") == SubDataset(ds, 1:1, :)
    @test view(ds, BigInt(1):BigInt(1), r"") == SubDataset(ds, 1:1, :)
    @test view(ds, 1:2, r"") == first(ds, 2)
    @test view(ds, vcat(trues(2), falses(8)), r"") == first(ds, 2)
    @test view(ds, [1, 2], r"") == first(ds, 2)

    @test view(ds, 1, r"") == IMD.DatasetRow(ds, 1, r"")
    @test view(ds, UInt(1), r"") == IMD.DatasetRow(ds, 1, r"")
    @test view(ds, BigInt(1), r"") == IMD.DatasetRow(ds, 1, r"")
    @test view(ds, UInt(1):UInt(1), r"") == SubDataset(ds, 1:1, r"")
    @test view(ds, BigInt(1):BigInt(1), r"") == SubDataset(ds, 1:1, r"")

    @test view(ds, 1, :x) == view(ds[!, :x], 1)
    @test view(ds, 1, :x) isa SubDatasetColumn
    @test size(view(ds, 1, :x)) == ()
    @test view(ds, 1:2, :x) == ds[!, :x][1:2]
    @test view(ds, 1:2, :x) isa SubDatasetColumn
    @test view(ds, vcat(trues(2), falses(8)), :x) == view(ds[!, :x], vcat(trues(2), falses(8)))
    @test view(ds, [1, 2], :x) == view(ds[!, :x], [1, 2])
    @test view(ds, 1, 1) == view(ds[!, 1], 1)
    @test view(ds, 1, 1) isa SubDatasetColumn
    @test size(view(ds, 1, 1)) == ()
    @test view(ds, 1:2, 1) == ds[!, 1][1:2]
    @test view(ds, 1:2, 1) isa SubDatasetColumn
    @test view(ds, vcat(trues(2), falses(8)), 1) == view(ds[!, 1], vcat(trues(2), falses(8)))
    @test view(ds, [1, 2], 1) == view(ds[!, 1], [1, 2])
    @test view(ds, 1:2, 1) == ds[!, 1][1:2]
    @test view(ds, 1:2, 1) isa SubDatasetColumn

    @test view(ds, 1, [:x, :y]) == IMD.DatasetRow(ds[:, [:x, :y]], 1, :)
    @test view(ds, 1, [:x, :y]) == IMD.DatasetRow(ds, 1, [:x, :y])
    @test view(ds, 1:2, [:x, :y]) == first(ds, 2)
    @test view(ds, vcat(trues(2), falses(8)), [:x, :y]) == first(ds, 2)
    @test view(ds, [1, 2], [:x, :y]) == first(ds, 2)

    @test view(ds, 1, r"[xy]") == IMD.DatasetRow(ds[:, [:x, :y]], 1, :)
    @test view(ds, 1, r"[xy]") == IMD.DatasetRow(ds, 1, [:x, :y])
    @test view(ds, 1:2, r"[xy]") == first(ds, 2)
    @test view(ds, vcat(trues(2), falses(8)), r"[xy]") == first(ds, 2)
    @test view(ds, [1, 2], r"[xy]") == first(ds, 2)

    @test view(ds, 1, [1, 2]) == IMD.DatasetRow(ds[:, 1:2], 1, :)
    @test view(ds, 1, [1, 2]) == IMD.DatasetRow(ds, 1, 1:2)
    @test view(ds, 1:2, [1, 2]) == first(ds, 2)
    @test view(ds, vcat(trues(2), falses(8)), [1, 2]) == first(ds, 2)
    @test view(ds, [1, 2], [1, 2]) == first(ds, 2)

    @test view(ds, 1, trues(2)) == IMD.DatasetRow(ds[:, trues(2)], 1, :)
    @test view(ds, 1, trues(2)) == IMD.DatasetRow(ds, 1, trues(2))
    @test view(ds, 1:2, trues(2)) == first(ds, 2)
    @test view(ds, vcat(trues(2), falses(8)), trues(2)) == first(ds, 2)
    @test view(ds, [1, 2], trues(2)) == first(ds, 2)

    @test view(ds, Integer[1, 2], :) == first(ds, 2)
    @test view(ds, UInt[1, 2], :) == first(ds, 2)
    @test view(ds, BigInt[1, 2], :) == first(ds, 2)
    @test view(ds, Union{Int, Missing}[1, 2], :) == first(ds, 2)
    @test view(ds, Union{Integer, Missing}[1, 2], :) == first(ds, 2)
    @test view(ds, Union{UInt, Missing}[1, 2], :) == first(ds, 2)
    @test view(ds, Union{BigInt, Missing}[1, 2], :) == first(ds, 2)

    @test view(ds, :, :) == ds
    @test view(ds, 1, :) == IMD.DatasetRow(ds, 1, :)
    @test view(ds, :, 1) == ds[:, 1]
    @test view(ds, :, 1) isa SubDatasetColumn

    @test view(ds, 1, r"") == IMD.DatasetRow(ds, 1, :)
    @test view(ds, :, 1) == ds[:, 1]
    @test view(ds, :, 1) isa SubDatasetColumn

    @test size(view(ds, :, r"a")) == (0, 0)
    @test size(view(ds, 1:2, r"a")) == (0, 0)
    @test size(view(ds, 2, r"a")) == (0,)

    @test_throws ArgumentError view(ds, :, [missing, 1])
    @test_throws ArgumentError view(ds, [missing, 1], :)
    @test_throws ArgumentError view(ds, [missing, 1], r"")
end

@testset "view -- SubDataset" begin
    ds = view(Dataset(x=1:10, y=1.0:10.0), 1:10, :)

    @test view(ds, 1, :) == IMD.DatasetRow(ds, 1, :)
    @test view(ds, UInt(1), :) == IMD.DatasetRow(ds, 1, :)
    @test view(ds, BigInt(1), :) == IMD.DatasetRow(ds, 1, :)
    @test view(ds, 1:2, :) == first(ds, 2)
    @test view(ds, vcat(trues(2), falses(8)), :) == first(ds, 2)
    @test view(ds, [1, 2], :) == first(ds, 2)

    @test view(ds, 1, r"") == IMD.DatasetRow(ds, 1, :)
    @test view(ds, UInt(1), r"") == IMD.DatasetRow(ds, 1, :)
    @test view(ds, BigInt(1), r"") == IMD.DatasetRow(ds, 1, :)
    @test view(ds, 1:2, r"") == first(ds, 2)
    @test view(ds, vcat(trues(2), falses(8)), r"") == first(ds, 2)
    @test view(ds, [1, 2], r"") == first(ds, 2)

    @test view(ds, 1, r"") == IMD.DatasetRow(ds, 1, r"")
    @test view(ds, UInt(1), r"") == IMD.DatasetRow(ds, 1, r"")
    @test view(ds, BigInt(1), r"") == IMD.DatasetRow(ds, 1, r"")

    @test view(ds, 1, :x) == view(ds[!, :x], 1)
    @test view(ds, 1, 1) isa SubDatasetColumn
    @test size(view(ds, 1, 1)) == ()
    @test view(ds, 1:2, :x) == view(ds[!, :x], 1:2)
    @test view(ds, vcat(trues(2), falses(8)), :x) == view(ds[!, :x], vcat(trues(2), falses(8)))
    @test view(ds, [1, 2], :x) == view(ds[!, :x], [1, 2])

    @test view(ds, 1, 1) == view(ds[!, 1], 1)
    @test view(ds, 1, 1) isa SubDatasetColumn
    @test size(view(ds, 1, 1)) == ()
    @test view(ds, 1:2, 1) == view(ds[!, :x], 1:2)
    @test view(ds, vcat(trues(2), falses(8)), 1) == view(ds[!, :x], vcat(trues(2), falses(8)))
    @test view(ds, [1, 2], 1) == view(ds[!, :x], [1, 2])

    @test view(ds, 1, [:x, :y]) == IMD.DatasetRow(ds[:, [:x, :y]], 1, :)
    @test view(ds, 1, [:x, :y]) == IMD.DatasetRow(ds, 1, [:x, :y])
    @test view(ds, 1:2, [:x, :y]) == first(ds, 2)

    @test view(ds, 1, r"[xy]") == IMD.DatasetRow(ds[:, [:x, :y]], 1, :)
    @test view(ds, 1, r"[xy]") == IMD.DatasetRow(ds, 1, [:x, :y])
    @test view(ds, 1:2, r"[xy]") == first(ds, 2)

    @test view(ds, vcat(trues(2), falses(8)), [:x, :y]) == first(ds, 2)
    @test view(ds, [1, 2], [:x, :y]) == first(ds, 2)
    @test view(ds, 1, [1, 2]) == IMD.DatasetRow(ds[:, 1:2], 1, :)
    @test view(ds, 1, [1, 2]) == IMD.DatasetRow(ds, 1, 1:2)
    @test view(ds, 1:2, [1, 2]) == first(ds, 2)
    @test view(ds, vcat(trues(2), falses(8)), [1, 2]) == first(ds, 2)
    @test view(ds, [1, 2], [1, 2]) == first(ds, 2)
    @test view(ds, 1, trues(2)) == IMD.DatasetRow(ds[:, trues(2)], 1, :)
    @test view(ds, 1, trues(2)) == IMD.DatasetRow(ds, 1, trues(2))
    @test view(ds, 1:2, trues(2)) == first(ds, 2)
    @test view(ds, vcat(trues(2), falses(8)), trues(2)) == first(ds, 2)
    @test view(ds, [1, 2], trues(2)) == first(ds, 2)
    @test view(ds, Integer[1, 2], :) == first(ds, 2)
    @test view(ds, UInt[1, 2], :) == first(ds, 2)
    @test view(ds, BigInt[1, 2], :) == first(ds, 2)
    @test view(ds, Union{Int, Missing}[1, 2], :) == first(ds, 2)
    @test view(ds, Union{Integer, Missing}[1, 2], :) == first(ds, 2)
    @test view(ds, Union{UInt, Missing}[1, 2], :) == first(ds, 2)
    @test view(ds, Union{BigInt, Missing}[1, 2], :) == first(ds, 2)

    @test view(ds, :, :) == ds
    @test view(ds, 1, :) == IMD.DatasetRow(ds, 1, :)
    @test view(ds, :, 1) == ds[:, 1]
    @test view(ds, :, 1) isa SubDatasetColumn

    @test view(ds, :, r"") == ds
    @test view(ds, 1, r"") == IMD.DatasetRow(ds, 1, :)

    @test_throws ArgumentError view(ds, :, [missing, 1])
    @test_throws ArgumentError view(ds, [missing, 1], :)
    @test_throws ArgumentError view(ds, [missing, 1], r"")
    @test_throws ArgumentError view(ds, :, true)
end

@testset "getproperty, setproperty! and propertynames" begin
    x = collect(1:10)
    y = collect(1.0:10.0)
    ds = view(Dataset(:x=>x, :y=>y), 2:6, :)

    @test propertynames(ds) == Symbol.(names(ds))

    @test ds.x == 2:6
    @test ds.y == 2:6
    @test_throws ArgumentError ds.z

    ds[:, :x] = 1:5
    @test ds.x == 1:5
    @test parent(ds)[:, :x] == [1; 1:5; 7:10]
    ds[:, :y] .= 1
    @test ds.y == [1, 1, 1, 1, 1]
    @test parent(ds)[:, :y] == [1; 1; 1; 1; 1; 1; 7:10]
end

@testset "index" begin
    y = 1.0:10.0
    ds = view(Dataset(y=y), 2:6, :)
    ds2 = view(Dataset(x=y, y=y), 2:6, 2:2)
    @test IMD.index(ds) == IMD.index(ds2)
    @test haskey(IMD.index(ds2), :y)
    @test !haskey(IMD.index(ds2), :x)
    @test haskey(IMD.index(ds2), 1)
    @test !haskey(IMD.index(ds2), 2)
    @test !haskey(IMD.index(ds2), 0)
    @test_throws ArgumentError haskey(IMD.index(ds2), true)
    @test names(IMD.index(ds2)) == ["y"]
    @test IMD._names(IMD.index(ds2)) == [:y]

    x = Dataset(ones(5, 4), :auto)
    ds = view(x, 2:3, 2:3)
    @test names(ds) == names(x)[2:3]
    ds = view(x, 2:3, [4, 2])
    @test names(ds) == names(x)[[4, 2]]
end

@testset "parent" begin
    ds = Dataset(a=Union{Int, Missing}[1, 2, 3, 1, 2, 2],
                   b=[2.0, missing, 1.2, 2.0, missing, missing],
                   c=["A", "B", "C", "A", "B", missing])
    @test parent(view(ds, [4, 2], :)) === ds
    @test parent(view(ds, [4, 2], r"")) === ds
    @test parentindices(view(ds, [4, 2], :)) == ([4, 2], Base.OneTo(3))
    @test parentindices(view(ds, [4, 2], r"")) == ([4, 2], [1, 2, 3])
    @test parent(view(ds, [4, 2], 1:3)) === ds
    @test parentindices(view(ds, [4, 2], 1:3)) == ([4, 2], Base.OneTo(3))
end

@testset "duplicate column" begin
    ds = Dataset([11:16 21:26 31:36 41:46], :auto)
    @test_throws ArgumentError view(ds, [3, 1, 4], [3, 3, 3])
end

@testset "conversion to Dataset" begin
    ds = Dataset([11:16 21:26 31:36 41:46], :auto)
    sds = view(ds, [3, 1, 4], [3, 2, 1])
    ds2 = Dataset(sds)
    @test ds2 isa Dataset
    @test ds2 == ds[[3, 1, 4], [3, 2, 1]]
    @test all(x -> IMD.__!(x) isa Vector{Union{Missing, Int}}, eachcol(ds2))
    ds2 = convert(Dataset, sds)
    @test ds2 isa Dataset
    @test ds2 == ds[[3, 1, 4], [3, 2, 1]]
    @test all(x -> IMD.__!(x) isa Vector{Union{Missing, Int}}, eachcol(ds2))

    ds = Dataset(x=1:4, y=11:14, z=21:24)
    sds = @view ds[2:3, [2]]
    ds2 = Dataset(sds)
    @test size(ds2) == (2, 1)
    @test ds2.y.val isa Vector{Union{Missing, Int}}
    @test ds2.y == [12, 13]

end

@testset "setindex! in view" begin
    ds = Dataset(A=Vector{Union{Int, Missing}}(1:4), B=Union{String, Missing}["M", "F", "F", "M"])

    s1 = view(ds, 1:3, :)
    s1[2, :A] = 4
    @test ds[2, :A] == 4
    @test view(s1, 1:2, :) == view(ds, 1:2, :)

    s2 = view(ds, 1:2:3, :)
    s2[2, :B] = "M"
    @test ds[3, :B] == "M"
    @test view(s2, 1:1:2, :) == view(ds, [1, 3], :)
end

@testset "SubDataset corner cases" begin
    ds = Dataset(a=[1, 2], b=[3, 4])
    @test_throws ArgumentError SubDataset(ds, Integer[true], 1)
    @test_throws ArgumentError SubDataset(ds, [true], 1)

    sds = @view ds[:, :]
    @test_throws ArgumentError SubDataset(sds, true, 1)
    @test_throws ArgumentError SubDataset(sds, true, :)
    @test_throws ArgumentError SubDataset(sds, Integer[true], 1)

    if VERSION >= v"1.7.0-DEV"
        @test_throws ArgumentError SubDataset(sds, Integer[true, true, true], :)
    else
        @test SubDataset(sds, Int[true, true, true], :) ==
              SubDataset(sds, [1, 1, 1], :)
    end
end

@testset "deletat! for views" begin
    ds1 = Dataset(a = Union{String, Missing}["a", "b", "a", "b", "a", "b"],
                    b = Vector{Union{Int, Missing}}(1:6),
                    c = Union{Int, Missing}[1:3;1:3])
    ds = vcat(ds1, ds1)
    sds = view(ds, 1:4, [1,3])
    deleteat!(sds, 1)
    @test sds == ds[2:4, [1,3]]
    sds = view(ds, 1:4, [1,3])
    deleteat!(sds, 1:2)
    @test sds == ds[3:4, [1,3]]
    sds = view(ds, 1:4, [1,3])
    deleteat!(sds, [1,3])
    @test sds == ds[[2,4], [1,3]]
    sds = view(ds, :, :)
    deleteat!(sds, Not(1:3))
    @test sds == ds[1:3, :]
    sds = view(ds, :, :)
    deleteat!(sds, [1,3,5,7,9,11])
    @test sds == ds[[2,4,6,8,10,12], :]


    ds = Dataset(a = repeat([1, 2, 3, 4], outer=[2]),
                             b = repeat([2, 1], outer=[4]),
                             c = 1:8)
    setformat!(ds, 1=>iseven)
    ds_c = copy(ds)
    sds = view(ds, 8:-1:1, [1,3])
    deleteat!(sds, 1:4)
    @test sds == ds[[4,3,2,1], [1,3]]
    @test ds_c == ds
end
