using Test, InMemoryDatasets, Random, CategoricalArrays

@testset "nonunique, nonunique, unique! with extra argument" begin
    ds1 = Dataset(a = Union{String, Missing}["a", "b", "a", "b", "a", "b"],
                    b = Vector{Union{Int, Missing}}(1:6),
                    c = Union{Int, Missing}[1:3;1:3])
    ds = vcat(ds1, ds1)
    @test findall(nonunique(ds)) == collect(7:12)
    @test findall(nonunique(ds, first = false)) == collect(1:6)
    @test findall(nonunique(ds, :)) == collect(7:12)
    @test findall(nonunique(ds, Colon())) == collect(7:12)
    @test findall(nonunique(ds, :a)) == collect(3:12)
    @test findall(nonunique(ds, "a")) == collect(3:12)
    @test findall(nonunique(ds, [:a, :c])) == collect(7:12)
    @test findall(nonunique(ds, ["a", "c"])) == collect(7:12)
    @test findall(nonunique(ds, r"[ac]")) == collect(7:12)
    @test findall(nonunique(ds, Not(2))) == collect(7:12)
    @test findall(nonunique(ds, Not([2]))) == collect(7:12)
    @test findall(nonunique(ds, Not(:b))) == collect(7:12)
    @test findall(nonunique(ds, Not([:b]))) == collect(7:12)
    @test findall(nonunique(ds, Not([false, true, false]))) == collect(7:12)
    @test findall(nonunique(ds, [1, 3])) == collect(7:12)
    @test findall(nonunique(ds, 1)) == collect(3:12)
    fmt(x)=1
    setformat!(ds, :a=>fmt)
    @test findall(nonunique(ds, :a, mapformats = true)) == 2:12


    @test unique(ds) == ds1
    @test unique(ds, keep = :last) == ds1
    @test unique(ds, :) == ds1
    @test unique(ds, Colon()) == ds1
    @test unique(ds, 2:3) == ds1
    @test unique(ds, 3) == ds1[1:3, :]
    @test unique(ds, [1, 3]) == ds1
    @test unique(ds, [:a, :c]) == ds1
    @test unique(ds, ["a", "c"]) == ds1
    @test unique(ds, r"[ac]") == ds1
    @test unique(ds, Not(2)) == ds1
    @test unique(ds, Not([2])) == ds1
    @test unique(ds, Not(:b)) == ds1
    @test unique(ds, Not([:b])) == ds1
    @test unique(ds, Not([false, true, false])) == ds1
    @test unique(ds, :a) == ds1[1:2, :]
    @test unique(ds, "a") == ds1[1:2, :]
    @test unique(ds, :a, mapformats = true) == ds[1:1, :]

    @test_throws ArgumentError unique(Dataset())
    @test_throws ArgumentError nonunique(Dataset())

    @test unique(copy(ds1), "a") == unique(copy(ds1), :a) == unique(copy(ds1), 1) ==
          ds1[1:2, :]

    unique!(ds, [1, 3])
    @test ds == ds1
    for cols in (r"[ac]", Not(:b), Not(2), Not([:b]), Not([2]), Not([false, true, false]))
        ds = vcat(ds1, ds1)
        unique!(ds, cols)
        @test ds == ds1
    end

    ds = Dataset([Union{Missing, Int64}[3, 3, 1, 2, 2, 3, 3, 1, 1, 3], Union{Missing, Int64}[1, 1, 1, 1, 2, 1, 2, 2, 1, 3], Union{Missing, Int64}[1, 3, 1, 1, 2, 3, 2, 2, 1, 1]], :auto)
    ds1 = unique(ds)
    ds2 = unique(ds, keep = :last)
    ds3 = unique(ds, keep = :none)
    ds4 = unique(ds, 1, keep = :last)
    ds5 = unique(ds, 1, keep = :none)
    ds1_t = Dataset([Union{Missing, Int64}[3, 3, 1, 2, 2, 3, 1, 3], Union{Missing, Int64}[1, 1, 1, 1, 2, 2, 2, 3], Union{Missing, Int64}[1, 3, 1, 1, 2, 2, 2, 1]], :auto)
    ds2_t = Dataset([Union{Missing, Int64}[3, 2, 2, 3, 3, 1, 1, 3], Union{Missing, Int64}[1, 1, 2, 1, 2, 2, 1, 3], Union{Missing, Int64}[1, 1, 2, 3, 2, 2, 1, 1]], :auto)
    ds3_t = Dataset([Union{Missing, Int64}[3, 2, 2, 3, 1, 3], Union{Missing, Int64}[1, 1, 2, 2, 2, 3], Union{Missing, Int64}[1, 1, 2, 2, 2, 1]], :auto)
    ds4_t = Dataset([Union{Missing, Int64}[2, 1, 3], Union{Missing, Int64}[2, 1, 3], Union{Missing, Int64}[2, 1, 1]], :auto)
    ds5_t = Dataset([Union{Missing, Int64}[], Union{Missing, Int64}[], Union{Missing, Int64}[]], :auto)
    ds6 = unique!(copy(ds))
    ds7 = unique!(copy(ds), keep = :last)
    ds8 = unique!(copy(ds), keep = :none)
    ds9 = unique!(copy(ds), 1, keep = :last)
    ds10 = unique!(copy(ds), 1, keep = :none)
    @test ds1 == ds1_t
    @test ds2 == ds2_t
    @test ds3 == ds3_t
    @test ds4 == ds4_t
    @test ds5 == ds5_t
    @test ds6 == ds1_t
    @test ds7 == ds2_t
    @test ds8 == ds3_t
    @test ds9 == ds4_t
    @test ds10 == ds5_t
    mft(x) = x == 1 ? missing : x==2 ? 10 : 4
    setformat!(ds, 1:3=>mft)
    ds1 = unique(ds, 2:3, mapformats = true)
    ds1_t = ds[[1,2,5,10], :]
    ds2 = unique(ds, 1, mapformats = true)
    ds2_t = ds[[1,3,4], :]
    ds3 = unique(ds, 1, mapformats = true, keep = :last)
    ds3_t = ds[[5,9,10], :]
    ds4 = unique(ds,1,mapformats = true, keep = :none)
    ds4_t = ds[[], :]
    ds5 = unique(ds,1:2, mapformats = true, keep = :none)
    ds5_t = ds[[4,5,7,8,10], :]
    @test ds1 == ds1_t
    @test ds2 == ds2_t
    @test ds3 == ds3_t
    @test ds4 == ds4_t
    @test ds5 == ds5_t
    @test byrow(compare(ds1, ds1_t, mapformats =true), all)|>all
    @test byrow(compare(ds2, ds2_t, mapformats =true), all)|>all
    @test byrow(compare(ds3, ds3_t, mapformats =true), all)|>all
    @test byrow(compare(ds4, ds4_t, mapformats =true), all)|>all
    @test byrow(compare(ds5, ds5_t, mapformats =true), all)|>all
end

@testset "completecases and dropmissing" begin
    ds1 = Dataset([Vector{Union{Int, Missing}}(1:4), Vector{Union{Int, Missing}}(1:4)],
                    :auto)
    ds2 = Dataset([Union{Int, Missing}[1, 2, 3, 4], ["one", "two", missing, "four"]],
                    :auto)
    ds3 = Dataset(x = Int[1, 2, 3, 4], y = Union{Int, Missing}[1, missing, 2, 3],
                    z = Missing[missing, missing, missing, missing])

    @test completecases(ds2) == .!ismissing.(ds2.x2)
    @test completecases(ds3, :x) == trues(nrow(ds3))
    @test completecases(ds3, :y) == .!ismissing.(ds3.y)
    @test completecases(ds3, :z) == completecases(ds3, [:z, :x]) ==
          completecases(ds3, [:x, :z]) == completecases(ds3, [:y, :x, :z]) ==
          falses(nrow(ds3))
    @test completecases(ds3, [:y, :x]) ==
          completecases(ds3, [:x, :y]) == .!ismissing.(ds3.y)
    @test dropmissing(ds2) == ds2[[1, 2, 4], :]
    returned = dropmissing(ds1)
    @test ds1 == returned && ds1 !== returned
    ds2b = copy(ds2)
    @test dropmissing!(ds2b) === ds2b
    @test ds2b == ds2[[1, 2, 4], :]
    ds1b = copy(ds1)
    @test dropmissing!(ds1b) === ds1b
    @test ds1b == ds1

    @test completecases(Dataset()) == Bool[]
    @test_throws MethodError completecases(Dataset(x=1), true)
    @test_throws ArgumentError completecases(ds3, :a)

    for cols in (:x2, "x2", [:x2], ["x2"], [:x1, :x2], ["x1", "x2"], 2, [2], 1:2,
                 [true, true], [false, true], :,
                 r"x2", r"x", Not(1), Not([1]), Not(Int[]), Not([]), Not(Symbol[]),
                 Not(1:0), Not([true, false]), Not(:x1), Not([:x1]))
        @test ds2[completecases(ds2, cols), :] == ds2[[1, 2, 4], :]
        @test dropmissing(ds2, cols) == ds2[[1, 2, 4], :]
        returned = dropmissing(ds1, cols)
        @test ds1 == returned && ds1 !== returned
        ds2b = copy(ds2)
        @test dropmissing!(ds2b, cols) === ds2b
        @test ds2b == ds2[[1, 2, 4], :]
        @test dropmissing(ds2, cols) == ds2b
        @test ds2 != ds2b
        ds1b = copy(ds1)
        @test dropmissing!(ds1b, cols) === ds1b
        @test ds1b == ds1
    end

    ds = Dataset(a=[1, missing, 3])
    sds = view(ds, :, :)
    @test dropmissing(sds) == Dataset(a=[1, 3])
    @test eltype(dropmissing(ds).a) == Union{Int, Missing}
    @test eltype(dropmissing(sds).a) == Union{Int, Missing}
    @test ds == Dataset(a=[1, missing, 3]) # make sure we did not mutate ds

    @test_throws MethodError dropmissing!(sds)

    ds2 = copy(ds)
    @test dropmissing!(ds) === ds
    @test dropmissing!(ds2) === ds2
    @test eltype(ds.a) == Union{Int, Missing}
    @test eltype(ds2.a) == Union{Int, Missing}
    @test ds.a.val == ds2.a.val == [1, 3]

    a = [1, 2]
    ds = Dataset(a=a)
    @test dropmissing!(ds) === ds
    @test a == ds.a.val
    dsx = dropmissing(ds)
    @test ds == ds
    @test dsx !== ds
    @test dsx.a !== ds.a.val
    @test a == ds.a.val # we did not touch ds

    b = Union{Int, Missing}[1, 2]
    ds = Dataset(b=b)
    @test eltype(dropmissing(ds).b) == Union{Int, Missing}
    @test eltype(dropmissing!(ds).b) == Union{Int, Missing}

    ds = Dataset(x1 = [1,2,3,4,missing, 1], x2 = [4,4,4,missing, missing, missing])
    fmt(x) = isequal(x, 1) ? missing : x
    setformat!(ds, 1=>fmt)
    @test completecases(ds, 1) == [true, true, true, true, false, true]
    @test completecases(ds, 1, mapformats = true) == [false, true, true, true, false, false]
    for i in 1:100
        @test completecases(ds, 1, mapformats = true, threads = true) == [false, true, true, true, false, false]
    end
    @test completecases(ds, 1:2) == [true, true, true, false, false, false]
    @test completecases(ds, 1:2, mapformats = true) == [false, true, true, false, false, false]
    @test completecases(ds, [2,1]) == [true, true, true, false, false, false]
    @test completecases(ds, [2,1], mapformats = true) == [false, true, true, false, false, false]
    @test dropmissing(ds, :) == ds[[1,2,3], :]
    @test dropmissing(ds, :, mapformats = true) == ds[[2,3], :]
    @test dropmissing(ds, :, mapformats = true, view = true) == view(ds, 2:3, :)

end
