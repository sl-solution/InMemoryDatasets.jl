using Test, InMemoryDatasets, Random, PooledArrays, CategoricalArrays, DataAPI,
    Combinatorics, Unitful

import DataFrames as DF
const ≅ = isequal
const ≇ = !isequal

function _levels!(x::PooledArray, levels::AbstractVector)
    res = similar(x)
    copyto!(res, levels)
    copyto!(res, x)
end
_levels!(x::CategoricalArray, levels::AbstractVector) = levels!(x, levels)

function get_groups_for_groupby(gds)
    res = Vector{Int}(undef, nrow(parent(gds)))
    cnt = 1
    cnt2 = 1
    for i in 1:IMD._ngroups(gds)
        i == IMD._ngroups(gds) ? hi = nrow(parent(gds)) : hi = gds.starts[i+1]-1
        lo = gds.starts[i]
        for j in lo:hi
            res[cnt2] = cnt
            cnt2 += 1
        end
        cnt += 1
    end
    res[invperm(gds.perm)]
end

function isequal_unordered(gd,
                            gdsparts::AbstractVector)
    n = gd.lastvalid
    @assert n == length(gdsparts)
    remaining = Set(1:n)
    for i in 1:n
        for j in remaining
            if parent(gd)[IMD._get_perms(gd)[IMD.getindex_group(gd, i)], :] == gdsparts[j]
                pop!(remaining, j)
                break
            end
        end
    end
    isempty(remaining) || error("gd is not equal to provided groups")
end

@testset "parent" begin
    ds = Dataset(a = [1, 1, 2, 2], b = [5, 6, 7, 8])
    gds = gatherby(ds, :a)
    @test parent(gds) === ds
    df = DF.DataFrame(ds)
    gdf = DF.groupby(df, :a, sort = false)
    @test gds.groups == gdf.groups
    ds = Dataset(x1 = rand([missing, 2.2, 1.1], 1000), x2 = rand(1000))
    gds = gatherby(ds, :x1)
    @test parent(gds) === ds
    df = DF.DataFrame(ds)
    gdf = DF.groupby(df, :x1, sort = false)
    @test gds.groups == gdf.groups
    ds = Dataset(a = [1, 1, 2, 2], b = [5, 6, 7, 8])
    gds = groupby(ds, :a)
    @test parent(gds) === ds
    df = DF.DataFrame(ds)
    gdf = DF.groupby(df, :a, sort = true)
    @test get_groups_for_groupby(gds) == gdf.groups
    ds = Dataset(x1 = rand([missing, 2.2, 1.1], 1000), x2 = rand(1000))
    gds = groupby(ds, :x1)
    @test parent(gds) === ds
    df = DF.DataFrame(ds)
    gdf = DF.groupby(df, :x1, sort = true)
    @test get_groups_for_groupby(gds) == gdf.groups
    ds = Dataset(x1 = rand([missing; collect(1:1000000)], 1000), x2 = rand(1000))
    gds = groupby(ds, :x1)
    @test parent(gds) === ds
    df = DF.DataFrame(ds)
    gdf = DF.groupby(df, :x1, sort = true)
    @test get_groups_for_groupby(gds) == gdf.groups
    ds = Dataset(x1 = categorical(rand([missing; collect(1:1000000)], 1000)), x2 = rand(1000))
    gds = groupby(ds, :x1)
    @test parent(gds) === ds
    df = DF.DataFrame(ds)
    gdf = DF.groupby(df, :x1, sort = true)
    @test get_groups_for_groupby(gds) == gdf.groups
    ds = Dataset(x1 = PooledArray(rand([missing; collect(1:1000000)], 1000)), x2 = rand(1000))
    gds = groupby(ds, :x1)
    @test parent(gds) === ds
    df = DF.DataFrame(ds)
    gdf = DF.groupby(df, :x1, sort = true)
    @test get_groups_for_groupby(gds) == gdf.groups
    ds = Dataset(x1 = (rand([missing; collect(1:1000000)], 1000)) .* 1.1, x2 = rand(1000))
    gds = groupby(ds, :x1)
    @test parent(gds) === ds
    df = DF.DataFrame(ds)
    gdf = DF.groupby(df, :x1, sort = true)
    @test get_groups_for_groupby(gds) == gdf.groups
    ds = Dataset(x1 = (rand([missing; collect(1:1000000)], 1000)) .* 1.1, x2 = rand(1000))
    gds = gatherby(ds, :x1)
    @test parent(gds) === ds
    df = DF.DataFrame(ds)
    gdf = DF.groupby(df, :x1, sort = false)
    @test gds.groups == gdf.groups
    ds = Dataset(x1 = categorical(rand([missing; collect(1:1000000)], 1000)), x2 = rand(1000))
    gds = gatherby(ds, :x1)
    @test parent(gds) === ds
    df = DF.DataFrame(ds)
    gdf = DF.groupby(df, :x1, sort = false)
    @test gds.groups == gdf.groups
    ds = Dataset(rand(1:1000000, 1000, 10), :auto)
    gds = gatherby(ds, :)
    @test parent(gds) === ds
    df = DF.DataFrame(ds)
    gdf = DF.groupby(df, :, sort = false)
    @test gds.groups == gdf.groups
    ds = Dataset(rand(1:1000000, 1000, 10), :auto)
    gds = groupby(ds, :)
    @test parent(gds) === ds
    df = DF.DataFrame(ds)
    gdf = DF.groupby(df, :, sort = true)
    @test get_groups_for_groupby(gds) == gdf.groups
end

@testset "consistency" begin
    ds = Dataset(a = [1, 1, 2, 2], b = [5, 6, 7, 8], c = 1:4)
    push!(ds.c.val, 5)
    @test_throws AssertionError gatherby(ds, :a)

    ds = Dataset(a = [1, 1, 2, 2], b = [5, 6, 7, 8], c = 1:4)
    push!(IMD._columns(ds), ds[:, :a])
    @test_throws AssertionError gatherby(ds, :a)

    ds = Dataset(a = [1, 1, 2, 2], b = [5, 6, 7, 8], c = 1:4)
    push!(ds.c.val, 5)
    @test_throws AssertionError groupby(ds, :a)

    ds = Dataset(a = [1, 1, 2, 2], b = [5, 6, 7, 8], c = 1:4)
    push!(IMD._columns(ds), ds[:, :a])
    @test_throws AssertionError groupby(ds, :a)
end

@testset "general usage" begin
    Random.seed!(1)
    ds = Dataset(a = repeat(Union{Int, Missing}[1, 3, 2, 4], outer=[2]),
                   b = repeat(Union{Int, Missing}[2, 1], outer=[4]),
                   c = repeat([0, 1], outer=[4]),
                   x = Vector{Union{Float64, Missing}}(randn(8)))

   df = DF.DataFrame(ds)
   @test ds[IMD._get_perms(groupby(ds, 1:2)), :] == Dataset(df[DF.groupby(df, 1:2, sort = true).idx, :])
   @test ds[IMD._get_perms(gatherby(ds, 1:2)), :] == Dataset(df[DF.groupby(df, 1:2, sort = false).idx, :])
   @test ds[IMD._get_perms(groupby(ds, [2, 3, 1])), :] == Dataset(df[DF.groupby(df, [2, 3, 1], sort = true).idx, :])
   @test ds[IMD._get_perms(gatherby(ds, [2, 3, 1])), :] == Dataset(df[DF.groupby(df, [2, 3, 1], sort = false).idx, :])
   @test ds[IMD._get_perms(groupby(ds, 1:3)), :] == Dataset(df[DF.groupby(df, 1:3, sort = true).idx, :])
   @test ds[IMD._get_perms(gatherby(ds, 1:3)), :] == Dataset(df[DF.groupby(df, 1:3, sort = false).idx, :])
    # test number of potential combinations higher than typemax(Int32)
    N = 2000
    ds2 = Dataset(v1 = levels!(categorical(rand(1:N, 100)), collect(1:N)),
                    v2 = levels!(categorical(rand(1:N, 100)), collect(1:N)),
                    v3 = levels!(categorical(rand(1:N, 100)), collect(1:N)))
    df2 = DF.DataFrame(ds2)
    ds2b = mapcols(ds2, Vector{Int}, :)
    @test ds2[IMD._get_perms(groupby(ds2, 1:2)), :] == Dataset(df2[DF.groupby(df2, 1:2, sort = true).idx, :])
    @test ds2[IMD._get_perms(gatherby(ds2, 1:2)), :] == Dataset(df2[DF.groupby(df2, 1:2, sort = false).idx, :])
    @test ds2[IMD._get_perms(groupby(ds2, [2, 3, 1])), :] == Dataset(df2[DF.groupby(df2, [2, 3, 1], sort = true).idx, :])
    @test ds2[IMD._get_perms(gatherby(ds2, [2, 3, 1])), :] == Dataset(df2[DF.groupby(df2, [2, 3, 1], sort = false).idx, :])
    @test ds2[IMD._get_perms(groupby(ds2, 1:3)), :] == Dataset(df2[DF.groupby(df2, 1:3, sort = true).idx, :])
    @test ds2[IMD._get_perms(gatherby(ds2, 1:3)), :] == Dataset(df2[DF.groupby(df2, 1:3, sort = false).idx, :])
    @test ds2b[IMD._get_perms(groupby(ds2b, 1:2)), :] == Dataset(df2[DF.groupby(df2, 1:2, sort = true).idx, :])
    @test ds2b[IMD._get_perms(gatherby(ds2b, 1:2)), :] == Dataset(df2[DF.groupby(df2, 1:2, sort = false).idx, :])
    @test ds2b[IMD._get_perms(groupby(ds2b, [2, 3, 1])), :] == Dataset(df2[DF.groupby(df2, [2, 3, 1], sort = true).idx, :])
    @test ds2b[IMD._get_perms(gatherby(ds2b, [2, 3, 1])), :] == Dataset(df2[DF.groupby(df2, [2, 3, 1], sort = false).idx, :])
    @test ds2b[IMD._get_perms(groupby(ds2b, 1:3)), :] == Dataset(df2[DF.groupby(df2, 1:3, sort = true).idx, :])
    @test ds2b[IMD._get_perms(gatherby(ds2b, 1:3)), :] == Dataset(df2[DF.groupby(df2, 1:3, sort = false).idx, :])
    x = CategoricalArray(collect(1:20))
    ds = Dataset(v1=x, v2=x)
    df = DF.DataFrame(v1 = x, v2 = x)
    @test groupby!(ds, 1, rev = true) == Dataset(sort!(df, DF.order(:v1, rev = true)))
end

@testset "grouping for PAs - From DataFrames.jl" begin
    xv = ["A", missing, "B", "B", "A", "B", "A", "A"]
    yv = ["B", "A", "A", missing, "A", missing, "A", "A"]
    xvars = (xv,
             categorical(xv),
             levels!(categorical(xv), ["A", "B", "X"]),
             levels!(categorical(xv), ["X", "B", "A"]),
             _levels!(PooledArray(xv), ["A", "B", missing]),
             _levels!(PooledArray(xv), ["B", "A", missing, "X"]),
             _levels!(PooledArray(xv), [missing, "X", "A", "B"]))
    yvars = (yv,
             categorical(yv),
             levels!(categorical(yv), ["A", "B", "X"]),
             levels!(categorical(yv), ["B", "X", "A"]),
             _levels!(PooledArray(yv), ["A", "B", missing]),
             _levels!(PooledArray(yv), [missing, "A", "B", "X"]),
             _levels!(PooledArray(yv), ["B", "A", "X", missing]))
    for x in xvars, y in yvars
        ds = Dataset(Key1=x, Key2=y, Value=1:8)

        @testset "gatherby" begin
            gd = gatherby(ds, :Key1)
            @test gd.lastvalid == 3
            @test isequal_unordered(gd, [Dataset(Key1="A", Key2=["B", "A", "A", "A"], Value=[1, 5, 7, 8]),
                                        Dataset(Key1="B", Key2=["A", missing, missing], Value=[3, 4, 6]),
                                        Dataset(Key1=missing, Key2="A", Value=2)])

            gd = gatherby(ds, [:Key1, :Key2])
            @test gd.lastvalid == 5
            @test isequal_unordered(gd, [Dataset(Key1="A", Key2="A", Value=[5, 7, 8]),
                                Dataset(Key1="A", Key2="B", Value=1),
                                Dataset(Key1="B", Key2="A", Value=3),
                                Dataset(Key1="B", Key2=missing, Value=[4, 6]),
                                Dataset(Key1=missing, Key2="A", Value=2)])
        end

        @testset "gatherby with dropmissing" begin
            gd = gatherby(dropmissing(ds, :Key1, view=true), :Key1)
            @test gd.lastvalid == 2
            @test isequal_unordered(gd, [Dataset(Key1="A", Key2=["B", "A", "A", "A"], Value=[1, 5, 7, 8]),
            Dataset(Key1="B", Key2=["A", missing, missing], Value=[3, 4, 6])])

            gd = gatherby(dropmissing(ds, [:Key1, :Key2], view=true), [:Key1, :Key2])
            @test gd.lastvalid == 3
            @test isequal_unordered(gd, [Dataset(Key1="A", Key2="A", Value=[5, 7, 8]),
            Dataset(Key1="A", Key2="B", Value=1),
            Dataset(Key1="B", Key2="A", Value=3)])

        end

        @testset "groupby" begin
            gd = groupby(ds, :Key1)
            @test gd.lastvalid == 3
            @test isequal_unordered(gd, [
                Dataset(Key1="A", Key2=["B", "A", "A", "A"], Value=[1, 5, 7, 8]),
                Dataset(Key1="B", Key2=["A", missing, missing], Value=[3, 4, 6]),
                Dataset(Key1=missing, Key2="A", Value=2)
            ])

            gd = groupby(ds, [:Key1, :Key2])
            @test gd.lastvalid == 5
            @test isequal_unordered(gd, [
                Dataset(Key1="A", Key2="A", Value=[5, 7, 8]),
                Dataset(Key1="A", Key2="B", Value=1),
                Dataset(Key1="B", Key2="A", Value=3),
                Dataset(Key1="B", Key2=missing, Value=[4, 6]),
                Dataset(Key1=missing, Key2="A", Value=2)
            ])
        end

        @testset "groupby with dropmissing" begin
            gd = groupby(dropmissing(ds, :Key1, view=true), :Key1)
            @test gd.lastvalid == 2
            @test isequal_unordered(gd, [
                Dataset(Key1="A", Key2=["B", "A", "A", "A"], Value=[1, 5, 7, 8]),
                Dataset(Key1="B", Key2=["A", missing, missing], Value=[3, 4, 6])
            ])

            gd = groupby(dropmissing(ds, [:Key1, :Key2], view=true), [:Key1, :Key2])
            @test gd.lastvalid == 3
            @test isequal_unordered(gd, [
                Dataset(Key1="A", Key2="A", Value=[5, 7, 8]),
                Dataset(Key1="A", Key2="B", Value=1),
                Dataset(Key1="B", Key2="A", Value=3)
            ])
        end
    end

    @test gatherby(Dataset(x=[missing]), :x).groups ==
        gatherby(Dataset(x=Union{Int, Missing}[missing]), :x).groups ==
        gatherby(Dataset(x=Union{String, Missing}[missing]), :x).groups ==
        gatherby(Dataset(x=Any[missing]), :x).groups == [1]
    @test isempty(gatherby(dropmissing(Dataset(x=[missing])), :x).groups)

end

@testset "combine - set 1" begin
    Random.seed!(1)
    # 5 is there to ensure we test a single-row group
    ds = Dataset(a=[rand([1:4;missing], 19); 5],
                   x1=rand(1:100, 20),
                   x2=rand(1:3, 20) + im*rand(1:3, 20),
                   x3=repeat(1:2, 10) .* u"m")
    combine_out1 = combine(groupby(ds, 1), :x1=>sum=>:x1_sum, :x1=>(x->sum(x))=>:x1_sum2)
    combine_out2 = combine(gatherby(ds, 1), :x1=>sum=>:x1_sum, :x1=>(x->sum(x))=>:x1_sum2)
    combine_out3 = combine(gatherby(dropmissing(ds, 1, view=true), 1), :x1=>sum=>:x1_sum, :x1=>(x->sum(x))=>:x1_sum2)
    combine_out4 = combine(gatherby(ds, 1), :x1=>maximum, :x1=>minimum, 2:3=>byrow(-))
    combine_out5 = combine(gatherby(dropmissing(ds, 1, view=true), 1), :x1=>maximum, :x1=>minimum, 2:3=>byrow(-))
    combine_out6 = combine(gatherby(ds, 1), :x1=>IMD.n)


    @test combine_out1[!, 2] == combine_out1[!, 3]
    @test combine_out2[!, 2] == combine_out2[!, 3]
    @test combine_out3[!, 2] == combine_out3[!, 3]
    @test all(x->x>=0, combine_out4[!, "row_-"])
    @test all(x->x>=0, combine_out5[!, "row_-"])
    @test dropmissing(combine_out4, 1) == combine_out5
    @test combine_out6[nrow(combine_out6), 1] == 5
    combine_out7 = combine(groupby(ds, :x3), :x1=>mean)
    combine_out8 = combine(gatherby(ds, :x3), :x2=>IMD.n)
    combine_out9 = combine(groupby(ds, :x3), :x3=>maximum, :x3=>minimum)
    combine_out7_v = combine(groupby(view(ds, nrow(ds):-1:1, ncol(ds):-1:1), :x3), :x1=>mean)
    combine_out8_v = combine(gatherby(view(ds, nrow(ds):-1:1, ncol(ds):-1:1), :x3), :x2=>IMD.n)
    combine_out8_v = combine_out8_v[nrow(combine_out8_v):-1:1, :]
    combine_out9_v = combine(groupby(view(ds, nrow(ds):-1:1, ncol(ds):-1:1), :x3), :x3=>maximum, :x3=>minimum)
    @test combine_out7[!, 1] == [1 * u"m", 2 * u"m"]
    @test combine_out8[!, 2] == [10, 10]
    @test combine_out9[!, 2] == combine_out9[!, 1]
    @test combine_out9[!, 3] == combine_out9[!, 1]

    @test all(byrow(compare(combine_out7, combine_out7_v, on = names(combine_out7)), all, :))
    @test all(byrow(compare(combine_out8, combine_out8_v, on = names(combine_out8)), all, :))
    @test all(byrow(compare(combine_out9, combine_out9_v, on = names(combine_out9)), all, :))
end

@testset "combine - set 2" begin
    ds = Dataset(a=[rand([1:4;missing], 19); 5],
                   x1=rand(1:100, 20),
                   x2=rand(1:3, 20) + im*rand(1:3, 20),
                   x3=repeat(1:2, 10) .* u"m")
    c1_IMD = combine(gatherby(ds, [:a, :x3]), :x1=>sum=>:o1, :x1=>argmax=>:o2, :x1=>sort=>:o3)
    c1_DF = DF.combine(DF.groupby(DF.DataFrame(ds), [:a, :x3], sort = false), :x1=>sum=>:o1, :x1=>argmax=>:o2, :x1=>sort=>:o3)
    @test all(byrow(compare(c1_IMD, Dataset(c1_DF), on = names(c1_IMD)), all, :))
    c1_IMD = combine(groupby(ds, [:a, :x3]), :x1=>sum=>:o1, :x1=>argmax=>:o2, :x1=>sort=>:o3)
    c1_DF = DF.combine(DF.groupby(DF.DataFrame(ds), [:a, :x3], sort = true), :x1=>sum=>:o1, :x1=>argmax=>:o2, :x1=>sort=>:o3)
    @test all(byrow(compare(c1_IMD, Dataset(c1_DF), on = names(c1_IMD)), all, :))

    for r in 0:.1:1
        ds = Dataset(rand(1:100, 100, 10), :auto)
        modify!(ds, [1,5,3] => x->Float32.(x))
        map!(ds, x->rand()<r ? missing : x, :)
        c1 = combine(groupby(ds, [:x1,:x3]), :x10=>sort, :x4=>sum)
        c2 = combine(groupby(view(ds, nrow(ds):-1:1, ncol(ds):-1:1), [:x1, :x3]), :x10=>sort, :x4=>sum)
        @test c1 == c2
        c1 = combine(gatherby(ds, [:x1,:x3]), :x10=>sort, :x4=>sum)
        c2 = combine(gatherby(view(ds, nrow(ds):-1:1, [10, 1, 3, 4]), [:x1, :x3]), :x10=>sort, :x4=>sum)
        @test sort(c1, :) == sort(c2, :)
    end
    ds = Dataset(rand(1000, 2), :auto)
    insertcols!(ds, 1, :g=>rand(1:10, nrow(ds)))
    c1_IMD = combine(groupby(ds, 1), (2, 3)=>cor=>:cor, 3=>sum=>:sum, 2 => mean => :mean)
    df = DF.DataFrame(ds)
    c1_DF = DF.combine(DF.groupby(df, 1, sort = true), 2:3=>cor=>:cor, 3=>sum=>:sum, 2=>mean=>:mean)
    @test all(byrow(compare(c1_IMD, Dataset(c1_DF), eq = isapprox), all, :))
    c1_IMD = combine(groupby(view(ds, nrow(ds):-1:1, ncol(ds):-1:1), 3), (2,1)=>cor=>:cor, 1=>sum=>:sum, 2 => mean => :mean)
    df = DF.DataFrame(ds)
    c1_DF = DF.combine(DF.groupby(df, 1, sort = true), 2:3=>cor=>:cor, 3=>sum=>:sum, 2=>mean=>:mean)
    @test all(byrow(compare(c1_IMD, Dataset(c1_DF), eq = isapprox), all, :))
end
@testset "combine - set 3"
    ds = Dataset(x1 = [1,2,1,2,2,1], x2=[1.0,missing,1.1,1.1,1.1,1.1],y=100:100:600.0)
    c1 = combine(gatherby(ds, 1), :x2=>sum)
    c2 = combine(groupby(ds, 1, stable = false), :y=>mean)
    c3 = combine(gatherby(ds,[:x2], isgathered=true), :y=>maximum)
    c4 = combine(groupby(ds, r"x", stable = false), (:y, :x2)=>(x,y)->y[argmax(x)])

    @test c1 == Dataset(x1=[1,2], sum_x2=[3.2,2.2])
    @test c2 == Dataset(x1=[1,2], mean_y=[1000/3, 1100/3])
    @test c3 == Dataset(x2=[1,missing,1.1], maximum_y=[100.0, 200, 600])
    @test c4 == Dataset(x1=[1,1,2,2], x2=[1.0,1.1,1.1,missing], function_y_x2=[1,1.1,1.1,missing])
    combinefmt(x)=ismissing(x) ? missing : 0
    setformat!(ds, :x2=>combinefmt)
    c1 = combine(groupby(ds, :x2), :y=>maximum)
    c2 = combine(groupby(ds, :x2, mapformats = false), :y=>maximum)
    c3 = combine(gatherby(ds, :x2), (:x1,:y)=>(x,y)->(maximum(x), minimum(y)))
    @test all(byrow(compare(c1, Dataset(x2=[0, missing], maximum_y=[600.0, 200]), mapformats=true), all))
    @test all(byrow(compare(c2, Dataset(x2=[0, 0, missing], maximum_y=[100, 600.0, 200]), mapformats=true), all))
    @test all(byrow(compare(c3, Dataset(x2=[0,missing], function_x1_y=[(2, 100.0), (2, 200.0)]), mapformats=true), all))
end
