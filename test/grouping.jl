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

# TODO windows on CI stuck here ??
if !Base.Sys.iswindows()
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
end

@testset "combine - set 3" begin
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
    ds = Dataset(x1 = [1,2,1,2,2,1], x2=[1.0,missing,1.1,1.1,1.1,1.1],y=100:100:600.0)
    @test byrow(compare(combine(gatherby(ds, 1), :x2=>[sum, maximum],2:3=>byrow(+)=>:row), Dataset(x1=[1,2], sum_x2=[3.2,2.2], maximum_x2=[1.1,1.1], row = [4.3, 3.3] ), eq = isapprox), all)|>all
    @test byrow(compare(combine(gatherby(ds, 1), :x2=>[x->sum(x), maximum],2:3=>byrow(+)=>:row), Dataset(x1=[1,2], function_x2=[3.2,2.2], maximum_x2=[1.1,1.1], row = [4.3, 3.3] ), eq = isapprox), all)|>all
    @test byrow(compare(combine(groupby(ds, 1), :x2=>[sum, maximum],2:3=>byrow(+)=>:row), Dataset(x1=[1,2], sum_x2=[3.2,2.2], maximum_x2=[1.1,1.1], row = [4.3, 3.3] ), eq = isapprox), all)|>all

    @test byrow(compare(combine(gatherby(view(ds, :, [2,1]), :x1), :x2=>[sum, maximum],2:3=>byrow(+)=>:row), Dataset(x1=[1,2], sum_x2=[3.2,2.2], maximum_x2=[1.1,1.1], row = [4.3, 3.3] ), eq = isapprox), all)|>all
    @test byrow(compare(combine(gatherby(view(ds, :, [2,1]), :x1), :x2=>[x->sum(x), maximum],2:3=>byrow(+)=>:row), Dataset(x1=[1,2], function_x2=[3.2,2.2], maximum_x2=[1.1,1.1], row = [4.3, 3.3] ), eq = isapprox), all)|>all
    @test byrow(compare(combine(groupby(view(ds, :, [2,1]), :x1), :x2=>[sum, maximum],2:3=>byrow(+)=>:row), Dataset(x1=[1,2], sum_x2=[3.2,2.2], maximum_x2=[1.1,1.1], row = [4.3, 3.3] ), eq = isapprox), all)|>all
end

@testset "combine - set4 - checking normalize_combine" begin
    ds = Dataset(g1 = [1,2,1,2,2,1], g2 = [2,3,1,1,2,3], x2=[1.0,missing,1.1,1.1,1.1,1.1],y=100:100:600.0)
    @test combine(groupby(ds, 2), [1,3] => [maximum, minimum])== Dataset(g2 = [1,2,3], maximum_g1 = [2,2,2], minimum_g1 = [1,1,1], maximum_x2 = [1.1,1.1,1.1], minimum_x2 = [1.1,1,1.1])
    @test combine(gatherby(ds, 2), [1,3] => [maximum, minimum])== Dataset(g2 = [2,3,1], maximum_g1 = [2,2,2], minimum_g1 = [1,1,1], maximum_x2 = [1.1,1.1,1.1], minimum_x2 = [1.,1.1,1.1])
    @test combine(groupby(ds, [2,1]), r"x"=>sum) == Dataset(g2 = [1,1,2,2,3,3], g1=[1,2,1,2,1,2], sum_x2 = [1.1,1.1,1,1.1,1.1, missing])
    @test combine(gatherby(ds, [2,1]), r"x"=>sum) == Dataset(g2 = [2,3,1,1,2,3], g1=[1,2,1,2,2,1], sum_x2 = [1, missing, 1.1,1.1,1.1,1.1])
    groupby!(ds, [2,1])
    @test combine(ds, r"x"=>sum) == Dataset(g2 = [1,1,2,2,3,3], g1=[1,2,1,2,1,2], sum_x2 = [1.1,1.1,1,1.1,1.1, missing])
    @test combine(groupby(ds, 2), [1,3] => [maximum, minimum])== Dataset(g2 = [1,2,3], maximum_g1 = [2,2,2], minimum_g1 = [1,1,1], maximum_x2 = [1.1,1.1,1.1], minimum_x2 = [1.1,1,1.1])
    @test combine(groupby(ds, [2,1]), r"x"=>sum) == Dataset(g2 = [1,1,2,2,3,3], g1=[1,2,1,2,1,2], sum_x2 = [1.1,1.1,1,1.1,1.1, missing])

    @test combine(groupby(ds, 1), 3:4 => sum,  2:3=> byrow.([maximum, minimum])) == Dataset(g1 = [1,2], sum_x2 = [3.2, 2.2], sum_y = [1000, 1100], row_maximum = [1000,1100], row_minimum=[3.2,2.2])
    @test combine(groupby(ds, 1), 3:4 => sum,  2:3=> byrow.([maximum, minimum]), 4:5=>byrow(sum)) == Dataset(g1 = [1,2], sum_x2 = [3.2, 2.2], sum_y = [1000, 1100], row_maximum = [1000,1100], row_minimum=[3.2,2.2], row_sum = [1003.2, 1102.2])

    @test combine(groupby(view(ds, :, [4,3,2,1]), 4), [2,1] => sum,  2:3=> byrow.([maximum, minimum])) == Dataset(g1 = [1,2], sum_x2 = [3.2, 2.2], sum_y = [1000, 1100], row_maximum = [1000,1100], row_minimum=[3.2,2.2])
    @test combine(groupby(view(ds,:, [4,3,2,1]), 4), [:x2, :y] => sum,  2:3=> byrow.([maximum, minimum]), 4:5=>byrow(sum)) == Dataset(g1 = [1,2], sum_x2 = [3.2, 2.2], sum_y = [1000, 1100], row_maximum = [1000,1100], row_minimum=[3.2,2.2], row_sum = [1003.2, 1102.2])

end

@testset "modifying and combining views" begin
    ds = Dataset(x = [3,1,2,2,missing,3,3], y = [1.1, missing, -1.0, -3.0, missing, 4.0, 5.0], z = [11,15,7,-11,12,0,0])
    sds1 = dropmissing(ds, 2, view = true)
    sds2 = view(ds, [2,1,1,3,5,6,7,4,4], [:z, :x])

    @test combine(groupby(sds1, :x), :z => sum) == Dataset(x = [2, 3], sum_z = [-4, 11])
    @test combine(gatherby(sds1, :x), :z => sum) == Dataset(x = [3, 2], sum_z = [11, -4])
    @test combine(groupby(sds2, :x), :z => sum) == Dataset(x = [1,2, 3, missing], sum_z = [15, -15, 22, 12])
    @test combine(gatherby(sds2, :x), :z => sum) == Dataset(x = [1, 3, 2, missing], sum_z = [15, 22, -15, 12])

    @test combine(groupby(sds1, :x), :z => (x->sum(x))=>:sum_z) == Dataset(x = [2, 3], sum_z = [-4, 11])
    @test combine(gatherby(sds1, :x), :z => (x->sum(x))=>:sum_z) == Dataset(x = [3, 2], sum_z = [11, -4])
    @test combine(groupby(sds2, :x), :z => (x->sum(x))=>:sum_z) == Dataset(x = [1,2, 3, missing], sum_z = [15, -15, 22, 12])
    @test combine(gatherby(sds2, :x), :z => (x->sum(x))=>:sum_z) == Dataset(x = [1, 3, 2, missing], sum_z = [15, 22, -15, 12])

    @test combine(groupby(sds1, :x), :y=>(sort!)=>:s_y) == Dataset(x=[2,2,3,3,3], s_y=[-3.0,-1.0,1.1,4.0,5.0])
    @test combine(gatherby(sds1, :x), :y=>(sort!)=>:s_y) == Dataset(x=[3,3,3,2,2], s_y=[1.1,4.0,5.0,-3.0,-1.0])

    @test combine(groupby(sds2, :x), :z=>(sort!)=>:s_z) == Dataset(x=[1,2,2,2,3,3,3,3,missing], s_z=[15,-11,-11,7,0,0,11,11,12])
    @test combine(gatherby(sds2, :x), :z=>(sort!)=>:s_z) == Dataset(x=[1,3,3,3,3,2,2,2,missing], s_z=[15,0,0,11,11,-11,-11,7,12])

    @test combine(groupby(sds1, :x), (:y,:z)=>cor) == Dataset(x=[2,3],cor_y_z=[1.0, -0.9690582663799521])
    @test combine(gatherby(sds1, :x), (:y,:z)=>cor) == Dataset(x=[3,2],cor_y_z=[-0.9690582663799521, 1.0])

    @test combine(groupby(sds2, :x), (1,2)=>((x,y)->maximum(x)/length(y))=>:q) == Dataset(x=[1,2,3,missing], q=[15.0, 7/3,11/4,12.0])
    @test combine(gatherby(sds2, :x), (1,2)=>((x,y)->maximum(x)/length(y))=>:q) == Dataset(x=[1,3,2,missing], q=[15.0, 11/4,7/3,12.0])

    @test combine(groupby(sds1, :x), :y=>maximum, :z=>maximum, 2:3=>byrow(-)=>:q) == Dataset(x=[2,3], maximum_y=[-1.0,5],maximum_z=[7, 11], q=[-8,-6.0])
    @test combine(gatherby(sds1, :x), :y=>maximum, :z=>maximum, 2:3=>byrow(-)=>:q) == Dataset(x=[3,2], maximum_y=reverse([-1.0,5]),maximum_z=reverse([7, 11]), q=reverse([-8,-6.0]))

    @test combine(groupby(sds2, :x), :z=>(sort!)=>:s_z, :x=>maximum, :z=>minimum, 3:4=>byrow(-)=>:q) == Dataset([Union{Missing, Int64}[1, 2, 2, 2, 3, 3, 3, 3, missing], Union{Missing, Int64}[15, -11, -11, 7, 0, 0, 11, 11, 12], Union{Missing, Int64}[1, 2, 2, 2, 3, 3, 3, 3, missing], Union{Missing, Int64}[15, -11, -11, -11, 0, 0, 0, 0, 12], Union{Missing, Int64}[-14, 13, 13, 13, 3, 3, 3, 3, missing]], [:x, :s_z, :maximum_x, :minimum_z, :q])

    @test combine(gatherby(sds2, :x), :z=>(sort!)=>:s_z, :x=>maximum, :z=>minimum, 3:4=>byrow(-)=>:q) == Dataset([Union{Missing, Int64}[1, 3, 3, 3, 3, 2, 2, 2, missing], Union{Missing, Int64}[15, 0, 0, 11, 11, -11, -11, 7, 12], Union{Missing, Int64}[1, 3, 3, 3, 3, 2, 2, 2, missing], Union{Missing, Int64}[15, 0, 0, 0, 0, -11, -11, -11, 12], Union{Missing, Int64}[-14, 3, 3, 3, 3, 13, 13, 13, missing]], [:x, :s_z, :maximum_x, :minimum_z, :q])

    @test ds == Dataset(x = [3,1,2,2,missing,3,3], y = [1.1, missing, -1.0, -3.0, missing, 4.0, 5.0], z = [11,15,7,-11,12,0,0])

    @test_throws ArgumentError combine(gatherby(sds2, :x), :q=>sum)
    @test_throws ArgumentError combine(gatherby(sds2, :x), :y=>sum)

    modify!(groupby(sds1, :x), :y=>maximum=>:q)
    @test ds == Dataset(x = [3,1,2,2,missing,3,3], y = [1.1, missing, -1.0, -3.0, missing, 4.0, 5.0], z = [11,15,7,-11,12,0,0], q=[5.0, missing, -1,-1,missing,5,5])
    modify!(gatherby(sds1, :x), :y=>maximum=>:q2)
    @test ds == Dataset(x = [3,1,2,2,missing,3,3], y = [1.1, missing, -1.0, -3.0, missing, 4.0, 5.0], z = [11,15,7,-11,12,0,0], q=[5.0, missing, -1,-1,missing,5,5], q2=[5.0, missing, -1,-1,missing,5,5])

    # original data set
    ds = Dataset(x = [3,1,2,2,missing,3,3], y = [1.1, missing, -1.0, -3.0, missing, 4.0, 5.0], z = [11,15,7,-11,12,0,0])
    sds1 = dropmissing(ds, 2, view = true)
    sds2 = view(ds, [2,1,1,3,5,6,7,4,4], [:z, :x])

    modify!(groupby(sds2, :x), :z=>maximum=>:q)
    @test ds == Dataset([Union{Missing, Int64}[3, 1, 2, 2, missing, 3, 3], Union{Missing, Float64}[1.1, missing, -1.0, -3.0, missing, 4.0, 5.0], Union{Missing, Int64}[11, 15, 7, -11, 12, 0, 0], Union{Missing, Int64}[11, 15, 7, 7, 12, 11, 11]], ["x", "y", "z", "q"])
    modify!(gatherby(sds2, :x), :z=>maximum=>:q2)
    @test ds == Dataset([Union{Missing, Int64}[3, 1, 2, 2, missing, 3, 3], Union{Missing, Float64}[1.1, missing, -1.0, -3.0, missing, 4.0, 5.0], Union{Missing, Int64}[11, 15, 7, -11, 12, 0, 0], Union{Missing, Int64}[11, 15, 7, 7, 12, 11, 11], [11, 15, 7, 7, 12, 11, 11]], ["x", "y", "z", "q","q2"])

    # original data set
    ds = Dataset(x = [3,1,2,2,missing,3,3], y = [1.1, missing, -1.0, -3.0, missing, 4.0, 5.0], z = [11,15,7,-11,12,0,0])
    sds1 = dropmissing(ds, 2, view = true)
    sds2 = view(ds, [2,1,1,3,5,6,7,4,4], [:z, :x])

    modify!(groupby(sds1, :x), (2,3)=>cor)
    @test ds == Dataset([Union{Missing, Int64}[3, 1, 2, 2, missing, 3, 3], Union{Missing, Float64}[1.1, missing, -1.0, -3.0, missing, 4.0, 5.0], Union{Missing, Int64}[11, 15, 7, -11, 12, 0, 0], Union{Missing, Float64}[-0.9690582663799521, missing, 1.0, 1.0, missing, -0.9690582663799521, -0.9690582663799521]], ["x", "y", "z", "cor_y_z"])
    modify!(gatherby(sds1, :x), (2,3)=>cor)
    @test ds == Dataset([Union{Missing, Int64}[3, 1, 2, 2, missing, 3, 3], Union{Missing, Float64}[1.1, missing, -1.0, -3.0, missing, 4.0, 5.0], Union{Missing, Int64}[11, 15, 7, -11, 12, 0, 0], Union{Missing, Float64}[-0.9690582663799521, missing, 1.0, 1.0, missing, -0.9690582663799521, -0.9690582663799521]], ["x", "y", "z", "cor_y_z"])
    # sds2 is not well defined anymore, since the original data has been changed
    sds2 = view(ds, [2,1,1,3,5,6,7,4,4], [:z, :x])

    modify!(groupby(sds2, :x), (:x, :z)=>((x,y)->length(x)/maximum(y))=>:q, [:z, :q]=>byrow(maximum)=>:q2)
    @test ds == Dataset([Union{Missing, Int64}[3, 1, 2, 2, missing, 3, 3], Union{Missing, Float64}[1.1, missing, -1.0, -3.0, missing, 4.0, 5.0], Union{Missing, Int64}[11, 15, 7, -11, 12, 0, 0], Union{Missing, Float64}[-0.9690582663799521, missing, 1.0, 1.0, missing, -0.9690582663799521, -0.9690582663799521], Union{Missing, Float64}[0.36363636363636365, 0.06666666666666667, 0.42857142857142855, 0.42857142857142855, 0.08333333333333333, 0.36363636363636365, 0.36363636363636365], Union{Missing, Float64}[11.0, 15.0, 7.0, 0.42857142857142855, 12.0, 0.36363636363636365, 0.36363636363636365]], ["x", "y", "z", "cor_y_z", "q", "q2"])

    @test IMD.index(view(ds,[2,1,1,3,5,6,7,4,4],[:z,:x,:q,:q2])) == IMD.index(sds2)


    ds = Dataset(x = [3,1,2,2,missing,3,3], y = [1.1, missing, -1.0, -3.0, missing, 4.0, 5.0], z = [11,15,7,-11,12,0,0])
    groupby!(ds, :y)
    sds1 = dropmissing(ds, 2, view = true)
    sds2 = view(ds, [2,1,1,3,5,6,7,4,4], [:z, :x])

    modify!(groupby(sds1, :x), (2,3)=>cor)
    @test ds == Dataset([Union{Missing, Int64}[2, 2, 3, 3, 3, 1, missing], Union{Missing, Float64}[-3.0, -1.0, 1.1, 4.0, 5.0, missing, missing], Union{Missing, Int64}[-11, 7, 11, 0, 0, 15, 12], Union{Missing, Float64}[1.0, 1.0, -0.9690582663799521, -0.9690582663799521, -0.9690582663799521, missing, missing]], ["x", "y", "z", "cor_y_z"])
    modify!(gatherby(sds1, :x), (2,3)=>cor)
    @test ds == Dataset([Union{Missing, Int64}[2, 2, 3, 3, 3, 1, missing], Union{Missing, Float64}[-3.0, -1.0, 1.1, 4.0, 5.0, missing, missing], Union{Missing, Int64}[-11, 7, 11, 0, 0, 15, 12], Union{Missing, Float64}[1.0, 1.0, -0.9690582663799521, -0.9690582663799521, -0.9690582663799521, missing, missing]], ["x", "y", "z", "cor_y_z"])
    # sds2 is not well defined anymore, since the original data has been changed
    sds2 = view(ds, [2,1,1,3,5,6,7,4,4], [:z, :x])

    modify!(groupby(sds2, :x), (:x, :z)=>((x,y)->length(x)/maximum(y))=>:q, [:z, :q]=>byrow(maximum)=>:q2)
    @test ds == Dataset([Union{Missing, Int64}[2, 2, 3, 3, 3, 1, missing], Union{Missing, Float64}[-3.0, -1.0, 1.1, 4.0, 5.0, missing, missing], Union{Missing, Int64}[-11, 7, 11, 0, 0, 15, 12], Union{Missing, Float64}[1.0, 1.0, -0.9690582663799521, -0.9690582663799521, -0.9690582663799521, missing, missing], Union{Missing, Float64}[0.42857142857142855, 0.42857142857142855, 0.36363636363636365, 0.36363636363636365, 0.36363636363636365, 0.06666666666666667, 0.08333333333333333], Union{Missing, Float64}[0.42857142857142855, 7.0, 11.0, 0.36363636363636365, 0.36363636363636365, 15.0, 12.0]], ["x", "y", "z", "cor_y_z", "q", "q2"])

    @test IMD.index(view(ds,[2,1,1,3,5,6,7,4,4],[:z,:x,:q,:q2])) == IMD.index(sds2)

    ds = Dataset(x = [3,1,2,2,missing,3,3], y = [1.1, missing, -1.0, -3.0, missing, 4.0, 5.0], z = [11,15,7,-11,12,0,0])
    sds2 = view(ds, [2,1,1,3,5,6,7,4,4], 1:2)
    @test combine(gatherby(sds2, 1), :y=>sum) == Dataset(x=[1,3,2, missing],  sum_y=[missing, 11.2,-7.0,missing])
    @test combine(gatherby(sds2, 1), :y=>(x->sum(x))=>:sum_y) == Dataset(x=[1,3,2, missing],  sum_y=[missing, 11.2,-7.0,missing])
    @test combine(groupby(sds2, 1), :y=>sum) == Dataset(x=[1,2,3, missing],  sum_y=[missing, -7.0, 11.2,missing])
    @test combine(gatherby(sds2, 1, isgathered = true), :y=>sum) == Dataset(x = [1,3,2,missing,3,2], sum_y=[missing, 2.2, -1,missing, 9,-6])

    ds = Dataset(x = [1,2,1,2,3], y1 = Union{Int8, Missing}[1,2,missing,4,missing], y2 = Union{Int32, Missing}[1,2,3,4,missing], y3=Union{Int16, Missing}[100,20,3000,4,missing], y4=Float16.(rand(5)), y5=rand(BigFloat, 5), y6=[missing, missing, missing, missing, missing])
    sds = view(ds, [1,2,3,4,5], [1,2,3,4,5,6,7])

    @test combine(gatherby(sds, 1), 2:4 .=>Ref([sum, mean, maximum, minimum, IMD.n, IMD.nmissing])) == Dataset([Union{Missing, Int64}[1, 2, 3], Union{Missing, Int64}[1, 6, missing], Union{Missing, Float64}[1.0, 3.0, missing], Union{Missing, Int8}[1, 4, missing], Union{Missing, Int8}[1, 2, missing], Union{Missing, Int64}[1, 2, 0], Union{Missing, Int64}[1, 0, 1], Union{Missing, Int64}[4, 6, missing], Union{Missing, Float64}[2.0, 3.0, missing], Union{Missing, Int32}[3, 4, missing], Union{Missing, Int32}[1, 2, missing], Union{Missing, Int64}[2, 2, 0], Union{Missing, Int64}[0, 0, 1], Union{Missing, Int64}[3100, 24, missing], Union{Missing, Float64}[1550.0, 12.0, missing], Union{Missing, Int16}[3000, 20, missing], Union{Missing, Int16}[100, 4, missing], Union{Missing, Int64}[2, 2, 0], Union{Missing, Int64}[0, 0, 1]], ["x", "sum_y1", "mean_y1", "maximum_y1", "minimum_y1", "n_y1", "nmissing_y1", "sum_y2", "mean_y2", "maximum_y2", "minimum_y2", "n_y2", "nmissing_y2", "sum_y3", "mean_y3", "maximum_y3", "minimum_y3", "n_y3", "nmissing_y3"])

    @test combine(gatherby(sds,1), :y6=>sum=>:mm) == combine(gatherby(sds,1), :y6=>minimum=>:mm) == combine(gatherby(sds,1), :y6=>var=>:mm) == combine(gatherby(sds,1), :y6=>(x->sum(x))=>:mm) == combine(gatherby(sds,1), :y6=>(x->var(x))=>:mm) == combine(gatherby(sds,1), :y6=>(x->minimum(x))=>:mm) == Dataset(x = [1,2,3] , mm = [missing, missing, missing])

    var1(x) = var(x)
    std1(x) = std(x)
    median1(x) = median(x)
    c1 =combine(gatherby(sds, 1), 2:4 .=>Ref([var, std, median]))
    c2 =  combine(gatherby(copy(sds), 1), 2:4 .=> Ref([var1, std1, median1]))
    @test byrow(compare(c1, c2, on = names(c1) .=> names(c2)) , all)|>all

    c3 = combine(gatherby(sds,1), :y4=>sum)
    @test eltype(c3.sum_y4) == Union{Missing, Float16}
    c3 = combine(gatherby(sds,1), :y5=>sum)
    @test eltype(c3.sum_y5) == Union{Missing, BigFloat}

    ds = Dataset(rand(1:10, 300_000,3), :auto)
    insertcols!(ds, 1, :g => repeat(1:150_000, inner = 2))
    map!(ds, x->rand()<.7 ? missing : x, r"x")
    sds = view(ds, :, :)

    @test combine(gatherby(sds, 1), r"x"=>sum) == combine(groupby(sds, 1), r"x"=>sum)
    @test combine(gatherby(sds, 1), r"x"=>maximum) == combine(groupby(sds, 1), r"x"=>maximum)
    @test combine(gatherby(sds, 1), r"x"=>minimum) == combine(groupby(sds, 1), r"x"=>minimum)
    @test combine(gatherby(sds, 1), r"x"=>var) == combine(groupby(sds, 1), r"x"=>var)

end
