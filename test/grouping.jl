using Test, InMemoryDatasets, Random, PooledArrays, CategoricalArrays, DataAPI,
    Combinatorics, Unitful

import DataFrames as DF
const ≅ = isequal
const ≇ = !isequal

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
