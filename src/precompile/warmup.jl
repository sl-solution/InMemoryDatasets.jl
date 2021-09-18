function warmup()
    t1 = now()
    ds = Dataset(x1 = rand(1:2, 10000), x2 = rand(1:2, 10000), x3 = rand(1:2, 10000) * 1.1, x4 = rand([Date(1), Date(2)], 10000), x5 = string.(rand(1:2), 10000),
                 x6 = PooledArray(Characters{5, UInt8}.(rand(1:2, 10000))), x7 = Int32.(rand(1:2, 10000)), x8 = Float32.(rand(1:2, 10000) * 1.1), x9 = PooledArray(Characters{3, UInt8}.(rand(1:2, 10000))), x10 = PooledArray(Characters{12, UInt8}.(rand(1:2, 10000))))
    for i in 1:ncol(ds)
        sortperm(ds, i)
    end
    for i in 1:ncol(ds)
        sortperm(ds, i, alg = QuickSort)
    end
    for i in 1:ncol(ds)
        sortperm(ds, 1:i)
    end
    for i in 1:ncol(ds)-1
        sortperm(ds, [ncol(ds), i])
    end
    for i in 1:ncol(ds)
        groupby(ds, i)
    end
    for i in 1:ncol(ds)
        groupby(ds, 1:i)
    end
    byrow(ds, all, :, by = isequal(1))
    byrow(ds, sum)
    byrow(ds, +, 1:2)
    byrow(ds, -, 1:2)
    byrow(ds, *, 1:2)
    byrow(ds, /, 1:2)
    combine(groupby(ds,1), Ref([1,2,3,7,8]) .=> [sum, mean, length, maximum, minimum, var, std, median, sort])
    ds2 = ds[1:2, [1,3]]
    innerjoin(ds, ds2, on = [:x1, :x3])
    leftjoin(ds, ds2, on = [:x1, :x3])
    transpose(ds, 1:ncol(ds))
    transpose(groupby(ds,1:8), [2,3])
    t2 = now()
    Dataset(x1 = "Finished warmup in", x2 = t2-t1)
end
