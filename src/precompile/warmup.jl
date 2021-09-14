function warmup()
    t1 = now()
    ds = Dataset(x1 = rand(1:2, 1000), x2 = rand(1:2, 1000), x3 = rand(1:2, 1000) * 1.1, x4 = rand([Date(1), Date(2)], 1000), x5 = string.(rand(1:2), 1000),
                 x6 = PooledArray(Characters{2, UInt8}.(rand(1:2, 1000))), x7 = Int32.(rand(1:2, 1000)), x8 = Float32.(rand(1:2, 1000) * 1.1))
    for i in 1:8
        sortperm(ds, i)
    end
    for i in 1:8
        sortperm(ds, i, alg = QuickSort)
    end
    for i in 1:8
        sortperm(ds, 1:i)
    end
    for i in 1:7
        sortperm(ds, [8, i])
    end
    for i in 1:8
        groupby(ds, i)
    end
    for i in 1:8
        groupby(ds, 1:i)
    end
    combine(groupby(ds,1), Ref([1,2,3,7,8]) .=> [sum, mean, length, maximum, minimum, var, std, median, sort])
    ds2 = ds[1:2, [1,3]]
    innerjoin(ds, ds2, on = [:x1, :x3])
    transpose(ds, 1:8)
    transpose(groupby(ds,1), [2,3])
    t2 = now()
    Dataset(x1 = "Time to finish warmup", x2 = t2-t1)
end
