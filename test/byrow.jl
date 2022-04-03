@testset "general reduction" begin
    ds = Dataset(g = [1, 1, 1, 2, 2],
                        x1_int = [0, 0, 1, missing, 2],
                        x2_int = [3, 2, 1, 3, -2],
                        x1_float = [1.2, missing, -1.0, 2.3, 10],
                        x2_float = [missing, missing, 3.0, missing, missing],
                        x3_float = [missing, missing, -1.4, 3.0, -100.0])
    @test byrow(ds, sum, r"int") == [3,2,2,3,0]
    @test byrow(repeat(ds,100), sum, r"int") == repeat([3,2,2,3,0], 100)

    @test isequal(byrow(ds, sum, r"float") , [1.2, missing, 0.6000000000000001,5.3,-90])
    @test isequal(byrow(repeat(ds, 100), sum, r"float") , repeat([1.2, missing, 0.6000000000000001,5.3,-90], 100))
    @test byrow(ds, mean, r"int") == [3/2,2/2,1.0,3.0,0]
    @test byrow(repeat(ds,100), mean, r"int") == repeat([3/2,2/2,1.0,3.0,0],100)
    @test isequal(byrow(ds, maximum, r"float") , [1.2, missing, 3,3.0,10.0])
    @test isequal(byrow(ds, maximum, r"float", by = abs) , [1.2, missing, 3,3.0,100.0])
    @test isequal(byrow(ds, minimum, r"float") , [1.2, missing, -1.4,2.3,-100.0])

    @test isequal(byrow(repeat(ds,100), maximum, r"float") , repeat([1.2, missing, 3,3.0,10.0], 100))
    @test isequal(byrow(repeat(ds,100), maximum, r"float", by = abs) , repeat([1.2, missing, 3,3.0,100.0], 100))
    @test isequal(byrow(repeat(ds,100), minimum, r"float") , repeat([1.2, missing, -1.4,2.3,-100.0], 100))

    @test byrow(ds, sum, r"int", threads = true) == [3,2,2,3,0]
    @test isequal(byrow(ds, sum, r"float", threads = true) , [1.2, missing, 0.6000000000000001,5.3,-90])
    @test byrow(ds, mean, r"int", threads = true) == [3/2,2/2,1.0,3.0,0]
    @test isequal(byrow(ds, maximum, r"float", threads = true) , [1.2, missing, 3,3.0,10.0])
    @test isequal(byrow(ds, maximum, r"float", by = abs, threads = true) , [1.2, missing, 3,3.0,100.0])
    @test isequal(byrow(ds, minimum, r"float", threads = true) , [1.2, missing, -1.4,2.3,-100.0])

    @test byrow(ds, argmax, :) == Symbol.(["x2_int", "x2_int", "x2_float", "x2_int", "x1_float"])
    @test isequal(byrow(ds, argmin, r"float"), [:x1_float, missing, :x3_float, :x1_float, :x3_float])
    @test isequal(byrow(ds, argmin, r"float", by = abs) , [:x1_float, missing, :x1_float, :x1_float, :x1_float])
    @test byrow(ds, coalesce, ["x2_float", "x1_float", "x1_int"]) == [1.2,0,3.0,2.3,10]
    @test isequal(byrow(ds, var, r"float"), [missing, missing, 5.92, 0.24499999999999922, 6050.0])
    @test isequal(byrow(ds, var, r"float", dof = false), [0.0, missing, 3.9466666666666663, 0.12249999999999961, 3025.0])
    @test byrow(ds, count, :, by=  ismissing) == [2,3,0,2,1]

    @test byrow(repeat(ds, 100), argmax, :) == repeat(Symbol.(["x2_int", "x2_int", "x2_float", "x2_int", "x1_float"]),100)
    @test isequal(byrow(repeat(ds, 100), argmin, r"float"), repeat([:x1_float, missing, :x3_float, :x1_float, :x3_float],100))
    @test isequal(byrow(repeat(ds, 100), argmin, r"float", by = abs) , repeat([:x1_float, missing, :x1_float, :x1_float, :x1_float],100))
    @test byrow(repeat(ds, 100), coalesce, ["x2_float", "x1_float", "x1_int"]) == repeat([1.2,0,3.0,2.3,10],100)
    @test isequal(byrow(repeat(ds, 100), var, r"float"), repeat([missing, missing, 5.92, 0.24499999999999922, 6050.0],100))
    @test isequal(byrow(repeat(ds, 100), var, r"float", dof = false), repeat([0.0, missing, 3.9466666666666663, 0.12249999999999961, 3025.0],100))
    @test byrow(repeat(ds, 100), count, :, by=  ismissing) == repeat([2,3,0,2,1],100)

    sds = view(ds, [5,5,5,3,3,3,2,2,2,4,4,4,4,4], [4,1,2,5])
    @test byrow(sds, sum, :) == [14.0,14.,14,4,4,4,1,1,1,4.3,4.3,4.3,4.3,4.3]
    @test byrow(sds, sum, [:g, :x2_float]) == [2,2.0,2,4,4,4,1,1,1,2,2,2,2,2]
    @test byrow(sds, argmax, [3,2,4,1]) == Symbol.(["x1_float","x1_float","x1_float","x2_float","x2_float","x2_float","g", "g", "g", "x1_float","x1_float","x1_float","x1_float","x1_float"])
    @test byrow(sds, argmax, [1,2,4,3], by = ismissing) == Symbol.(["x2_float","x2_float","x2_float", "x1_float","x1_float","x1_float", "x1_float","x1_float","x1_float", "x2_float", "x2_float", "x2_float", "x2_float", "x2_float"])
    @test isequal(byrow(sds, argmax, [3,4]), [:x1_int,:x1_int,:x1_int,:x2_float,:x2_float,:x2_float,:x1_int,:x1_int,:x1_int,missing, missing, missing, missing, missing])
    @test isequal(byrow(sds, argmin, [3,4], threads = true), [:x1_int,:x1_int,:x1_int,:x1_int,:x1_int,:x1_int,:x1_int,:x1_int,:x1_int,missing, missing, missing, missing, missing])

    @test byrow(sds, any, :, by = ismissing) == [true, true, true, false, false, false, true, true, true, true, true, true, true, true]

    ds = Dataset(x1 = [1,2,3,4,missing], x2 = [3,2,4,5, missing])
    @test byrow(ds, isequal, :) == [false, true, false, false, true]
    sds = view(ds, [1,2,2,1,3,4,5,5,5], [2,1])
    @test byrow(sds, isequal, :) == [0,1,1,0,0,0, 1,1,1]
    @test byrow(sds, isequal, [1]) == ones(9)

    ds = Dataset(x1 = [1,2,3,4,missing], x2 = [3,2,4,5, missing])
    @test byrow(ds, isequal, :, threads = true) == [false, true, false, false, true]
    sds = view(ds, [1,2,2,1,3,4,5,5,5], [2,1])
    @test byrow(sds, isequal, :, threads = true) == [0,1,1,0,0,0, 1,1,1]
    @test byrow(sds, isequal, [1], threads = true) == ones(9)

    ds = Dataset(x1 = [1,2,3,1,1], x2 =[2,2,3,missing, 1], x3 = [1,1,1,1,1])

    @test byrow(ds, isequal, 1:2, with = :x3) == [false, false, false, false, true]
    @test byrow(ds, isequal, 1:3, with = :x3) == [false, false, false, false, true]
    @test byrow(ds, isequal, [1,3], with = [1,1,1,1,1]) == [true, false, false, true, true]
    @test byrow(ds, isequal, 1:2, with = ds[!, :x3]) == [false, false, false, false, true]
    @test byrow(ds, isequal, 1:3, with = ds[:,3]) == [false, false, false, false, true]
    @test byrow(ds, isequal, 1, with = ds[:,3]) == [true, false, false, true, true]

    @test byrow(view(ds, :, :), isequal, 1:2, with = :x3) == [false, false, false, false, true]
    @test byrow(view(ds, :, :), isequal, 1:3, with = :x3) == [false, false, false, false, true]
    @test byrow(view(ds, :, :), isequal, [1,3], with = [1,1,1,1,1]) == [true, false, false, true, true]


    @test byrow(repeat(ds,100), isequal, 1:2, with = :x3) == repeat([false, false, false, false, true],100)
    @test byrow(repeat(ds,100), isequal, 1:3, with = :x3) == repeat([false, false, false, false, true],100)
    @test byrow(repeat(ds,100), isequal, [1,3], with = repeat([1,1,1,1,1],100)) == repeat([true, false, false, true, true],100)
    @test byrow(repeat(ds,100), isequal, 1:2, with = repeat(ds,100)[!, :x3]) == repeat([false, false, false, false, true],100)
    @test byrow(repeat(ds,100), isequal, 1:3, with = repeat(ds,100)[:,3]) == repeat([false, false, false, false, true],100)
    @test byrow(repeat(ds,100), isequal, 1, with = repeat(ds,100)[:,3]) == repeat([true, false, false, true, true],100)


    ds = Dataset(x1 = [1,2,3,4,missing], x2 = [3,2,4,5, missing])
    @test byrow(ds, issorted, :) == [true, true, true, true, true]
    @test byrow(ds, issorted, :, rev = true) == [false, true, false, false, true]

    ds = Dataset(randn(10000, 3), :auto)
    map!(ds, x->rand()<.1 ? missing : x, :)
    dsm = Matrix(ds)
    @test byrow(ds, issorted, :) == issorted.(eachrow(dsm))
    @test byrow(ds, issorted, :,  rev = true) == issorted.(eachrow(dsm), rev = true)
    insertcols!(ds, 1, :y=>rand(-1:1, nrow(ds)))
    dsm = Matrix(ds)
    @test byrow(ds, issorted, :) == byrow(ds, issorted, :, threads = false) == issorted.(eachrow(dsm))
    @test byrow(ds, issorted, :,  rev = true) == byrow(ds, issorted, :,  rev = true, threads = false) == issorted.(eachrow(dsm), rev = true)

    ds = Dataset(g = [1, 1, 1, 2, 2],
                        x1_int = [0, 0, 1, missing, 2],
                        x2_int = [3, 2, 1, 3, -2],
                        x1_float = [1.2, missing, -1.0, 2.3, 10],
                        x2_float = [missing, missing, 3.0, missing, missing],
                        x3_float = [missing, missing, -1.4, 3.0, -100.0])
    @test isequal(byrow(ds, findfirst, :, by = ismissing), [:x2_float, :x1_float, missing, :x1_int, :x2_float])
    @test isequal(byrow(ds, findlast, :, by = ismissing), [:x3_float, :x3_float, missing, :x2_float, :x2_float])
    @test isequal(byrow(ds, findfirst, :, by = x->isless(x,0)), [missing, missing, :x1_float, missing, :x2_int])
    @test isequal(byrow(ds, findlast, :, by = x->isless(x,0)), [missing, missing, :x3_float, missing, :x3_float])
    @test isequal(byrow(ds, findfirst, :, by = x->1), [:g,:g,:g, :g,:g])
    @test isequal(byrow(ds, findfirst, :), [:g,:g,:g, missing, missing])
    @test isequal(byrow(ds, findlast, :), [:g,:g,:x2_int, missing, missing])
    @test isequal(byrow(ds, findfirst, [3,2,1], by = isequal(2)) ,byrow(ds, findlast, 1:3, by = isequal(2)))
    @test isequal(byrow(ds, findfirst, 1:3, by = isequal(2)) ,byrow(ds, findlast, [3,2,1], by = isequal(2)))

    @test isequal(byrow(repeat(ds,100), findfirst, :, by = ismissing), repeat([:x2_float, :x1_float, missing, :x1_int, :x2_float],100))
    @test isequal(byrow(repeat(ds,100), findlast, :, by = ismissing), repeat([:x3_float, :x3_float, missing, :x2_float, :x2_float],100))
    @test isequal(byrow(repeat(ds,100), findfirst, :, by = x->isless(x,0)), repeat([missing, missing, :x1_float, missing, :x2_int],100))
    @test isequal(byrow(repeat(ds,100), findlast, :, by = x->isless(x,0)), repeat([missing, missing, :x3_float, missing, :x3_float],100))
    @test isequal(byrow(repeat(ds,100), findfirst, :, by = x->1), repeat([:g,:g,:g, :g,:g],100))
    @test isequal(byrow(repeat(ds,100), findfirst, :), repeat([:g,:g,:g, missing, missing],100))
    @test isequal(byrow(repeat(ds,100), findlast, :), repeat([:g,:g,:x2_int, missing, missing],100))
    @test isequal(byrow(repeat(ds,100), findfirst, [3,2,1], by = isequal(2)) ,byrow(repeat(ds,100), findlast, 1:3, by = isequal(2)))
    @test isequal(byrow(repeat(ds,100), findfirst, 1:3, by = isequal(2)) ,byrow(repeat(ds,100), findlast, [3,2,1], by = isequal(2)))


    sds = view(ds, rand(1:5, 100), [2,1,6,5,3,4])
    @test isequal(byrow(sds, findfirst,:, by = x->isless(x,0)), byrow(Dataset(sds), findfirst, :, by = x->isless(x,0)))
    @test isequal(byrow(sds, findlast,:, by = x->isless(x,0)), byrow(Dataset(sds), findlast, :, by = x->isless(x,0)))
    @test isequal(byrow(sds, findfirst,:, by = x->isless(x,0), threads = true), byrow(Dataset(sds), findfirst, :, by = x->isless(x,0)))
    @test isequal(byrow(sds, findlast,:, by = x->isless(x,0), threads = true), byrow(Dataset(sds), findlast, :, by = x->isless(x,0)))
    sds = view(ds, rand(1:5, 100), [2,1,6,5,3,4])
    @test isequal(byrow(sds, findfirst,:, by = x->isless(x,0)), byrow(Dataset(sds), findfirst, :, by = x->isless(x,0)))
    @test isequal(byrow(sds, findlast,:, by = x->isless(x,0)), byrow(Dataset(sds), findlast, :, by = x->isless(x,0)))
    @test isequal(byrow(sds, findfirst,:, by = x->isless(x,0), threads = true), byrow(Dataset(sds), findfirst, :, by = x->isless(x,0)))
    @test isequal(byrow(sds, findlast,:, by = x->isless(x,0), threads = true), byrow(Dataset(sds), findlast, :, by = x->isless(x,0)))

    ds = Dataset(g = [1, 1, 1, 2, 2],
                        x1_int = [0, 0, 1, missing, 2],
                        x2_int = [3, 2, 1, 3, -2],
                        x1_float = [1.2, missing, -1.0, 2.3, 10],
                        x2_float = [missing, missing, 3.0, missing, missing],
                        x3_float = [missing, missing, -1.4, 3.0, -100.0])
    @test isequal(byrow(ds, findfirst, r"int", item = [missing, missing, missing, missing, missing]), [missing, missing, missing, :x1_int, missing])
    @test isequal(byrow(ds, findfirst, r"int", item = [1,1,1,1,1]), [missing, missing, :x1_int, missing, missing])
    @test isequal(byrow(ds, findlast, r"int", item = [1,1,1,1,1]), [missing, missing, :x2_int, missing, missing])
    @test isequal(byrow(ds, findfirst, r"int", item = :x1_float, eq = isless), [:x2_int, missing, :x1_int, :x1_int, missing])
    @test isequal(byrow(ds, findlast, r"int", item = :x1_float, eq = isless), [:x2_int, missing, :x2_int, :x2_int, missing])

    repeat!(ds, 2)
    sds1 = view(ds, [6,7,8,9,10], [6,5,4,2,3,1])
    sds2 = view(ds, 6:10, 1:6)

    @test isequal(byrow(sds1, findfirst, r"int", item = [missing, missing, missing, missing, missing]), [missing, missing, missing, :x1_int, missing])
    @test isequal(byrow(sds1, findfirst, r"int", item = [1,1,1,1,1]), [missing, missing, :x1_int, missing, missing])
    @test isequal(byrow(sds1, findlast, r"int", item = [1,1,1,1,1]), [missing, missing, :x2_int, missing, missing])
    @test isequal(byrow(sds1, findfirst, r"int", item = :x1_float, eq = isless), [:x2_int, missing, :x1_int, :x1_int, missing])
    @test isequal(byrow(sds1, findlast, r"int", item = :x1_float, eq = isless), [:x2_int, missing, :x2_int, :x2_int, missing])

    @test isequal(byrow(sds2, findfirst, r"int", item = [missing, missing, missing, missing, missing]), [missing, missing, missing, :x1_int, missing])
    @test isequal(byrow(sds2, findfirst, r"int", item = [1,1,1,1,1]), [missing, missing, :x1_int, missing, missing])
    @test isequal(byrow(sds2, findlast, r"int", item = [1,1,1,1,1]), [missing, missing, :x2_int, missing, missing])
    @test isequal(byrow(sds2, findfirst, r"int", item = :x1_float, eq = isless), [:x2_int, missing, :x1_int, :x1_int, missing])
    @test isequal(byrow(sds2, findlast, r"int", item = :x1_float, eq = isless), [:x2_int, missing, :x2_int, :x2_int, missing])

    sds = view(ds, rand(1:5, 100), [2,1,3,4])
    @test isequal(byrow(sds, findfirst,[1,4,3,2], by = x->isless(x,0)), byrow(Dataset(sds), findfirst, [1,4,3,2], by = x->isless(x,0)))
    @test isequal(byrow(sds, findlast,[1,4,3,2], by = x->isless(x,0)), byrow(Dataset(sds), findlast, [1,4,3,2], by = x->isless(x,0)))
    @test isequal(byrow(sds, findfirst,[1,4,3,2], by = x->isless(x,0), threads = true), byrow(Dataset(sds), findfirst, [1,4,3,2], by = x->isless(x,0)))
    @test isequal(byrow(sds, findlast,[1,4,3,2], by = x->isless(x,0), threads = true), byrow(Dataset(sds), findlast, [1,4,3,2], by = x->isless(x,0)))

    ds = Dataset(x1 = [1,2,3,4], x2 = [1.5,6.5,3.4,2.4], x3 = [true, false, true, false], y1 = ["x2", "x1", missing, "x2"], y2 = [:x2, :x1, missing, :x2], y3 = [3,1,1,2])
    @test isequal(byrow(ds, select, 1:2, with = :y1), [1.5, 2,missing,2.4])
    @test isequal(byrow(ds, select, 1:2, with = :y2), [1.5, 2,missing,2.4])
    @test isequal(byrow(ds, select, 1:2, with = ds[!, :y1]), [1.5, 2,missing,2.4])
    @test isequal(byrow(ds, select, 1:2, with = ds[:, :y1]), [1.5, 2,missing,2.4])
    @test isequal(byrow(ds, select, [2,1], with = ds[:, :y1]), [1.5, 2,missing,2.4])
    @test isequal(byrow(ds, select, [2,1,3], with = ds[:, :y2]), [1.5, 2,missing,2.4])
    @test isequal(byrow(ds, select, [2,1,3], with = :y3), [1, 6.5,3.4,4])
    @test isequal(byrow(ds, select, [2,1,3], with = [3,1,1,2]), [1, 6.5,3.4,4])
    @test isequal(byrow(ds, select, [2,1,3], with = ds[!,  :y3]), [1, 6.5,3.4,4])

    @test isequal(byrow(repeat(ds,100), select, 1:2, with = :y1), repeat([1.5, 2,missing,2.4],100))
    @test isequal(byrow(repeat(ds,100), select, 1:2, with = :y2), repeat([1.5, 2,missing,2.4],100))
    @test isequal(byrow(repeat(ds,100), select, 1:2, with = repeat(ds,100)[!, :y1]), repeat([1.5, 2,missing,2.4],100))
    @test isequal(byrow(repeat(ds,100), select, 1:2, with = repeat(ds,100)[:, :y1]), repeat([1.5, 2,missing,2.4],100))
    @test isequal(byrow(repeat(ds,100), select, [2,1], with = repeat(ds,100)[:, :y1]), repeat([1.5, 2,missing,2.4],100))
    @test isequal(byrow(repeat(ds,100), select, [2,1,3], with = repeat(ds,100)[:, :y2]), repeat([1.5, 2,missing,2.4],100))
    @test isequal(byrow(repeat(ds,100), select, [2,1,3], with = :y3), repeat([1, 6.5,3.4,4],100))
    @test isequal(byrow(repeat(ds,100), select, [2,1,3], with = repeat([3,1,1,2],100)), repeat([1, 6.5,3.4,4],100))
    @test isequal(byrow(repeat(ds,100), select, [2,1,3], with = repeat(ds,100)[!,  :y3]), repeat([1, 6.5,3.4,4],100))

    repeat!(ds, 2)
    sds = view(ds, [5,6,7,8], [2,1,3,4])
    @test isequal(byrow(sds, select, 1:2, with = :y1), [1.5, 2,missing,2.4])
    @test isequal(byrow(sds, select, 1:2, with = sds[!, :y1]), [1.5, 2,missing,2.4])
    @test isequal(byrow(sds, select, 1:2, with = sds[:, :y1]), [1.5, 2,missing,2.4])

    sds = view(ds, 1:4, 1:6)
    @test isequal(byrow(sds, select, 1:2, with = :y1), [1.5, 2,missing,2.4])
    @test isequal(byrow(sds, select, 1:2, with = :y2), [1.5, 2,missing,2.4])
    @test isequal(byrow(sds, select, 1:2, with = sds[!, :y1]), [1.5, 2,missing,2.4])
    @test isequal(byrow(sds, select, 1:2, with = sds[:, :y1]), [1.5, 2,missing,2.4])
    @test isequal(byrow(sds, select, [2,1], with = sds[:, :y1]), [1.5, 2,missing,2.4])
    @test isequal(byrow(sds, select, [2,1,3], with = sds[:, :y2]), [1.5, 2,missing,2.4])
    @test isequal(byrow(sds, select, [2,1,3], with = :y3), [1, 6.5,3.4,4])
    @test isequal(byrow(sds, select, [2,1,3], with = [3,1,1,2]), [1, 6.5,3.4,4])
    @test isequal(byrow(sds, select, [2,1,3], with = sds[!,  :y3]), [1, 6.5,3.4,4])


    ds = Dataset(x1 = [1.0,missing,3,4], x2 = [1.5,6.5,missing,2.4], y3 = [3,1,1,2])
    @test byrow(ds, fill, r"x", with = :y3) == Dataset(x1 = [1.0,1,3,4], x2 = [1.5,6.5,1,2.4], y3 = [3,1,1,2])
    @test byrow(ds, fill, [1, 2], with = :y3) == Dataset(x1 = [1.0,1,3,4], x2 = [1.5,6.5,1,2.4], y3 = [3,1,1,2])
    @test byrow(ds, fill, 1:2, with = 3) == Dataset(x1 = [1.0,1,3,4], x2 = [1.5,6.5,1,2.4], y3 = [3,1,1,2])
    @test byrow(ds, fill, r"x", with = :y3, rolling = true) == Dataset(x1 = [1.0,1,3,4], x2 = [1.5,6.5,3,2.4], y3 = [3,1,1,2])
    @test byrow(ds, fill, r"x", with = :y3, by = x->isless(2,x)) == Dataset(x1 = [1.0,1,1,2], x2 = [1.5,1,1,2], y3 = [3,1,1,2])
    @test byrow(ds, fill, r"x", with = :y3, by = x->isless(2,x), rolling = true) == Dataset(x1 = [1.0,1,1,2.0], x2 = [1.5,1,1,2], y3 = [3,1,1,2])
    @test byrow(ds, fill, r"x", with = [missing, missing, missing, missing], by = x->isless(2,x), rolling = true) == Dataset(x1 = [1.0,missing, missing, missing], x2 = [1.5,missing, missing, missing], y3 = [3,1,1,2])

    @test byrow(repeat(ds,100), fill, r"x", with = :y3) == repeat(Dataset(x1 = [1.0,1,3,4], x2 = [1.5,6.5,1,2.4], y3 = [3,1,1,2]),100)
    @test byrow(repeat(ds,100), fill, [1, 2], with = :y3) == repeat(Dataset(x1 = [1.0,1,3,4], x2 = [1.5,6.5,1,2.4], y3 = [3,1,1,2]),100)
    @test byrow(repeat(ds,100), fill, 1:2, with = 3) == repeat(Dataset(x1 = [1.0,1,3,4], x2 = [1.5,6.5,1,2.4], y3 = [3,1,1,2]),100)
    @test byrow(repeat(ds,100), fill, r"x", with = :y3, rolling = true) == repeat(Dataset(x1 = [1.0,1,3,4], x2 = [1.5,6.5,3,2.4], y3 = [3,1,1,2]),100)
    @test byrow(repeat(ds,100), fill, r"x", with = :y3, by = x->isless(2,x)) == repeat(Dataset(x1 = [1.0,1,1,2], x2 = [1.5,1,1,2], y3 = [3,1,1,2]),100)
    @test byrow(repeat(ds,100), fill, r"x", with = :y3, by = x->isless(2,x), rolling = true) == repeat(Dataset(x1 = [1.0,1,1,2.0], x2 = [1.5,1,1,2], y3 = [3,1,1,2]),100)
    @test byrow(repeat(ds,100), fill, r"x", with = repeat([missing, missing, missing, missing],100), by = x->isless(2,x), rolling = true) == repeat(Dataset(x1 = [1.0,missing, missing, missing], x2 = [1.5,missing, missing, missing], y3 = [3,1,1,2]),100)

    repeat!(ds, 2)
    sds = view(ds, [5,6,7,8], [1,2,3])
    @test byrow(sds, fill, r"x", with = :y3) == Dataset(x1 = [1.0,1,3,4], x2 = [1.5,6.5,1,2.4], y3 = [3,1,1,2])
    @test byrow(sds, fill, [1, 2], with = :y3) == Dataset(x1 = [1.0,1,3,4], x2 = [1.5,6.5,1,2.4], y3 = [3,1,1,2])
    @test byrow(sds, fill, 1:2, with = 3) == Dataset(x1 = [1.0,1,3,4], x2 = [1.5,6.5,1,2.4], y3 = [3,1,1,2])
    @test byrow(sds, fill, r"x", with = :y3, rolling = true) == Dataset(x1 = [1.0,1,3,4], x2 = [1.5,6.5,3,2.4], y3 = [3,1,1,2])
    @test byrow(sds, fill, r"x", with = :y3, by = x->isless(2,x)) == Dataset(x1 = [1.0,1,1,2], x2 = [1.5,1,1,2], y3 = [3,1,1,2])
    @test byrow(sds, fill, r"x", with = :y3, by = x->isless(2,x), rolling = true) == Dataset(x1 = [1.0,1,1,2.0], x2 = [1.5,1,1,2], y3 = [3,1,1,2])
    @test byrow(sds, fill, r"x", with = [missing, missing, missing, missing], by = x->isless(2,x), rolling = true) == Dataset(x1 = [1.0,missing, missing, missing], x2 = [1.5,missing, missing, missing], y3 = [3,1,1,2])

    sds = view(ds, 1:4, 1:3)
    @test byrow(sds, fill, r"x", with = :y3) == Dataset(x1 = [1.0,1,3,4], x2 = [1.5,6.5,1,2.4], y3 = [3,1,1,2])
    @test byrow(sds, fill, [1, 2], with = :y3) == Dataset(x1 = [1.0,1,3,4], x2 = [1.5,6.5,1,2.4], y3 = [3,1,1,2])
    @test byrow(sds, fill, 1:2, with = 3) == Dataset(x1 = [1.0,1,3,4], x2 = [1.5,6.5,1,2.4], y3 = [3,1,1,2])
    @test byrow(sds, fill, r"x", with = :y3, rolling = true) == Dataset(x1 = [1.0,1,3,4], x2 = [1.5,6.5,3,2.4], y3 = [3,1,1,2])
    @test byrow(sds, fill, r"x", with = :y3, by = x->isless(2,x)) == Dataset(x1 = [1.0,1,1,2], x2 = [1.5,1,1,2], y3 = [3,1,1,2])
    @test byrow(sds, fill, r"x", with = :y3, by = x->isless(2,x), rolling = true) == Dataset(x1 = [1.0,1,1,2.0], x2 = [1.5,1,1,2], y3 = [3,1,1,2])
    @test byrow(sds, fill, r"x", with = [missing, missing, missing, missing], by = x->isless(2,x), rolling = true) == Dataset(x1 = [1.0,missing, missing, missing], x2 = [1.5,missing, missing, missing], y3 = [3,1,1,2])

    ds = Dataset(x1 = [1.0,missing,3,4], x2 = [1.5,6.5,missing,2.4], y3 = [3,1,1,2])
    setformat!(ds,:x1=>sqrt)
    byrow(ds, fill!, r"x", with = :y3)
    @test getformat(ds,:x1) == sqrt
    ds = Dataset(x1 = [1.0,missing,3,4], x2 = [1.5,6.5,missing,2.4], y3 = [3,1,1,2])
    setformat!(ds,:x1=>sqrt)
    repeat!(ds, 2)
    sds = view(ds, [5,6,7,8], [1,2,3])
    byrow(sds, fill!, r"x", with = :y3)
    @test getformat(ds,:x1) == sqrt

    ds = Dataset(x1 = [1.0,missing,3,4], x2 = [1.5,6.5,missing,2.4], y3 = [3,1,1,2])
    sort!(ds, 2)
    byrow(ds, fill!, r"x", with = :y3)
    @test IMD.index(ds).sortedcols == []
    ds = Dataset(x1 = [1.0,missing,3,4], x2 = [1.5,6.5,missing,2.4], y3 = [3,1,1,2])
    repeat!(ds, 2)
    sort!(ds, 2)
    sds = view(ds, [5,6,7,8], [3,1,2])
    byrow(sds, fill!, r"x", with = [0,0,0,0])
    @test IMD.index(ds).sortedcols == []

    ds = Dataset(x1 = [1.0,missing,3,4], x2 = [1.5,6.5,missing,2.4], y3 = [3,1,1,2])
    sort!(ds, 3)
    byrow(ds, fill!, 1:2, with = 3)
    @test IMD.index(ds).sortedcols == [3]

    ds = Dataset(rand(1:10, 10000, 4), :auto)
    @test byrow(ds, fill, :, with = missings(Int, nrow(ds)), by = isequal(1), threads = false) == byrow(ds, fill, :, with = missings(Int, nrow(ds)), by = isequal(1), threads = true)
    byrow(ds, fill!, :, with = missings(Int, nrow(ds)), by = isequal(1))
    @test IMD.minimum.(eachcol(ds)) == [2,2,2,2]

    ds = Dataset(g = [1, 1, 1, 2, 2],
                        x1_int = [0, 0, 1, missing, 2],
                        x2_int = [3, 2, 1, 3, -2],
                        x1_float = [1.2, missing, -1.0, 2.3, 10],
                        x2_float = [missing, missing, 3.0, missing, missing],
                        x3_float = [missing, missing, -1.4, 3.0, -100.0])
    @test byrow(ds, isless, r"int", with = :x1_float) == [0,1,0,0,1]
    @test byrow(ds, isless, r"int", with = :x1_float, rev = true) == [0,0,1,1,0]
    @test byrow(ds, isless, r"int", with = ds[!,:x1_float]) == [0,1,0,0,1]
    @test byrow(ds, isless, r"int", with = ds[!,:x1_float], rev = true) == [0,0,1,1,0]
    @test byrow(ds, isless, r"int", with = ds[:,:x1_float]) == [0,1,0,0,1]
    @test byrow(ds, isless, r"int", with = ds[:,:x1_float], rev = true) == [0,0,1,1,0]
    @test byrow(view(ds, :, :), isless, r"int", with = :x1_float) == [0,1,0,0,1]
    @test byrow(view(ds, :, :), isless, r"int", with = :x1_float, rev = true) == [0,0,1,1,0]
    @test byrow(view(ds, :, :), isless, r"int", with = view(ds, :, :)[!,:x1_float]) == [0,1,0,0,1]
    @test byrow(view(ds, :, :), isless, r"int", with = ds[!,:x1_float], rev = true) == [0,0,1,1,0]

    ds = Dataset(rand(100, 10), :auto)
    _tval = byrow(view(ds, :, 1:9) .> ds[!, 10], all)
    @test byrow(ds, isless, 1:9, with=:x10, rev = true) == _tval

    ds = Dataset(x1 = [1,2,3,1,1], x2 = [4,3,1,2,5], x3 = 1:5)
    @test byrow(ds, in, 1:2, item = :x3) == [1,1,1,0,1]
    @test byrow(ds, in, 1:2, item = ds[!, 3])== [1,1,1,0,1]
    @test byrow(ds, in, 1:2, item = ds[:, 3])== [1,1,1,0,1]

    repeat!(ds, 2)
    sds1 = view(ds, [6,7,8,9,10], [2,1,3])
    sds2 = view(ds, 6:10, 1:3)
    @test byrow(sds1, in, 1:2, item = :x3) == [1,1,1,0,1]
    @test byrow(sds1, in, 1:2, item = sds1[!, 3])== [1,1,1,0,1]
    @test byrow(sds1, in, 1:2, item = sds1[:, 3])== [1,1,1,0,1]
    @test byrow(sds2, in, 1:2, item = :x3) == [1,1,1,0,1]
    @test byrow(sds2, in, 1:2, item = sds2[!, 3])== [1,1,1,0,1]
    @test byrow(sds2, in, 1:2, item = sds2[:, 3])== [1,1,1,0,1]

    ds = Dataset(x1 = ["A", "B,"], x2 =["TEA", "TOOOOL"])
    @test byrow(ds, join, r"x") == ["ATEA", "B,TOOOOL"]
    @test byrow(ds, join, r"x", delim = ",") == ["A,TEA", "B,,TOOOOL"]
    @test byrow(ds, join, r"x", last = ".") == ["A.TEA", "B,.TOOOOL"]
    @test byrow(ds, join, r"x", last = "end", delim = "/-/") == ["AendTEA", "B,endTOOOOL"]

    @test byrow(view(ds, [1,2], :), join, r"x") == ["ATEA", "B,TOOOOL"]
    @test byrow(view(ds, [1,2], :), join, r"x", delim = ",") == ["A,TEA", "B,,TOOOOL"]
    @test byrow(view(ds, [1,2], :), join, r"x", last = ".") == ["A.TEA", "B,.TOOOOL"]
    @test byrow(view(ds, [1,2], :), join, r"x", last = "end", delim = "/-/") == ["AendTEA", "B,endTOOOOL"]

    repeat!(ds, 1000)
    @test byrow(ds, join, r"x", threads = true) == repeat(["ATEA", "B,TOOOOL"], 1000)
    @test byrow(ds, join, r"x", threads = true, delim = ",") == repeat(["A,TEA", "B,,TOOOOL"], 1000)
    @test byrow(ds, join, r"x", threads = true, last = ".") == repeat(["A.TEA", "B,.TOOOOL"], 1000)
    @test byrow(ds, join, r"x", threads = true, last = "end", delim = "/-/") == repeat(["AendTEA", "B,endTOOOOL"], 1000)

    ds = Dataset(x = [1,10], x2 = ["A", "BC"], x3 = [2.0,4.54])
    @test byrow(ds, join, :, delim = "--", last = "-") == ["1--A-2.0", "10--BC-4.54"]


    ds = Dataset(x = ["α1", "β∘1"], y = [1,2], z = [1.4,5.6])
    @test byrow(ds, join, :) == ["α111.4", "β∘125.6"]
    @test byrow(ds, join, :, delim = ",") == ["α1,1,1.4", "β∘1,2,5.6"]
    @test byrow(ds, join, :, delim = ":×:") == ["α1:×:1:×:1.4", "β∘1:×:2:×:5.6"]


    # promoting small ints
    ds = Dataset(x1=Int8[127,127],x2=Int8[127,-127])
    @test byrow(ds,sum) == [254,0]
    @test byrow(ds,prod) == [16129,-16129]
    ds = Dataset(x1=[true,true,false],x2=[true,true,false],x3=[true,false,true])
    @test byrow(ds,sum)==[3,2,1]
end

@testset "cum*/! - sort/!" begin
    ds = Dataset(x1 = [1,missing,3,missing], x2 = [1.0,2.0,missing,4.0], x3 = [1,missing,3,4])
    @test byrow(ds, cumsum, :) == Dataset(x1 = [1.0, missing, 3.0, missing], x2 = [2.0, 2.0, 3.0, 4.0], x3 = [3.0, 2.0, 6.0, 8.0])
    @test byrow(ds, cumsum, :, missings = :skip) == Dataset(x1 = [1.0, missing, 3.0, missing], x2 = [2.0, 2.0, missing, 4.0], x3=[3.0, missing, 6.0, 8.0])
    @test byrow(ds, cumprod, :) == Dataset(x1 = [1.0, missing, 3.0, missing], x2 = [1.0, 2.0, 3.0, 4.0], x3=[1.0, 2.0, 9.0, 16.0])
    @test byrow(ds, cumprod, :, missings = :skip) == Dataset(x1 = [1.0, missing, 3.0, missing], x2 = [1.0, 2.0, missing, 4.0], x3=[1.0, missing, 9.0, 16.0])

    @test byrow(repeat(ds,100), cumsum, :) == repeat(Dataset(x1 = [1.0, missing, 3.0, missing], x2 = [2.0, 2.0, 3.0, 4.0], x3 = [3.0, 2.0, 6.0, 8.0]),100)
    @test byrow(repeat(ds,100), cumsum, :, missings = :skip) == repeat(Dataset(x1 = [1.0, missing, 3.0, missing], x2 = [2.0, 2.0, missing, 4.0], x3=[3.0, missing, 6.0, 8.0]),100)
    @test byrow(repeat(ds,100), cumprod, :) == repeat(Dataset(x1 = [1.0, missing, 3.0, missing], x2 = [1.0, 2.0, 3.0, 4.0], x3=[1.0, 2.0, 9.0, 16.0]),100)
    @test byrow(repeat(ds,100), cumprod, :, missings = :skip) == repeat(Dataset(x1 = [1.0, missing, 3.0, missing], x2 = [1.0, 2.0, missing, 4.0], x3=[1.0, missing, 9.0, 16.0]),100)

    byrow(ds, cumsum!, :, missings = :skip)
    @test ds == Dataset(x1 = [1.0, missing, 3.0, missing], x2 = [2.0, 2.0, missing, 4.0], x3=[3.0, missing, 6.0, 8.0])
    byrow(ds, cumsum!, :, missings = :ignore)
    @test ds == Dataset(x1 = [1.0, missing, 3.0, missing], x2 = [3.0, 2.0, 3.0, 4.0], x3=[6.0, 2.0, 9.0, 12.0])
    ds = Dataset(x1 = [1,missing,3,missing], x2 = [1.0,2.0,missing,4.0], x3 = [1,missing,3,4])
    byrow(ds, cumprod!, :, missings = :skip)
    @test ds == Dataset(x1 = [1.0, missing, 3.0, missing], x2 = [1.0, 2.0, missing, 4.0], x3=[1.0, missing, 9.0, 16.0])
    byrow(ds, cumprod!, :, missings = :ignore)
    @test ds == Dataset(x1 = [1.0, missing, 3.0, missing], x2 = [1.0, 2.0, 3.0, 4.0], x3=[1.0, 2.0, 27.0, 64.0])

    ds = Dataset(x1 = [1,missing,3,missing], x2 = [1.0,2.0,missing,4.0], x3 = [1,missing,3,4])
    sds = view(ds, [1,2,1,3,1,4], [3,1,2])
    @test byrow(sds, cumsum, :) == Dataset(x3 = [1.0, missing, 1,3,1,4], x1 = [2, missing, 2.0, 6, 2,4], x2 = [3.0, 2.0, 3, 6, 3, 8])
    @test byrow(sds, cumsum, :, missings = :skip) == Dataset(x3 = [1.0, missing, 1,3,1,4], x1 = [2, missing, 2.0, 6, 2,missing], x2 = [3.0, 2.0, 3, missing, 3, 8])
    @test byrow(sds, cumprod, :) == Dataset(x3 = [1.0, missing, 1,3,1,4], x1 = [1, missing, 1.0, 9, 1,4], x2 = [1.0, 2.0, 1, 9, 1, 16])
    @test byrow(sds, cumprod, :, missings = :skip) == Dataset(x3 = [1.0, missing, 1,3,1,4], x1 = [1, missing, 1.0, 9, 1,missing], x2 = [1.0, 2.0, 1, missing, 1, 16])

    ds = Dataset(x1 = [1,missing,3,missing], x2 = [1.0,2.0,missing,4.0], x3 = [1,missing,3,4])
    @test byrow(ds, cummax, :) == Dataset(x1 = [1.0, missing, 3.0, missing], x2 = [1.0, 2.0, 3.0, 4.0], x3 = [1.0, 2.0, 3.0, 4.0])
    @test byrow(ds, cummax, :, missings = :skip) == Dataset(x1 = [1.0, missing, 3.0, missing], x2 = [1.0, 2.0, missing, 4.0], x3 = [1.0, missing, 3.0, 4.0])
    @test byrow(ds, cummin, :) == Dataset(x1 = [1.0, missing, 3.0, missing], x2 = [1.0, 2.0, 3.0, 4.0], x3 = [1.0, 2.0, 3.0, 4.0])
    @test byrow(ds, cummin, :, missings = :skip) == Dataset(x1 = [1.0, missing, 3.0, missing], x2 = [1.0, 2.0, missing, 4.0], x3 = [1.0, missing, 3.0, 4.0])

    byrow(ds, cummax!, :)
    @test ds == Dataset(x1 = [1.0, missing, 3.0, missing], x2 = [1.0, 2.0, 3.0, 4.0], x3 = [1.0, 2.0, 3.0, 4.0])
    ds = Dataset(x1 = [1,missing,3,missing], x2 = [1.0,2.0,missing,4.0], x3 = [1,missing,3,4])
    byrow(ds, cummin!, :, missings = :skip)
    @test ds == Dataset(x1 = [1.0, missing, 3.0, missing], x2 = [1.0, 2.0, missing, 4.0], x3 = [1.0, missing, 3.0, 4.0])

    ds = Dataset(x1 = [1,missing,3,missing], x2 = [1.0,2.0,missing,4.0], x3 = [1,missing,3,6])
    sds = view(ds, [1,2,1,3,1,4], [3,1,2])
    @test byrow(sds, cummax, :) == Dataset(x3 = [1.0, missing, 1,3,1,6], x1 = [1, missing, 1,3,1,6.0], x2 = [1.0,2.0, 1.0,3.0,1.0,6.0])
    @test byrow(sds, cummax, :, missings = :skip) == Dataset(x3 = [1.0, missing, 1,3,1,6], x1 = [1, missing, 1,3,1.0, missing], x2 = [1.0,2.0, 1.0,missing,1.0,6.0])

    ds = Dataset(rand(1:100, 1000, 10), :auto)
    ds2 = byrow(ds, sort, :)
    @test all(byrow(ds2, issorted, :))
    @test Matrix(ds2) == sort(Matrix(ds), dims = 2)

    ds = Dataset(rand(1:100, 1000, 10), :auto)
    ds2 = byrow(view(ds, :, :), sort, :)
    @test all(byrow(ds2, issorted, :))
    @test Matrix(ds2) == sort(Matrix(ds), dims = 2)
end
