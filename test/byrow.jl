@testset "general reduction" begin
    ds = Dataset(g = [1, 1, 1, 2, 2],
                        x1_int = [0, 0, 1, missing, 2],
                        x2_int = [3, 2, 1, 3, -2],
                        x1_float = [1.2, missing, -1.0, 2.3, 10],
                        x2_float = [missing, missing, 3.0, missing, missing],
                        x3_float = [missing, missing, -1.4, 3.0, -100.0])
    @test byrow(ds, sum, r"int") == [3,2,2,3,0]
    @test isequal(byrow(ds, sum, r"float") , [1.2, missing, 0.6000000000000001,5.3,-90])
    @test byrow(ds, mean, r"int") == [3/2,2/2,1.0,3.0,0]
    @test isequal(byrow(ds, maximum, r"float") , [1.2, missing, 3,3.0,10.0])
    @test isequal(byrow(ds, maximum, r"float", by = abs) , [1.2, missing, 3,3.0,100.0])
    @test isequal(byrow(ds, minimum, r"float") , [1.2, missing, -1.4,2.3,-100.0])

    @test byrow(ds, sum, r"int", threads = true) == [3,2,2,3,0]
    @test isequal(byrow(ds, sum, r"float", threads = true) , [1.2, missing, 0.6000000000000001,5.3,-90])
    @test byrow(ds, mean, r"int", threads = true) == [3/2,2/2,1.0,3.0,0]
    @test isequal(byrow(ds, maximum, r"float", threads = true) , [1.2, missing, 3,3.0,10.0])
    @test isequal(byrow(ds, maximum, r"float", by = abs, threads = true) , [1.2, missing, 3,3.0,100.0])
    @test isequal(byrow(ds, minimum, r"float", threads = true) , [1.2, missing, -1.4,2.3,-100.0])

    @test byrow(ds, argmax, :) == ["x2_int", "x2_int", "x2_float", "x2_int", "x1_float"]
    @test byrow(ds, argmin, r"float") == ["x1_float", "x1_float", "x3_float", "x1_float", "x3_float"]
    @test byrow(ds, argmin, r"float", by = abs) == ["x1_float", "x1_float", "x1_float", "x1_float", "x1_float"]
    @test byrow(ds, coalesce, ["x2_float", "x1_float", "x1_int"]) == [1.2,0,3.0,2.3,10]
    @test isequal(byrow(ds, var, r"float"), [missing, missing, 5.92, 0.24499999999999922, 6050.0])
    @test isequal(byrow(ds, var, r"float", dof = false), [0.0, missing, 3.9466666666666663, 0.12249999999999961, 3025.0])
    @test byrow(ds, count, :, by=  ismissing) == [2,3,0,2,1]
    sds = view(ds, [5,5,5,3,3,3,2,2,2,4,4,4,4,4], [4,1,2,5])
    @test byrow(sds, sum, :) == [14.0,14.,14,4,4,4,1,1,1,4.3,4.3,4.3,4.3,4.3]
    @test byrow(sds, sum, [:g, :x2_float]) == [2,2.0,2,4,4,4,1,1,1,2,2,2,2,2]
    @test byrow(sds, argmax, [3,2,4,1]) == ["x1_float","x1_float","x1_float","x2_float","x2_float","x2_float","g", "g", "g", "x1_float","x1_float","x1_float","x1_float","x1_float"]
    @test byrow(sds, argmax, [1,2,4,3], by = ismissing) == ["x2_float","x2_float","x2_float", "x1_float","x1_float","x1_float", "x1_float","x1_float","x1_float", "x2_float", "x2_float", "x2_float", "x2_float", "x2_float"]
    @test byrow(sds, any, :, by = ismissing) == [true, true, true, false, false, false, true, true, true, true, true, true, true, true]
end

@testset "cumsum/! and cumprod/!" begin
    ds = Dataset(x1 = [1,missing,3,missing], x2 = [1.0,2.0,missing,4.0], x3 = [1,missing,3,4])
    @test byrow(ds, cumsum, :) == Dataset(x1 = [1.0, missing, 3.0, missing], x2 = [2.0, 2.0, 3.0, 4.0], x3 = [3.0, 2.0, 6.0, 8.0])
    @test byrow(ds, cumsum, :, missings = :skip) == Dataset(x1 = [1.0, missing, 3.0, missing], x2 = [2.0, 2.0, missing, 4.0], x3=[3.0, missing, 6.0, 8.0])
    @test byrow(ds, cumprod, :) == Dataset(x1 = [1.0, missing, 3.0, missing], x2 = [1.0, 2.0, 3.0, 4.0], x3=[1.0, 2.0, 9.0, 16.0])
    @test byrow(ds, cumprod, :, missings = :skip) == Dataset(x1 = [1.0, missing, 3.0, missing], x2 = [1.0, 2.0, missing, 4.0], x3=[1.0, missing, 9.0, 16.0])

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
end
