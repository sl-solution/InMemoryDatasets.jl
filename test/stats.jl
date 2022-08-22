using Random,PooledArrays,CategoricalArrays
@testset "topk" begin
    # general usage
    for i in 1:100
        x = rand(Int, 11)
        for j in 1:11
            @test partialsort(x, 1:j) == topk(x, j, rev=true)
            @test partialsort(x, 1:j, rev=true) == topk(x, j)
            @test partialsortperm(x, 1:j) == topkperm(x, j, rev=true)
            @test partialsortperm(x, 1:j, rev=true) == topkperm(x, j)
        end
        x = rand(11)
        for j in 1:11
            @test partialsort(x, 1:j) == topk(x, j, rev=true)
            @test partialsort(x, 1:j, rev=true) == topk(x, j)
            @test partialsortperm(x, 1:j) == topkperm(x, j, rev=true)
            @test partialsortperm(x, 1:j, rev=true) == topkperm(x, j)
        end
        x = randn(11)
        for j in 1:11
            @test partialsort(x, 1:j) == topk(x, j, rev=true)
            @test partialsort(x, 1:j, rev=true) == topk(x, j)
            @test partialsortperm(x, 1:j) == topkperm(x, j, rev=true)
            @test partialsortperm(x, 1:j, rev=true) == topkperm(x, j)
        end
        x = rand(Int8, 10000)
        for j in 1:30
            @test partialsort(x, 1:j) == topk(x, j, rev=true) == topk(x, j, rev=true, threads=true)
            @test partialsort(x, 1:j, rev=true) == topk(x, j) == topk(x, j, threads=true)
            @test partialsortperm(x, 1:j) == topkperm(x, j, rev=true) == topkperm(x, j, rev=true, threads=true)
            @test partialsortperm(x, 1:j, rev=true) == topkperm(x, j) == topkperm(x, j, threads=true)
            @test abs.(partialsort(x, 1:j, by=abs)) == abs.(topk(x, j, rev=true, by=abs)) == abs.(topk(x, j, rev=true, by=abs, threads=true))
            @test abs.(partialsort(x, 1:j, rev=true, by=abs)) == abs.(topk(x, j, by=abs)) == abs.(topk(x, j, by=abs, threads=true))
            @test partialsortperm(x, 1:j, by=abs) == topkperm(x, j, rev=true, by=abs) == topkperm(x, j, rev=true, by=abs, threads=true)
            @test partialsortperm(x, 1:j, rev=true, by=abs) == topkperm(x, j, by=abs) == topkperm(x, j, by=abs, threads = true)
        end
        x = zeros(Bool, 11)
        for j in 1:15
            @test partialsort(x, 1:min(11, j)) == topk(x, j, rev=true)
            @test partialsort(x, 1:min(j, 11), rev=true) == topk(x, j)
            @test partialsortperm(x, 1:min(11, j)) == topkperm(x, j, rev=true)
            @test partialsortperm(x, 1:min(11, j), rev=true) == topkperm(x, j)
        end
        x = ones(Bool, 11)
        for j in 1:15
            @test partialsort(x, 1:min(11, j)) == topk(x, j, rev=true)
            @test partialsort(x, 1:min(j, 11), rev=true) == topk(x, j)
            @test partialsortperm(x, 1:min(11, j)) == topkperm(x, j, rev=true)
            @test partialsortperm(x, 1:min(11, j), rev=true) == topkperm(x, j)
        end
        x = [randstring() for _ in 1:101]
        for j in 1:30
            @test partialsort(x, 1:j) == topk(x, j, rev=true) == topk(x, j, rev=true, threads=true)
            @test partialsort(x, 1:j, rev=true) == topk(x, j) == topk(x, j, threads=true)
            @test partialsortperm(x, 1:j) == topkperm(x, j, rev=true) == topkperm(x, j, rev=true, threads=true)
            @test partialsortperm(x, 1:j, rev=true) == topkperm(x, j) == topkperm(x, j, threads = true)
        end
        x = PooledArray(rand(1:100, 100))
        for j in 1:50
            @test partialsort(x, 1:j) == topk(x, j, rev=true) == topk(x, j, rev=true, threads=true)
            @test partialsort(x, 1:j, rev=true) == topk(x, j) == topk(x, j, threads=true)
            @test partialsortperm(x, 1:j) == topkperm(x, j, rev=true) == topkperm(x, j, rev=true, threads=true)
            @test partialsortperm(x, 1:j, rev=true) == topkperm(x, j) == topkperm(x, j, threads = true)
        end
        x = CategoricalArray(rand(100))
        for j in 1:50
            @test partialsort(x, 1:j) == topk(x, j, rev=true, lt = isless) 
            @test partialsort(x, 1:j, rev=true) == topk(x, j, lt = isless) 
            @test partialsortperm(x, 1:j) == topkperm(x, j, rev=true, lt = isless)
            @test partialsortperm(x, 1:j, rev=true) == topkperm(x, j, lt = isless)
        end

    end
    x = [1, 10, missing, 100, -1000, 32, 54, 0, missing, missing, -1]
    @test topk(x, 2) == [100, 54] == topk(x, 2, threads = true)
    @test topk(x, 2, rev=true) == [-1000, -1] == topk(x, 2, rev=true, threads = true)
    @test topkperm(x, 2) == [4, 7] == topkperm(x, 2, threads = true)
    @test topkperm(x, 2, rev=true) == [5, 11] == topkperm(x, 2, rev=true, threads = true)
    @test topk(x, 10) == [100, 54, 32, 10, 1, 0, -1, -1000] == topk(x, 10, threads = true)
    @test topk(x, 10, rev=true) == [-1000, -1, 0, 1, 10, 32, 54, 100] == topk(x, 10, rev=true, threads = true)
    @test topkperm(x, 10) == [4, 7, 6, 2, 1, 8, 11, 5] == topkperm(x, 10, threads = true)
    @test topkperm(x, 10, rev=true) == [5, 11, 8, 1, 2, 6, 7, 4] == topkperm(x, 10, rev=true, threads = true)
    @test isequal(topk([missing, missing], 2), [missing])
    @test isequal(topk([missing, missing], 2, rev=true), [missing])
    @test isequal(topkperm([missing, missing], 2), [missing])
    @test isequal(topkperm([missing, missing], 2, rev=true), [missing])
    @test topk(x, 2, by=abs) == [-1000, 100] == topk(x, 2, by=abs, threads = true)
    @test topk(x, 2, by=abs, rev=true) == [0, 1] == topk(x, 2, by=abs, rev=true, threads = true)
    @test topkperm(x, 2, by=abs) == [5, 4] == topkperm(x, 2, by=abs, threads = true)
    @test topkperm(x, 2, by=abs, rev=true) == [8, 1] == topkperm(x, 2, by=abs, rev=true, threads = true)

    x = Int8[-128, -128, -128]
    y = Union{Int8,Missing}[-128, -128, missing, missing, -128]

    @test topk(x, 2) == [-128, -128]
    @test topk(x, 2, rev=true) == [-128, -128]
    @test topkperm(x, 2, rev=true) == [1, 2]
    @test topkperm(x, 2) == [1, 2]

    @test topk(y, 3) == [-128, -128, -128]
    @test topk(y, 3, rev=true) == [-128, -128, -128]
    @test topkperm(y, 3, rev=true) == [1, 2, 5]
    @test topkperm(y, 3) == [1, 2, 5]

    ff678(x) = isequal(x, 1) ? missing : abs(x)
    x = [-1, 1, 1, missing, 1, 1, missing, -100]
    @test topk(x, 3, by=ff678) == [-100, -1]
    @test topkperm(x, 3, by=ff678) == [8, 1]
    @test topk(x, 3, by=ff678, rev=true) == [-1,-100]
    @test topkperm(x, 3, by=ff678, rev=true) == [1,8]

    x=[missing for _ in 1:1000]
    @test isequal(topk(x, 10), topk(x,10,threads=true))
    @test isequal(topk(x, 10), [missing])
    @test isequal(topk(x, 100), topk(x,100,threads=true))
    @test isequal(topk(x, 100), [missing])
    @test isequal(topkperm(x, 100), topkperm(x,100,threads=true))
    @test isequal(topkperm(x, 100), [missing])
    @test isequal(topkperm(x, 10), topkperm(x,10,threads=true))
    @test isequal(topkperm(x, 10), [missing])
    @test isequal(topkperm(x, 10,rev=true), topkperm(x,10,threads=true,rev=true))
    @test isequal(topkperm(x, 10,rev=true), [missing])

    x=CategoricalArray(rand(1000))
    # TODO categorical array is not thread safe - fortunately, it throws Errors - however, in future we may need to fix it
    @test_throws UndefRefError topk(x,10,lt=isless,threads=true)
end