using Random
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
        for j in 1:15
            @test partialsort(x, 1:j) == topk(x, j, rev=true)
            @test partialsort(x, 1:j, rev=true) == topk(x, j)
            @test partialsortperm(x, 1:j) == topkperm(x, j, rev=true)
            @test partialsortperm(x, 1:j, rev=true) == topkperm(x, j)
            @test abs.(partialsort(x, 1:j, by=abs)) == abs.(topk(x, j, rev=true, by=abs))
            @test abs.(partialsort(x, 1:j, rev=true, by=abs)) == abs.(topk(x, j, by=abs))
            @test partialsortperm(x, 1:j, by=abs) == topkperm(x, j, rev=true, by=abs)
            @test partialsortperm(x, 1:j, rev=true, by=abs) == topkperm(x, j, by=abs)
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
        for j in 1:15
            @test partialsort(x, 1:j) == topk(x, j, rev=true)
            @test partialsort(x, 1:j, rev=true) == topk(x, j)
            @test partialsortperm(x, 1:j) == topkperm(x, j, rev=true)
            @test partialsortperm(x, 1:j, rev=true) == topkperm(x, j)
        end
    end
    x = [1, 10, missing, 100, -1000, 32, 54, 0, missing, missing, -1]
    @test topk(x, 2) == [100, 54]
    @test topk(x, 2, rev=true) == [-1000, -1]
    @test topkperm(x, 2) == [4, 7]
    @test topkperm(x, 2, rev=true) == [5, 11]
    @test topk(x, 10) == [100, 54, 32, 10, 1, 0, -1, -1000]
    @test topk(x, 10, rev=true) == [-1000, -1, 0, 1, 10, 32, 54, 100]
    @test topkperm(x, 10) == [4, 7, 6, 2, 1, 8, 11, 5]
    @test topkperm(x, 10, rev=true) == [5, 11, 8, 1, 2, 6, 7, 4]
    @test isequal(topk([missing, missing], 2), [missing])
    @test isequal(topk([missing, missing], 2, rev=true), [missing])
    @test isequal(topkperm([missing, missing], 2), [missing])
    @test isequal(topkperm([missing, missing], 2, rev=true), [missing])
    @test topk(x, 2, by=abs) == [-1000, 100]
    @test topk(x, 2, by=abs, rev=true) == [0, 1]
    @test topkperm(x, 2, by=abs) == [5, 4]
    @test topkperm(x, 2, by=abs, rev=true) == [8, 1]

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
end