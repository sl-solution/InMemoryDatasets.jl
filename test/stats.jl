@testset "topk" begin
    # general usage
    for i in 1:100
        x = rand(Int, 11)
        for j in 1:11
            @test partialsort(x, 1:j) == topk(x, j, rev=true)
            @test partialsort(x, 1:j, rev=true) == topk(x, j)
            @test partialsortperm(x, 1:j) == topk(x, j, rev=true, output_indices = true)[2]
            @test partialsortperm(x, 1:j, rev=true) == topk(x, j, output_indices = true)[2]
        end
        x = rand(11)
        for j in 1:11
            @test partialsort(x, 1:j) == topk(x, j, rev=true)
            @test partialsort(x, 1:j, rev=true) == topk(x, j)
            @test partialsortperm(x, 1:j) == topk(x, j, rev=true, output_indices = true)[2]
            @test partialsortperm(x, 1:j, rev=true) == topk(x, j, output_indices=true)[2]
        end
        x = randn(11)
        for j in 1:11
            @test partialsort(x, 1:j) == topk(x, j, rev=true)
            @test partialsort(x, 1:j, rev=true) == topk(x, j)
            @test partialsortperm(x, 1:j) == topk(x, j, rev=true, output_indices=true)[2]
            @test partialsortperm(x, 1:j, rev=true) == topk(x, j, output_indices=true)[2]
        end
    end
    x = [1, 10, missing, 100, -1000, 32, 54, 0, missing, missing, -1]
    @test topk(x, 2) == [100, 54]
    @test topk(x, 2, rev=true) == [-1000, -1]
    @test topk(x, 2, output_indices=true)[2] == [4, 7]
    @test topk(x, 2, rev=true, output_indices=true)[2] == [5, 11]
    @test topk(x, 10) == [100, 54, 32, 10, 1, 0, -1, -1000]
    @test topk(x, 10, rev=true) == [-1000, -1, 0, 1, 10, 32, 54, 100]
    @test topk(x, 10, output_indices=true)[2] == [4, 7, 6, 2, 1, 8, 11, 5]
    @test topk(x, 10, rev=true, output_indices=true)[2] == [5, 11, 8, 1, 2, 6, 7, 4]
    @test isequal(topk([missing, missing], 2), [missing])
    @test isequal(topk([missing, missing], 2, rev = true), [missing])
    @test isequal(topk([missing, missing], 2, output_indices=true)[2], [missing])
    @test isequal(topk([missing, missing], 2, rev=true, output_indices=true)[2], [missing])
end