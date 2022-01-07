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
        gatherby(ds, i)
    end
    for i in 1:ncol(ds)
        groupby(ds, 1:i)
        gatherby(ds, 1:i)
    end
    byrow(ds, all, :, by = isequal(1))
    byrow(ds, sum)
    for op in (+, *, -, /)
        byrow(ds, op, 1:2)
    end
    combine(groupby(ds, [6,1]), (1,2)=>cor)
    combine(groupby(ds,1), Ref([1,2,3,7,8]) .=> [sum, mean, length, maximum, minimum, var, std, median, median!, sort])
    combine(groupby(ds,1), r"x1$" .=> [sum, mean, length, maximum, minimum, var, std, median, sort])
    ds2 = ds[1:2, [1,3]]
    combine(gatherby(ds,1), Ref([1,2,3,7,8]) .=> [median, sort])
    combine(gatherby(ds,1), Ref([1,2,3,7,8]) .=> [sum, mean, length, maximum, minimum, var, std])
    combine(gatherby(ds,1), r"x1$" .=> [sum, mean, length, maximum, minimum, var, std])

    ds2 = ds[1:2, [1,3,7]]
    innerjoin(ds, ds2, on = [:x1, :x3, :x7])
    leftjoin(ds, ds2, on = [:x1, :x3, :x7])
    leftjoin(ds, ds2, on = [:x1, :x3, :x7], accelerate = true)
    ds3 = unique(ds, 1)[:, [1,3,5,7]]
    leftjoin(ds, ds3, on = :x1, makeunique = true)
    innerjoin(ds, ds3, on = :x1, makeunique = true)
    transpose(ds, 1:ncol(ds))
    transpose(groupby(ds,1:8), [2,3])
    # views
    ds = Dataset("A"=> ["A", "A" ,"A", "A", "B", "B","G"],
        "B"=> ["C", "D", "E", "B", "F", "G","N"]
        )
    select!(ds, 2,1)
    rename!(ds, [:child, :parent])

    function ff(ds, i)
        newds = leftjoin(ds, unique(dropmissing(ds,ncol(ds), view=true)[!, reverse(ncol(ds):-1:ncol(ds)-1)], [1,2] , view=true), on = [i+1=>1], makeunique = true)
        rename!(x->replace(x,"1"=>"grand"), newds)
        newds
    end

    ff(ff(ff(ds, 1),2), 3)

    # some from test
    store = Dataset([[Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-02"), Date("2020-01-01"), Date("2019-10-01"), Date("2019-10-02"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-03"), Date("2019-10-03")],
        ["A", "A", "B", "A", "B", "A", "B", "B", "A", "B"]], [:date, :store])
    roster = Dataset([Union{Missing, String}["A", "A", "B", "A", "B", "A", "B", "B"],
             Union{Missing, Int64}[4, 1, 8, 2, 5, 3, 6, 7],
             Union{Missing, Date}[Date("2019-10-04"), Date("2019-09-30"), Date("2019-10-04"), Date("2019-10-02"), Date("2019-09-30"), Date("2019-10-03"), Date("2019-10-02"), Date("2019-10-03")],
             Union{Missing, Date}[Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-06"), Date("2019-10-04"), Date("2019-10-04"), Date("2019-10-05"), Date("2019-10-04"), Date("2019-10-05")]],
             ["store", "employee_ID", "start_date", "end_date"])
    inn_r1 =  innerjoin(store, roster, on = [:date => (:start_date, nothing)], makeunique = true, stable = true)
    inn_r1_v =  innerjoin(store, view(roster, :, :), on = [:date => (:start_date, nothing)], makeunique = true, stable = true)
    inn_r1_a =  innerjoin(store, roster, on = [:date => (:start_date, nothing)], makeunique = true, stable = true, accelerate = true)
    inn_r1_v_a =  innerjoin(store, view(roster, :, :), on = [:date => (:start_date, nothing)], makeunique = true, stable = true, accelerate = true)

    inn_r1 =  innerjoin(store, roster, on = [:store => :store, :date => (nothing, :start_date)], stable = true)
    inn_r1_a =  innerjoin(store, roster, on = [:store => :store, :date => (nothing, :start_date)], stable = true, accelerate = true)
    inn_r1 =  innerjoin(store, roster, on = [:store => :store, :date => (:end_date, :start_date)], stable = true)
    inn_r1_a =  innerjoin(store, roster, on = [:store => :store, :date => (:end_date, :start_date)], stable = true, accelerate = true)

    inn_r1 =  innerjoin(view(store, :, :), roster, on = [:store => :store, :date => (nothing, :start_date)], stable = true)
    inn_r1_a =  innerjoin(view(store, :, :), roster, on = [:store => :store, :date => (nothing, :start_date)], stable = true, accelerate = true)
    inn_r1 =  innerjoin(view(store, :, :), roster, on = [:store => :store, :date => (:end_date, :start_date)], stable = true)
    inn_r1_a =  innerjoin(view(store, :, :), roster, on = [:store => :store, :date => (:end_date, :start_date)], stable = true, accelerate = true)

    ds = Dataset(foo = ["one", "one", "one", "two", "two","two"],
                      bar = ['A', 'B', 'C', 'A', 'B', 'C'],
                      baz = [1, 2, 3, 4, 5, 6],
                      zoo = ['x', 'y', 'z', 'q', 'w', 't'])
    ds2 = transpose(groupby(ds, :foo, stable = true), :baz, id = :bar)
    ds = Dataset(g1 = ["g2", "g1", "g1", "g2", "g1", "g3"], c1 = ["c1", "c1", "c1", "c1", "c1", "c1"],
               id1 = ["id2_g2", "id1_g1", "id2_g1", "id1_g2", "id3_g1", "id4_g3"], id2 = [2, 3, 1, 5, 4, 6],
               val1 = [1, 2, 3, 4, 5, 6], val2 = [1.1, 2.1, 0.5, 1.3, 1.0, 2])
   ds2 = transpose(ds, [:val1, :val2])
   ds3 = transpose(ds, [:val1, :val2], id = :id2)
   ds4 = transpose(ds, [:val1, :val2], id = [:id1, :id2])
   ds5 = transpose(groupby(ds, 1, stable = true), [:val1, :val2])
   ds6 = transpose(groupby(ds, 1, stable = true), [:val1, :val2], id = :id1)
   ds7 = transpose(groupby(ds, 1, stable = true), [:val1, :val2], id = [:id1, :id2])
   ds8 = transpose(groupby(ds, 1:2, stable = true), [:val1, :val2], id = :id2)
   ds9 = transpose(groupby(ds, 1:2, stable = true), [:val1], id = [:id2, :id1])

   ds = Dataset([Union{Missing, Int64}[1, 1, 1, 2, 2, 2],
                Union{Missing, String}["foo", "bar", "monty", "foo", "bar", "monty"],
                Union{Missing, String}["a", "b", "c", "d", "e", "f"],
                Union{Missing, Int64}[1, 2, 3, 4, 5, 6]], [:g, :key, :foo, :bar])
    dst = transpose(groupby(ds, :g), (:foo, :bar), id = :key, variable_name = "_variables_")

    ds = Dataset(g = [1, 1, 1, 2, 2],
                        x1_int = [0, 0, 1, missing, 2],
                        x2_int = [3, 2, 1, 3, -2],
                        x1_float = [1.2, missing, -1.0, 2.3, 10],
                        x2_float = [missing, missing, 3.0, missing, missing],
                        x3_float = [missing, missing, -1.4, 3.0, -100.0])
    isequal(byrow(ds, argmin, r"float"), [:x1_float, missing, :x3_float, :x1_float, :x3_float])
    ds1 = Dataset(a = Union{String, Missing}["a", "b", "a", "b", "a", "b"],
                    b = Vector{Union{Int, Missing}}(1:6),
                    c = Union{Int, Missing}[1:3;1:3])
    ds = vcat(ds1, ds1)
    findall(nonunique(ds)) == collect(7:12)
    fmt(x)=1
    setformat!(ds, :a=>fmt)
    findall(nonunique(ds, :a, mapformats = true)) == 2:12
    unique(ds) == ds1
    unique(ds, 2:3) == ds1

    t2 = now()
    Dataset(x1 = "Finished warmup in", x2 = t2-t1)
end
