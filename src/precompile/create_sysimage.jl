function create_sysimage(sysout::AbstractString)
    juliaCMD = Base.julia_cmd()
    tmpIMD = mktemp()
    close(tmpIMD[2])
    run(`$juliaCMD -e "using Pkg; Pkg.add(\"PackageCompiler\")"`)

    run(`$juliaCMD --trace-compile=$(tmpIMD[1]) -e "using InMemoryDatasets; ds = IMD.warmup();println(ds)"`)
    tmpIMD_out = mktemp()
    close(tmpIMD_out[2])
    f = open(tmpIMD[1])
    fout = open(tmpIMD_out[1], "w")
    # TODO should make this part less hard coding
    write(fout, "using REPL, Pkg, InMemoryDatasets, InMemoryDatasets.PooledArrays, InMemoryDatasets.DataAPI, InMemoryDatasets.PrettyTables, InMemoryDatasets.Tables, PackageCompiler, TOML, Logging, SuiteSparse
    ")
    for line in eachline(f)
        # not sure why this happens
        write(fout, replace(line, "000000"=>""))
    end
    close(fout)
    close(f)
    run(`$juliaCMD -e "using PackageCompiler;
    create_sysimage(:InMemoryDatasets, sysimage_path=\"$(sysout)\", precompile_execution_file=\"$(tmpIMD_out[1])\")"`)
    println("Now exit julia and re-run it again using $(sysout) as `--sysimage`, e.g. in unix type OS you can use the following command:
    julia --sysimage $sysout")
end
