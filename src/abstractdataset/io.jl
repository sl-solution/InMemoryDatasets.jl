"""
    IMD.getmaxwidths(ds::AbstractDataset,
                            io::IO,
                            rowindices1::AbstractVector{Int},
                            rowindices2::AbstractVector{Int},
                            rowlabel::Symbol,
                            rowid::Union{Integer, Nothing},
                            show_eltype::Bool,
                            buffer::IOBuffer,
                            truncstring::Int;
                            mapformats::Bool = true)

Calculate, for each column of an AbstractDataset, the maximum
string width used to render the name of that column, its type, and the
longest entry in that column -- among the rows of the data frame
will be rendered to IO. The widths for the columns that will be displayed 
are returned as a vector.

Return a `Vector{Int}` giving the maximum string widths required to render
each column, including that column's name and type.

NOTE: The last entry of the result vector is the string width of the
implicit row ID column contained in every `AbstractDataset`.

# Arguments
- `ds::AbstractDataset`: The data frame whose columns will be printed.
- `io::IO`: The `IO` to which `ds` is to be printed
- `rowindices1::AbstractVector{Int}: A set of indices of the first
  chunk of the AbstractDataset that would be rendered to IO.
- `rowindices2::AbstractVector{Int}: A set of indices of the second
  chunk of the AbstractDataset that would be rendered to IO. Can
  be empty if the AbstractDataset would be printed without any
  ellipses.
- `rowlabel::AbstractString`: The label that will be used when rendered the
  numeric ID's of each row. Typically, this will be set to "Row".
- `rowid`: Used to handle showing `DataFrameRow`.
- `show_eltype`: Whether to print the column type
   under the column name in the heading.
- `buffer`: buffer passed around to avoid reallocations in `ourstrwidth`
- `truncstring`: The length of the string to be truncated. The string will
   not be truncated if it is equal to 0.
- `mapformats`: Whether to calculate the max widths after mapping format 
   for each column.
"""
function getmaxwidths(ds::AbstractDataset,
                      io::IO,
                      rowindices1::AbstractVector{Int},
                      rowindices2::AbstractVector{Int},
                      rowlabel::Symbol,
                      rowid::Union{Integer, Nothing},
                      show_eltype::Bool,
                      buffer::IOBuffer,
                      truncstring::Int;
                      mapformats::Bool = true)

    maxwidths = zeros(Int, size(ds, 2) + 1)

    undefstrwidth = ourstrwidth(io, "#undef", buffer, truncstring)

    ct = show_eltype ? batch_compacttype(Any[eltype(c) for c in eachcol(ds)]) : String[]
    tty_cols = displaysize(io)[2]
    maxwidthsum = 0
    j = 1
    for (col_idx, (name, col)) in enumerate(pairs(eachcol(ds)))
        # (1) Consider length of column name
        # do not truncate column name
        maxwidth = ourstrwidth(io, name, buffer, 0)
        # Calculates max widths after mapping formats if mapformats = true, because formats may affact the max widths
        if mapformats
            f = getformat(ds, col_idx)
            col = f.(col)
        end
        # (2) Consider length of longest entry in that column
        for indices in (rowindices1, rowindices2), i in indices
            if isassigned(col, i)
                maxwidth = max(maxwidth, ourstrwidth(io, col[i], buffer, truncstring))
            else
                maxwidth = max(maxwidth, undefstrwidth)
            end
        end
        if show_eltype
            # do not truncate eltype name
            maxwidths[j] = max(maxwidth, ourstrwidth(io, ct[col_idx], buffer, 0))
        else
            maxwidths[j] = maxwidth
        end
        maxwidthsum += (maxwidths[j] + 2)
        j += 1
        # If the sum of column widths is already larger than COLUMNS, do not need calculate max width for the rest columns
        if maxwidthsum >= tty_cols
            break
        end
    end

    # do not truncate rowlabel
    if rowid isa Nothing
        rowmaxwidth1 = isempty(rowindices1) ? 0 : ndigits(maximum(rowindices1))
        rowmaxwidth2 = isempty(rowindices2) ? 0 : ndigits(maximum(rowindices2))
        maxwidths[j] = max(max(rowmaxwidth1, rowmaxwidth2),
                           ourstrwidth(io, rowlabel, buffer, 0))
    else
        maxwidths[j] = max(ndigits(rowid), ourstrwidth(io, rowlabel, buffer, 0))
    end

    return maxwidths
end

"""
    show(io::IO, mime::MIME, ds::AbstractDataset)

Render a data set to an I/O stream in MIME type `mime`.

# Arguments
- `io::IO`: The I/O stream to which `ds` will be printed.
- `mime::MIME`: supported MIME types are: `"text/plain"`, `"text/html"`, `"text/latex"`,
  `"text/csv"`, `"text/tab-separated-values"` (the last two MIME types do not support
   showing `#undef` values)
- `ds::AbstractDataset`: The data set to print.

Additionally selected MIME types support passing the following keyword arguments:
- MIME type `"text/plain"` accepts all listed keyword arguments and therir behavior
  is identical as for `show(::IO, ::AbstractDataset)`
- MIME type `"text/html"` accepts `summary` keyword argument which
  allows to choose whether to print a brief string summary of the data set.

# Examples
```jldoctest
julia> show(stdout, MIME("text/latex"), Dataset(A = 1:3, B = ["x", "y", "z"]))
\\begin{tabular}{r|cc}
\t& A & B\\\\
\t\\hline
\t& Int64 & String\\\\
\t\\hline
\t1 & 1 & x \\\\
\t2 & 2 & y \\\\
\t3 & 3 & z \\\\
\\end{tabular}

julia> show(stdout, MIME("text/csv"), Dataset(A = 1:3, B = ["x", "y", "z"]))
"A","B"
1,"x"
2,"y"
3,"z"
```
"""
Base.show(io::IO, mime::MIME, ds::AbstractDataset)
Base.show(io::IO, mime::MIME"text/html", ds::AbstractDataset;
          summary::Bool = true, eltypes::Bool = true, mapformats = true) =
    _show(io, mime, ds, summary = summary, eltypes = eltypes, mapformats = mapformats)
Base.show(io::IO, mime::MIME"text/latex", ds::AbstractDataset; eltypes::Bool = true, mapformats = true, formats = true) =
    _show(io, mime, ds, eltypes = eltypes, mapformats = mapformats, formats = formats)
Base.show(io::IO, mime::MIME"text/csv", ds::AbstractDataset; mapformats = true) =
    printtable(io, ds, header = true, separator = ',', mapformats = mapformats)
Base.show(io::IO, mime::MIME"text/tab-separated-values", ds::AbstractDataset; mapformats = true) =
    printtable(io, ds, header = true, separator = '\t', mapformats = mapformats)
Base.show(io::IO, mime::MIME"text/plain", ds::AbstractDataset; kwargs...) =
    show(io, ds; kwargs...)

##############################################################################
#
# HTML output
#
##############################################################################

function digitsep(value::Integer)
    # Adapted from https://github.com/IainNZ/Humanize.jl
    value = string(abs(value))
    group_ends = reverse(collect(length(value):-3:1))
    groups = [value[max(end_index - 2, 1):end_index]
              for end_index in group_ends]
    return join(groups, ',')
end

function html_escape(cell::AbstractString)
    cell = replace(cell, "&"=>"&amp;")
    cell = replace(cell, "<"=>"&lt;")
    cell = replace(cell, ">"=>"&gt;")
    # Replace quotes so that the resulting string could also be used in the attributes of
    # HTML tags
    cell = replace(cell, "\""=>"&quot;")
    cell = replace(cell, "'"=>"&apos;")
    return cell
end

function _show(io::IO, ::MIME"text/html", ds::AbstractDataset;
               summary::Bool=true, eltypes::Bool=true, rowid::Union{Int, Nothing}=nothing, mapformats = true)
    _check_consistency(ds)

    # we will pass around this buffer to avoid its reallocation in ourstrwidth
    buffer = IOBuffer(Vector{UInt8}(undef, 80), read=true, write=true)

    if rowid !== nothing
        if size(ds, 2) == 0
            rowid = nothing
        elseif size(ds, 1) != 1
            throw(ArgumentError("rowid may be passed only with a single row data frame"))
        end
    end

    mxrow, mxcol = size(ds)
    if get(io, :limit, false)
        tty_rows, tty_cols = displaysize(io)
        mxrow = min(mxrow, tty_rows)
        maxwidths = getmaxwidths(ds, io, 1:mxrow, 0:-1, :X, nothing, true, buffer, 0, mapformats = mapformats) .+ 2
        mxcol = min(mxcol, searchsortedfirst(cumsum(maxwidths), tty_cols))
    end

    cnames = _names(ds)[1:mxcol]
    write(io, "<table class=\"data-set\">")
    write(io, "<thead>")
    write(io, "<tr>")
    write(io, "<th></th>")
    for column_name in cnames
        write(io, "<th>$(html_escape(String(column_name)))</th>")
    end
    write(io, "</tr>")
    write(io, "<th></th>")
    for column_name in cnames
        write(io, "<th>$(getformat(ds, column_name))</th>")
    end
    write(io, "</tr>")
    if eltypes
        write(io, "<tr>")
        write(io, "<th></th>")
        # We put a longer string for the type into the title argument of the <th> element,
        # which the users can hover over. The limit of 256 characters is arbitrary, but
        # we want some maximum limit, since the types can sometimes get really-really long.
        types = Any[eltype(ds[!, idx]) for idx in 1:mxcol]
        ct, ct_title = batch_compacttype(types), batch_compacttype(types, 256)
        for j in 1:mxcol
            s = html_escape(ct[j])
            title = html_escape(ct_title[j])
            write(io, "<th title=\"$title\">$s</th>")
        end
        write(io, "</tr>")
    end
    write(io, "</thead>")
    write(io, "<tbody>")
    if summary
        omitmsg = if mxcol < size(ds, 2)
                      " (omitted printing of $(size(ds, 2)-mxcol) columns)"
                  else
                      ""
                  end
        if ds isa SubDataset
            mainmsg = "<p>$(digitsep(nrow(ds))) rows × $(digitsep(ncol(ds))) columns$omitmsg</p><p><b> SubDataset (view of Dataset)</p>"
        else
            mainmsg = if !isempty(index(ds).sortedcols) && index(ds).grouped[]
                    "<p>$(digitsep(nrow(ds))) rows × $(digitsep(ncol(ds))) columns$omitmsg</p><p><b> Grouped Dataset with $(index(ds).ngroups[]) groups </p><p> Grouped by: $(join(_names(ds)[index(ds).sortedcols],", ")) </p>"
                elseif !isempty(index(ds).sortedcols)
                    "<p>$(digitsep(nrow(ds))) rows × $(digitsep(ncol(ds))) columns$omitmsg</p><p><b> Sorted Dataset </p><p> Sorted by: $(join(_names(ds)[index(ds).sortedcols],", ")) </p>"
                else
                    "<p>$(digitsep(nrow(ds))) rows × $(digitsep(ncol(ds))) columns$omitmsg</p>"
                end
        end
        write(io, mainmsg)

    end
    for row in 1:mxrow
        write(io, "<tr>")
        if rowid === nothing
            write(io, "<th>$row</th>")
        else
            write(io, "<th>$rowid</th>")
        end
        for column_name in cnames
            if isassigned(ds[!, column_name], row)
                cell_val = getformat(ds, column_name)(ds[row, column_name])
                if ismissing(cell_val)
                    write(io, "<td><em>missing</em></td>")
                elseif cell_val isa Markdown.MD
                    write(io, "<td>")
                    show(io, "text/html", cell_val)
                    write(io, "</td>")
                elseif cell_val isa SHOW_TABULAR_TYPES
                    write(io, "<td><em>")
                    cell = sprint(ourshow, cell_val, 0)
                    write(io, html_escape(cell))
                    write(io, "</em></td>")
                else
                    cell = sprint(ourshow, cell_val, 0)
                    write(io, "<td>$(html_escape(cell))</td>")
                end
            else
                write(io, "<td><em>#undef</em></td>")
            end
        end
        write(io, "</tr>")
    end
    if size(ds, 1) > mxrow
        write(io, "<tr>")
        write(io, "<th>&vellip;</th>")
        for column_name in cnames
            write(io, "<td>&vellip;</td>")
        end
        write(io, "</tr>")
    end
    write(io, "</tbody>")
    write(io, "</table>")
    nothing
end

# function Base.show(io::IO, mime::MIME"text/html", dsr::DataFrameRow;
#                    summary::Bool=true, eltypes::Bool=true)
#     r, c = parentindices(dsr)
#     summary && write(io, "<p>DataFrameRow ($(length(dsr)) columns)</p>")
#     _show(io, mime, view(parent(dsr), [r], c), summary=false, eltypes=eltypes, rowid=r)
# end
#
# function Base.show(io::IO, mime::MIME"text/html", dsrs::DataFrameRows;
#                    summary::Bool=true, eltypes::Bool=true)
#     ds = parent(dsrs)
#     summary && write(io, "<p>$(nrow(ds))×$(ncol(ds)) DataFrameRows</p>")
#     _show(io, mime, ds, summary=false, eltypes=eltypes)
# end
#
# function Base.show(io::IO, mime::MIME"text/html", dscs::DataFrameColumns;
#                    summary::Bool=true, eltypes::Bool=true)
#     ds = parent(dscs)
#     if summary
#         write(io, "<p>$(nrow(ds))×$(ncol(ds)) DataFrameColumns</p>")
#     end
#     _show(io, mime, ds, summary=false, eltypes=eltypes)
# end
#
# function Base.show(io::IO, mime::MIME"text/html", gd::GroupedDataFrame)
#     N = length(gd)
#     keys = html_escape(join(string.(groupcols(gd)), ", "))
#     keystr = length(gd.cols) > 1 ? "keys" : "key"
#     groupstr = N > 1 ? "groups" : "group"
#     write(io, "<p><b>$(nameof(typeof(gd))) with $N $groupstr based on $keystr: $keys</b></p>")
#     if N > 0
#         nrows = size(gd[1], 1)
#         rows = nrows > 1 ? "rows" : "row"
#
#         identified_groups = [html_escape(string(col, " = ",
#                                                 repr(first(gd[1][!, col]))))
#                              for col in gd.cols]
#
#         write(io, "<p><i>First Group ($nrows $rows): ")
#         join(io, identified_groups, ", ")
#         write(io, "</i></p>")
#         show(io, mime, gd[1], summary=false)
#     end
#     if N > 1
#         nrows = size(gd[N], 1)
#         rows = nrows > 1 ? "rows" : "row"
#
#         identified_groups = [html_escape(string(col, " = ",
#                                                 repr(first(gd[N][!, col]))))
#                              for col in gd.cols]
#
#         write(io, "<p>&vellip;</p>")
#         write(io, "<p><i>Last Group ($nrows $rows): ")
#         join(io, identified_groups, ", ")
#         write(io, "</i></p>")
#         show(io, mime, gd[N], summary=false)
#     end
# end

##############################################################################
#
# LaTeX output
#
##############################################################################

function latex_char_escape(char::Char)
    if char == '\\'
        return "\\textbackslash{}"
    elseif char == '~'
        return "\\textasciitilde{}"
    else
        return string('\\', char)
    end
end

function latex_escape(cell::AbstractString)
    replace(cell, ['\\','~', '#', '$', '%', '&', '_', '^', '{', '}']=>latex_char_escape)
end

function _show(io::IO, ::MIME"text/latex", ds::AbstractDataset;
               eltypes::Bool=true, rowid=nothing, formats::Bool = true, mapformats = true)
    _check_consistency(ds)

    # we will pass around this buffer to avoid its reallocation in ourstrwidth
    buffer = IOBuffer(Vector{UInt8}(undef, 80), read=true, write=true)

    if rowid !== nothing
        if size(ds, 2) == 0
            rowid = nothing
        elseif size(ds, 1) != 1
            throw(ArgumentError("rowid may be passed only with a single row data frame"))
        end
    end

    mxrow, mxcol = size(ds)
    if get(io, :limit, false)
        tty_rows, tty_cols = get(io, :displaysize, displaysize(io))
        mxrow = min(mxrow, tty_rows)
        maxwidths = getmaxwidths(ds, io, 1:mxrow, 0:-1, :X, nothing, true, buffer, 0) .+ 2
        mxcol = min(mxcol, searchsortedfirst(cumsum(maxwidths), tty_cols))
    end

    cnames = _names(ds)[1:mxcol]
    alignment = repeat("c", mxcol)
    write(io, "\\begin{tabular}{r|")
    write(io, alignment)
    mxcol < size(ds, 2) && write(io, "c")
    write(io, "}\n")
    write(io, "\t& ")
    header = join(map(c -> latex_escape(string(c)), cnames), " & ")
    write(io, header)
    mxcol < size(ds, 2) && write(io, " & ")
    write(io, "\\\\\n")
    write(io, "\t\\hline\n")
    if formats
        write(io, "\t& ")
        ftm = Any[string(getformat(ds, idx)) for idx in 1:mxcol]
        header = join(latex_escape.(ftm), " & ")
        write(io, header)
        mxcol < size(ds, 2) && write(io, " & ")
        write(io, "\\\\\n")
        write(io, "\t\\hline\n")
    end
    if eltypes
        write(io, "\t& ")
        ct = batch_compacttype(Any[eltype(ds[!, idx]) for idx in 1:mxcol])
        header = join(latex_escape.(ct), " & ")
        write(io, header)
        mxcol < size(ds, 2) && write(io, " & ")
        write(io, "\\\\\n")
        write(io, "\t\\hline\n")
    end
    for row in 1:mxrow
        write(io, "\t")
        write(io, @sprintf("%d", rowid === nothing ? row : rowid))
        for col in 1:mxcol
            write(io, " & ")
            if !isassigned(ds[!, col], row)
                print(io, "\\emph{\\#undef}")
            else
                if mapformats
                    cell = getformat(ds, col)(ds[row, col])
                else
                    cell = ds[row, col]
                end
                if ismissing(cell)
                    print(io, "\\emph{missing}")
                elseif cell isa Markdown.MD
                    print(io, strip(repr(MIME("text/latex"), cell)))
                elseif cell isa SHOW_TABULAR_TYPES
                    print(io, "\\emph{")
                    print(io, latex_escape(sprint(ourshow, cell, 0, context=io)))
                    print(io, "}")
                else
                    if showable(MIME("text/latex"), cell)
                        show(io, MIME("text/latex"), cell)
                    else
                        print(io, latex_escape(sprint(ourshow, cell, 0, context=io)))
                    end
                end
            end
        end
        mxcol < size(ds, 2) && write(io, " & \$\\dots\$")
        write(io, " \\\\\n")
    end
    if size(ds, 1) > mxrow
        write(io, "\t\$\\dots\$")
        for col in 1:mxcol
            write(io, " & \$\\dots\$")
        end
        mxcol < size(ds, 2) && write(io, " & ")
        write(io, " \\\\\n")
    end
    write(io, "\\end{tabular}\n")
    nothing
end

# function Base.show(io::IO, mime::MIME"text/latex", dsr::DataFrameRow; eltypes::Bool=true)
#     r, c = parentindices(dsr)
#     _show(io, mime, view(parent(dsr), [r], c), eltypes=eltypes, rowid=r)
# end
#
# Base.show(io::IO, mime::MIME"text/latex", dsrs::DataFrameRows; eltypes::Bool=true) =
# 	_show(io, mime, parent(dsrs), eltypes=eltypes)
# Base.show(io::IO, mime::MIME"text/latex", dscs::DataFrameColumns; eltypes::Bool=true) =
# 	_show(io, mime, parent(dscs), eltypes=eltypes)
#
# function Base.show(io::IO, mime::MIME"text/latex", gd::GroupedDataFrame)
#     N = length(gd)
#     keys = join(latex_escape.(string.(groupcols(gd))), ", ")
#     keystr = length(gd.cols) > 1 ? "keys" : "key"
#     groupstr = N > 1 ? "groups" : "group"
#     write(io, "$(nameof(typeof(gd))) with $N $groupstr based on $keystr: $keys\n\n")
#     if N > 0
#         nrows = size(gd[1], 1)
#         rows = nrows > 1 ? "rows" : "row"
#
#         identified_groups = [latex_escape(string(col, " = ",
#                                                  repr(first(gd[1][!, col]))))
#                              for col in gd.cols]
#
#         write(io, "First Group ($nrows $rows): ")
#         join(io, identified_groups, ", ")
#         write(io, "\n\n")
#         show(io, mime, gd[1])
#     end
#     if N > 1
#         nrows = size(gd[N], 1)
#         rows = nrows > 1 ? "rows" : "row"
#
#         identified_groups = [latex_escape(string(col, " = ",
#                                                  repr(first(gd[N][!, col]))))
#                              for col in gd.cols]
#
#         write(io, "\n\$\\dots\$\n\n")
#         write(io, "Last Group ($nrows $rows): ")
#         join(io, identified_groups, ", ")
#         write(io, "\n\n")
#         show(io, mime, gd[N])
#     end
# end
#
##############################################################################
#
# MIME: text/csv and text/tab-separated-values
#
##############################################################################

escapedprint(io::IO, x::SHOW_TABULAR_TYPES, escapes::AbstractString) =
    escapedprint(io, summary(x), escapes)
escapedprint(io::IO, x::Any, escapes::AbstractString) =
    escapedprint(io, sprint(print, x), escapes)
escapedprint(io::IO, x::AbstractString, escapes::AbstractString) =
    escape_string(io, x, escapes)

function printtable(io::IO,
                    ds::AbstractDataset;
                    header::Bool = true,
                    separator::Char = ',',
                    quotemark::Char = '"',
                    missingstring::AbstractString = "missing",
                    nothingstring::AbstractString = "nothing",
                    mapformats = true)
    _check_consistency(ds)
    n, p = size(ds)
    etypes = eltype.(eachcol(ds))
    if header
        cnames = _names(ds)
        for j in 1:p
            print(io, quotemark)
            print(io, cnames[j])
            print(io, quotemark)
            if j < p
                print(io, separator)
            else
                print(io, '\n')
            end
        end
    end
    quotestr = string(quotemark)
    for i in 1:n
        for j in 1:p
            if mapformats
                cell = getformat(ds, j)(ds[i, j])
            else
                cell = ds[i, j]
            end
            if ismissing(cell)
                print(io, missingstring)
            elseif isnothing(cell)
                print(io, nothingstring)
            else
                if cell isa Markdown.MD
                    print(io, quotemark)
                    r = repr(cell)
                    escapedprint(io, chomp(r), quotestr)
                    print(io, quotemark)
                elseif !(etypes[j] <: Real)
                    print(io, quotemark)
                    escapedprint(io, cell, quotestr)
                    print(io, quotemark)
                else
                    print(io, cell)
                end
            end
            if j < p
                print(io, separator)
            else
                print(io, '\n')
            end
        end
    end
    nothing
end

# function Base.show(io::IO, mime::MIME"text/csv", dsr::DataFrameRow)
#     r, c = parentindices(dsr)
#     show(io, mime, view(parent(dsr), [r], c))
# end
#
# function Base.show(io::IO, mime::MIME"text/tab-separated-values", dsr::DataFrameRow)
#     r, c = parentindices(dsr)
#     show(io, mime, view(parent(dsr), [r], c))
# end
#
# Base.show(io::IO, mime::MIME"text/csv",
#           dss::Union{DataFrameRows, DataFrameColumns}) =
#     show(io, mime, parent(dss))
# Base.show(io::IO, mime::MIME"text/tab-separated-values",
#           dss::Union{DataFrameRows, DataFrameColumns}) =
#     show(io, mime, parent(dss))
#
# function Base.show(io::IO, mime::MIME"text/csv", gd::GroupedDataFrame)
#     isfirst = true
#     for sds in gd
#         printtable(io, sds, header = isfirst, separator = ',')
#         isfirst && (isfirst = false)
#     end
# end
#
# function Base.show(io::IO, mime::MIME"text/tab-separated-values", gd::GroupedDataFrame)
#     isfirst = true
#     for sds in gd
#         printtable(io, sds, header = isfirst, separator = '\t')
#         isfirst && (isfirst = false)
#     end
# end
