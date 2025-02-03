function CodeBlock (cb)
    local button_html = [[
<button class="copy" title="Copy">
    <svg width="16" height="16"><use href="#svg-copy"></use></svg>
</button>
    ]]
    local wrapper = pandoc.Div(
        { pandoc.RawInline("html", button_html), cb },
        pandoc.Attr("", { "code-wrapper" })
    )
    return wrapper
end
