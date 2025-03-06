function Pandoc(doc)
  local cwd_path = pandoc.system.get_working_directory()
  local abs_path = PANDOC_STATE.input_files[1]
  local website_path = "/src/docs_website";
  local repo_path = cwd_path:sub(1, -#website_path)
  local rel_path = pandoc.path.make_relative(abs_path, repo_path)
  local edit_url = "https://github.com/tigerbeetle/tigerbeetle/edit/main/"..rel_path

  local footer = pandoc.RawBlock("html", [[
<a class="edit-link" href="]]..edit_url..[[">
  <svg width="16" height="16" fill="none">
    <path d="M10 4L12 6M8.667 13.333H14M3.333 10.667L2.667 13.333L5.333 12.667L13.0574 4.94263C13.3074 4.69259 13.4478 4.35351 13.4478 4C13.4478 3.64641 13.3074 3.30733 13.0574 3.05729L12.9427 2.94263C12.6927 2.69267 12.3536 2.55225 12 2.55225C11.6465 2.55225 11.3075 2.69267 11.0574 2.94263L3.333 10.667Z" stroke-width="1.333" stroke-linecap="round" stroke-linejoin="round" stroke="currentColor"/>
  </svg>
  Edit this page
</a>]])

  table.insert(doc.blocks, footer)
  return doc
end
