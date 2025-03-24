is_readme = PANDOC_STATE.input_files[1]:sub(-9) == "README.md"

function Link (link)
  local is_external = link.target:sub(1, 8) == "https://" or link.target:sub(1, 7) == "http://"
  local is_mailto = link.target:sub(1, 7) == "mailto:"
  local is_absolute = link.target:sub(1, 1) == "/"
  local is_anchor = link.target:sub(1, 1) == "#"

  local docs = "https://docs.tigerbeetle.com/"
  if link.target:sub(1, #docs) == docs then
    link.target = link.target:gsub(docs, "/")
    is_external = false
    is_absolute = true
  end

  -- We have to adjust relative links to go up one more level.
  if not (is_readme or is_external or is_mailto or is_absolute or is_anchor) then
    if link.target:sub(1, 2) == "./"  then
      link.target = "." .. link.target
    else
      link.target = "../" .. link.target
    end
  end

  -- Links to client documentation
  if link.target:sub(1, 12) == "/src/clients" then
    local _, target_level = link.target:gsub("/", "")
    local is_client_readme = link.target:find("README.md") and target_level == 4
    if is_client_readme then
      link.target = "/coding" .. link.target:sub(5) -- Cut "/src"
    else
      -- Make GitHub link
      link.target = "https://github.com/tigerbeetle/tigerbeetle/blob/main" .. link.target
    end
  end

  if not is_external then
    link.target = link.target:gsub("README%.md", "")
    link.target = link.target:gsub("%.md", "")
  end

  return link
end
