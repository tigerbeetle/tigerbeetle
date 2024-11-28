is_readme = PANDOC_STATE.input_files[1]:sub(-9) == "README.md"

function Link (link)
  link.target = link.target:gsub("README.md", "")
  link.target = link.target:gsub(".md", "")

  if not is_readme then
    -- We have to adjust relative links to go up one more level.
    if link.target:sub(1, 2) == "./"  then
      link.target = "." .. link.target
    elseif link.target:sub(1, 3) == "../" then
      link.target = "../" .. link.target
    end
  end

  return link
end