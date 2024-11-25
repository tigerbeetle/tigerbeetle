function Link (link)
  link.target = link.target:gsub("README.md", "")
  link.target = link.target:gsub(".md", "")
  return link
end