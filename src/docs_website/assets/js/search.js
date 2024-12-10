let pages = [];
let sections = [];

const searchInput = document.querySelector("input[type=search]");
const searchResults = document.querySelector(".search-results");
const searchStats = document.querySelector(".search-stats");
const searchHotkey = document.querySelector(".search-box>.hotkey");
const searchClearButton = document.querySelector(".search-box>.clear-button");
const content = document.querySelector("article>.content");

async function initSearch() {
  const response = await fetch(urlPrefix + "/search-index.json");
  pages = await response.json();
  const parser = new DOMParser();
  pages.forEach((page, pageIndex) => {
    const doc = parser.parseFromString(page.html, "text/html");
    const body = doc.querySelector("body");

    let currentSection;
    for (const child of body.children) {
      const anchor = child.querySelector(".anchor");
      if (anchor) {
        const title = anchor.innerText.replace(/\n/g, " ");
        if (!page.title) page.title = title;
        currentSection = {
          pageIndex,
          title,
          path: page.path,
          hash: anchor.hash,
          text: title,
        };
        sections.push(currentSection);
      } else if (currentSection) {
        currentSection.text += " " + child.innerText.replace(/\n/g, " ");
      }
    }
  });

  if (searchInput.value) onSearchInput(); // Repeat search once the index is fetched.
}

initSearch();

function onSearchInput() {
  const results = search(searchInput.value);
  const groups = [];
  let currentGroup;
  for (const result of results) {
    if (!currentGroup || currentGroup.pageIndex !== result.section.pageIndex) {
      currentGroup = { pageIndex: result.section.pageIndex, results: [] };
      groups.push(currentGroup);
    }
    currentGroup.results.push(result);
  }
  const rootMenu = document.createElement("div");
  rootMenu.classList.add("menu");
  searchResults.replaceChildren(rootMenu);
  for (const group of groups) {
    const menuHead = document.createElement("div");
    rootMenu.appendChild(menuHead);
    menuHead.classList.add("menu-head");
    menuHead.classList.add("expanded");
    menuHead.innerHTML = `<p>${pages[group.pageIndex].title}</p>`;
    menuHead.addEventListener("click", () => menuHead.classList.toggle("expanded"));
    const menu = document.createElement("div");
    rootMenu.appendChild(menu);
    menu.classList.add("menu");
    for (const result of group.results) {
      const a = document.createElement("a");
      menu.appendChild(a);
      a.addEventListener("click", (e) => {
        e.preventDefault();
        selectResult(a);
      });
      a.href = urlPrefix + "/" + result.section.path + "/" + result.section.hash;
      a.pageIndex = result.section.pageIndex;
      const h3 = document.createElement("h3");
      a.appendChild(h3);
      h3.innerText = result.section.title;
      const p = document.createElement("p");
      a.appendChild(p);
      p.innerHTML = result.context;
    }
  }

  const resultText = results.length === 1 ? "result" : "results";
  const pageText = groups.length === 1 ? "page" : "pages";
  searchStats.innerText = `${results.length} ${resultText} on ${groups.length} ${pageText}`

  highlightText(searchInput.value, searchResults);

  const displaySearch = searchInput.value === "" ? "none" : "block";
  searchClearButton.style.display = displaySearch;
  searchResults.style.display = displaySearch;
  searchStats.style.display = displaySearch;
}

function search(term) {
  if (term.length === 0) return [];
  const escapedTerm = term.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const regex = new RegExp(escapedTerm, 'gi');
  let hits = [];
  for (const section of sections) {
    const match = regex.exec(section.text);
    if (match) {
      const firstIndex = match.index;
      const count = section.text.match(regex).length;
      hits.push({ firstIndex, count, section });
    }
  }
  hits.forEach(hit => hit.context = makeContext(hit.section.text, hit.firstIndex, term.length));
  return hits;
}

function makeContext(text, i, length, windowSize = 40) {
  const highlight = text.slice(i, i + length);

  let contextLeft = "";
  let i0 = Math.max(0, i - windowSize / 2);
  if (i0 > 0) contextLeft = "...";
  while (i0 > 0 && text[i0] !== ' ') i0--;
  contextLeft = contextLeft + text.slice(i0, i).trimLeft();

  let contextRight = "";
  let i1 = Math.min(text.length, i + length + windowSize / 2);
  if (i1 < text.length) contextRight = "...";
  while (i1 < text.length && text[i1] !== ' ') i1++;
  contextRight = text.slice(i + length, i1).trimRight() + contextRight;

  // return `${escapeHtml(contextLeft)}<strong>${escapeHtml(highlight)}</strong>${escapeHtml(contextRight)}`;
  return contextLeft + highlight + contextRight;
}

function selectResult(node) {
  searchResults.querySelectorAll(".selected").forEach(r => r.classList.remove("selected"));
  node.classList.add("selected");
  scrollIntoViewIfNeeded(node, searchResults.parentNode);
  // Preview result
  const page = pages[node.pageIndex];
  content.innerHTML = page.html;
  window.history.pushState({}, "", node.href);
  document.title = "TigerBeetle Docs | " + page.title;
  const anchor = document.getElementById(window.location.hash.slice(1));
  anchor.scrollIntoView();
  highlightText(searchInput.value, content);
}

function selectNextResult() {
  const nodes = [...searchResults.querySelectorAll("a")];
  if (nodes.length === 0) return;
  const selected = searchResults.querySelector(".selected");
  const i = selected ? nodes.indexOf(selected) + 1 : 0;
  selectResult(nodes[i % nodes.length]);
}

function selectPreviousResult() {
  const nodes = [...searchResults.querySelectorAll("a")];
  if (nodes.length === 0) return;
  const selected = searchResults.querySelector(".selected");
  const i = selected ? nodes.indexOf(selected) - 1 : -1;
  selectResult(nodes[(i + nodes.length) % nodes.length]);
}

function scrollIntoViewIfNeeded(element, parent) {
  const elementRect = element.getBoundingClientRect();
  const parentRect = parent.getBoundingClientRect();

  const isOutOfView = (
    elementRect.top < parentRect.top ||
    elementRect.left < parentRect.left ||
    elementRect.bottom > parentRect.bottom ||
    elementRect.right > parentRect.right
  );

  if (isOutOfView) {
    element.scrollIntoView({ block: 'center', inline: 'center' });
  }
}

function closeSearch() {
  searchInput.blur();
  searchHotkey.style.display = "block";
  searchInput.value = "";
  onSearchInput();
  removeTextHighlight(content);
  syncSideNavWithLocation();
}

document.addEventListener("keydown", event => {
  if (event.shiftKey || event.ctrlKey || event.altKey || event.metaKey) return;
  if (event.key === "/" && searchInput !== document.activeElement) {
    searchInput.focus();
    event.preventDefault();
  } else if (event.key === "Escape") {
    if (searchInput === document.activeElement || searchInput.value !== "") {
      closeSearch();
      event.preventDefault();
    }
  }
})

searchInput.addEventListener("focus", () => {
  searchHotkey.style.display = "none";
});
searchInput.addEventListener("blur", () => {
  if (searchInput.value === "") searchHotkey.style.display = "block";
});
searchInput.addEventListener("input", onSearchInput);
searchInput.addEventListener("keydown", event => {
  if (event.shiftKey || event.ctrlKey || event.altKey || event.metaKey) return;
  if (event.key === "ArrowDown") {
    selectNextResult();
    event.preventDefault();
  } else if (event.key === "ArrowUp") {
    selectPreviousResult();
    event.preventDefault();
  } else if (event.key === "Enter") {
    const selected = searchResults.querySelector(".selected");
    if (!selected) selectNextResult();
    closeSearch();
    event.preventDefault();
  }
});
searchClearButton.addEventListener("click", () => {
  searchInput.value = "";
  onSearchInput();
  if (searchInput !== document.activeElement) searchHotkey.style.display = "block";
  removeTextHighlight(content);
});

function highlightText(term, container) {
  const escapedTerm = term.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const regex = new RegExp(`(${escapedTerm})`, 'gi');
  let replacements = [];
  const walker = document.createTreeWalker(container, NodeFilter.SHOW_TEXT, null);
  let node;
  while (node = walker.nextNode()) {
    const parent = node.parentNode;
    if (parent && node.nodeValue.match(regex)) {
      const span = document.createElement("span");
      span.innerHTML = node.nodeValue.replace(regex, '<span class="highlight">$1</span>');
      replacements.push({ parent, node, span });
    }
  }
  replacements.forEach(r => r.parent.replaceChild(r.span, r.node));
}

function removeTextHighlight(container) {
  container.querySelectorAll(".highlight").forEach(h => h.classList.remove("highlight"));
}

function escapeHtml(unsafe) {
  return unsafe.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&#039;');
}