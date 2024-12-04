let sections = [];

const searchInput = document.querySelector("input[type=search]");
const searchResults = document.querySelector(".search-results");
const searchHotkey = document.querySelector(".search-container>.hotkey");
const searchClearButton = document.querySelector(".search-container>.clear-button");

async function init() {
  const response = await fetch(urlPrefix + "/search-index.json");
  const index = await response.json();
  const parser = new DOMParser();
  index.forEach(entry => {
    const doc = parser.parseFromString(entry.html, "text/html");
    const body = doc.querySelector("body");

    let pageTitle;
    let currentSection;
    for (const child of body.children) {
      const anchor = child.querySelector(".anchor");
      if (anchor) {
        const title = anchor.innerText.replace(/\n/g, " ");
        if (!pageTitle) pageTitle = title;
        currentSection = {
          pageTitle,
          title,
          path: entry.path,
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

init();

function onSearchInput() {
  const results = search(searchInput.value);
  const highlightQuery = `?highlight=${encodeURIComponent(searchInput.value)}`
  searchResults.replaceChildren(...results.map(result => {
    const a = document.createElement("a");
    a.href = urlPrefix + "/" + result.section.path + "/" + highlightQuery + result.section.hash;
    a.innerHTML = "<h3>" + result.section.pageTitle + "</h3><p>" + result.context + "</p>";
    return a;
  }));

  searchClearButton.style.display = searchInput.value === "" ? "none" : "block";
}

function search(term, maxResults = 100) {
  if (term.length === 0) return [];
  const escapedTerm = term.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const regex = new RegExp(escapedTerm, 'gi');
  let hits = [];
  for (const section of sections) {
    const match = regex.exec(section.text);
    if (match) {
      const firstIndex = match.index;
      const count = section.text.match(regex).length;
      let score = 1 + 0.01 * count;
      score -= 0.004 * firstIndex; // penalty if match isn't early
      if (section.pageTitle.match(regex)) {
        score += 10;
        if (term.length === section.pageTitle.length) score += 10; // exact match
        score -= 0.02 * Math.abs(term.length - section.pageTitle.length); // penalty
      }
      if (section.title.match(regex)) {
        score += 4;
        if (term.length === section.title.length) score += 10; // exact match
        score -= 0.02 * Math.abs(term.length - section.title.length); // penalty
      }
      hits.push({ firstIndex, score, section });
    }
  }
  hits.sort((a, b) => b.score - a.score);
  hits = hits.slice(0, maxResults);
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

  return `${escapeHtml(contextLeft)}<strong>${escapeHtml(highlight)}</strong>${escapeHtml(contextRight)}`;
}

function selectNextResult() {
  const selected = searchResults.querySelector(".selected");
  if (selected) {
    selected.classList.remove("selected");
    if (selected.nextSibling) {
      selected.nextSibling.classList.add("selected");
      return;
    }
  }
  if (searchResults.firstChild) {
    searchResults.firstChild.classList.add("selected");
  }
}

function selectPreviousResult() {
  const selected = searchResults.querySelector(".selected");
  if (selected) {
    selected.classList.remove("selected");
    if (selected.previousSibling) {
      selected.previousSibling.classList.add("selected");
      return;
    }
  }
  if (searchResults.lastChild) {
    searchResults.lastChild.classList.add("selected");
  }
}

function clickSelectedResult() {
  const selected = searchResults.querySelector(".selected") || searchResults.firstChild;
  if (!selected) return;
  selected.click();
}

document.addEventListener("keydown", event => {
  if (event.shiftKey || event.ctrlKey || event.altKey || event.metaKey) return;
  if (event.key === "/" && searchInput !== document.activeElement) {
    searchInput.focus();
    event.preventDefault();
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
    clickSelectedResult();
    event.preventDefault();
  } else if (event.key === "Escape") {
    searchInput.blur();
    searchInput.value = "";
    onSearchInput();
    event.preventDefault();
  }
});
searchClearButton.addEventListener("click", () => {
  searchInput.value = "";
  searchResults.replaceChildren();
  searchClearButton.style.display = "none";  if (searchInput !== document.activeElement) searchHotkey.style.display = "block";
});

if (location.search) {
  const params = new URLSearchParams(location.search);
  const highlight = params.get("highlight");
  if (highlight) highlightText(decodeURIComponent(highlight));
}

function highlightText(term) {
  const escapedTerm = term.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const regex = new RegExp(escapedTerm, 'gi');
  const content = document.querySelector('article>.content');
  traverseAndHighlight(content, regex);
}

function traverseAndHighlight(node, regex) {
  if (node.nodeType === Node.TEXT_NODE) {
    const match = node.nodeValue.match(regex);
    if (match) {
      const fragment = document.createDocumentFragment();

      let lastIndex = 0;
      node.nodeValue.replace(regex, (matchedText, offset) => {
        // Add plain text before the match
        if (offset > lastIndex) {
          fragment.appendChild(document.createTextNode(node.nodeValue.slice(lastIndex, offset)));
        }

        const span = document.createElement('span');
        span.className = 'highlight';
        span.textContent = matchedText;
        fragment.appendChild(span);

        lastIndex = offset + matchedText.length;
      });

      // Add any remaining text after the last match
      if (lastIndex < node.nodeValue.length) {
        fragment.appendChild(document.createTextNode(node.nodeValue.slice(lastIndex)));
      }

      node.parentNode.replaceChild(fragment, node);
    }
  } else if (node.nodeType === Node.ELEMENT_NODE) {
    node.childNodes.forEach(child => traverseAndHighlight(child, regex));
  }
}

function escapeHtml(unsafe) {
  return unsafe.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&#039;');
}