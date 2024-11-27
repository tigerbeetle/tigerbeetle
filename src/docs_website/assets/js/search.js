let sections = [];

const searchInput = document.querySelector("input[type=search]");
const searchResults = document.querySelector(".search-results");
const searchHotkey = document.querySelector(".search-container .hotkey");
const searchClearButton = document.querySelector(".search-container .clear-button");

async function init() {
    const response = await fetch(urlPrefix + "/search_index.json");
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
                    link: entry.path + anchor.hash,
                    text: title,
                };
                sections.push(currentSection);
            } else if (currentSection) {
                currentSection.text += " " + child.innerText.replace(/\n/g, " ");
            }
        }
    });
}

init();

function makeContext(text, i, length, windowSize = 40) {
    let contextLeft = "";
    let i0 = i - windowSize / 2;
    if (i0 > 0) contextLeft = "...";
    while (i0 > 0 && text[i0] !== ' ') i0--;
    contextLeft = contextLeft + text.slice(i0, i).trimLeft();

    let contextRight = "";
    let i1 = i + length + windowSize / 2;
    if (i1 < text.length) contextRight = "...";
    while (i1 < text.length && text[i1] !== ' ') i1++;
    contextRight = text.slice(i + length, i1).trimRight() + contextRight;

    const highlight = "<strong>" + text.slice(i, i + length) + "</strong>";
    return contextLeft + highlight + contextRight;
}

function search(term, maxResults = 20) {
    if (term.length === 0) return [];
    term = term.toLowerCase();
    let hits = [];
    for (const section of sections) {
        const searchText = section.text.toLowerCase();
        const firstIndex = searchText.indexOf(term);
        if (firstIndex >= 0) {
            let count = 0;
            for (let index = firstIndex; index >= 0; index = searchText.indexOf(term, index + 1)) {
                count++;
            }
            hits.push({ firstIndex, count, section });
        }
    }
    hits.sort((a, b) => b.count - a.count);
    hits = hits.slice(0, maxResults);
    hits.forEach(hit => hit.context = makeContext(hit.section.text, hit.firstIndex, term.length));
    return hits;
}

document.addEventListener("keydown", event => {
    if (event.key === "/" && searchInput !== document.activeElement) {
        searchInput.focus();
        event.preventDefault();
        return false;
    }
})

searchInput.addEventListener("focus", () => {
    searchHotkey.style.display = "none";
});
searchInput.addEventListener("blur", () => {
    if (searchInput.value === "") searchHotkey.style.display = "block";
});
searchInput.addEventListener("input", () => {
    const results = search(searchInput.value);
    searchResults.replaceChildren(...results.map(result => {
        const a = document.createElement("a");
        a.href = urlPrefix + "/" + result.section.link;
        a.innerHTML = "<h3>" + result.section.pageTitle + "</h3><p>" + result.context + "</p>";
        return a;
    }));
    
    searchClearButton.style.display = searchInput.value === "" ? "none" : "block";
})
searchClearButton.addEventListener("click", () => {
    searchInput.value = "";
    searchResults.replaceChildren();
    searchClearButton.style.display = "none";
    if (searchInput !== document.activeElement) searchHotkey.style.display = "block";
})