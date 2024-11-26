let sections = [];

async function init() {
    const response = await fetch(urlPrefix + "/search_index.json");
    const index = await response.json();
    const parser = new DOMParser();
    index.forEach(entry => {
        const doc = parser.parseFromString(entry.html, "text/html");
        const body = doc.querySelector("body");

        let currentSection;
        for (const child of body.children) {
            const anchor = child.querySelector(".anchor");
            if (anchor) {
                const title = anchor.innerText.replace(/\n/g, " ");
                currentSection = {
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

    const searchInput = document.querySelector("input[type=search]");
    const searchResults = document.querySelector(".search-results");
    searchInput.addEventListener("input", () => {
        const results = search(searchInput.value);
        searchResults.replaceChildren(...results.map(result => {
            const a = document.createElement("a");
            a.href = urlPrefix + "/" + result.section.link;
            a.innerHTML = result.context;
            return a;
        }));
    })
}

init();

function makeContext(text, i, length, windowSize = 40) {
    return text.slice(i - 20, i) + "<strong>" + text.slice(i, i + length) + "</strong>" + text.slice(i + length, i + length + 20);
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