function assert(condition) {
  if (!condition) {
    alert("Assertion failed");
    throw "Assertion failed";
  }
}

window.urlPrefix = "$url_prefix";

if ("serviceWorker" in navigator) {
  navigator.serviceWorker.register(urlPrefix + "/service-worker.js");
}

document.querySelector("article").focus();
const content = document.querySelector("article>.content");
const leftPane = document.querySelector(".left-pane");
const resizer = document.querySelector(".resizer");
resizer.addEventListener("mousedown", () => {
  resizer.classList.add("resizing");
  document.body.style.cursor = "col-resize";
  document.body.style.userSelect = document.body.style.webkitUserSelect = "none";
  document.addEventListener("mousemove", onMouseMove);
  document.addEventListener("mouseup", onMouseUp);
  function onMouseMove(e) {
    leftPane.style.width = Math.max(176, e.clientX) + "px";
    if (e.clientX < 130) {
      document.body.classList.add("sidenav-collapsed");
    } else if (e.clientX > 140) {
      document.body.classList.remove("sidenav-collapsed");
    }
  }
  function onMouseUp() {
    resizer.classList.remove("resizing");
    document.body.style.cursor = "";
    document.body.style.userSelect = document.body.style.webkitUserSelect = "";
    document.removeEventListener("mousemove", onMouseMove);
  }
});
resizer.addEventListener("dblclick", () => leftPane.style.removeProperty("width"));

const collapseButton = document.querySelector(".collapse-button");
collapseButton.addEventListener("click", () => document.body.classList.add("sidenav-collapsed"));
const expandButton = document.querySelector(".expand-button");
expandButton.addEventListener("click", () => document.body.classList.remove("sidenav-collapsed"));
document.addEventListener("keydown", event => {
  if (event.ctrlKey || event.altKey || event.metaKey) return;
  if (document.activeElement.tagName === "INPUT") return;
  if (event.key === "m") document.body.classList.toggle("sidenav-collapsed");
});

const menuButton = document.querySelector(".menu-button");
menuButton.addEventListener("click", () => {
  document.body.classList.toggle("mobile-expanded");
  if (leftPane.classList.contains("search-active")) closeSearch();
});

// Restore and save the state of the side navigation.
const navState = JSON.parse(localStorage.getItem("navState"));
if (navState) {
  leftPane.style.width = navState.width;
  if (navState.collapsed) document.body.classList.add("sidenav-collapsed");
  leftPane.scrollTop = navState.scrollTop;
}
window.addEventListener("beforeunload", () => {
  const navState = {
    width: leftPane.style.width,
    collapsed: document.body.classList.contains("sidenav-collapsed"),
    scrollTop: leftPane.scrollTop,
  };
  localStorage.setItem("navState", JSON.stringify(navState));
});

function syncSideNavWithLocation() {
  const target = document.querySelector("nav.side .target");
  if (target) target.classList.remove("target");
  document.querySelectorAll("nav.side details").forEach(details => details.open = false);

  let path = location.pathname;
  if (path.includes("single-page")) path = location.hash;
  if (path.length > 1) {
    document.querySelectorAll("nav.side a").forEach(a => {
      if (a.href.endsWith(path)) {
        a.classList.add("target");
        for (let parent = a.parentElement; parent; parent = parent.parentElement) {
          if (parent.tagName === "DETAILS") parent.open = true;
        }
      }
    });
  }
}

syncSideNavWithLocation();
addEventListener("hashchange", syncSideNavWithLocation);

function addContentEventHandlers() {
  function copyCode(button) {
    let codeBlock = button.nextElementSibling;
    navigator.clipboard.writeText(codeBlock.innerText).then(() => {
      const before = button.innerHTML;
      button.innerText = "Copied!";
      setTimeout(() => button.innerHTML = before, 1000);
    });
  }

  content.querySelectorAll("button.copy").forEach(button =>
    button.addEventListener("click", () => copyCode(button))
  );
}

addContentEventHandlers();
