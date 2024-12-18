window.urlPrefix = "$url_prefix";

if ("serviceWorker" in navigator) {
  navigator.serviceWorker.register(urlPrefix + "/service-worker.js");
}

const leftPane = document.querySelector(".left-pane");
const resizer = document.querySelector(".resizer");
resizer.addEventListener("mousedown", () => {
  resizer.classList.add("resizing");
  document.body.style.cursor = "col-resize";
  document.body.style.userSelect = document.body.style.webkitUserSelect = "none";
  document.addEventListener("mousemove", onMouseMove);
  document.addEventListener("mouseup", onMouseUp);
  function onMouseMove(e) {
    leftPane.style.width = Math.max(160, e.clientX) + "px";
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
resizer.addEventListener("dblclick", () => leftPane.style.width = "300px");

const collapseButton = document.querySelector(".collapse-button");
collapseButton.addEventListener("click", () => document.body.classList.add("sidenav-collapsed"));
const expandButton = document.querySelector(".expand-button");
expandButton.addEventListener("click", () => document.body.classList.remove("sidenav-collapsed"));

const navSide = document.querySelector("nav.side");
const menuHeads = [...navSide.querySelectorAll(".menu-head")];
menuHeads.forEach(e => e.addEventListener("click", ev => {
  if (ev.target == e) e.classList.toggle("expanded");
}));

syncSideNavWithLocation();

const menuButton = document.querySelector(".menu-button");
menuButton.addEventListener("click", () => leftPane.classList.toggle("mobile-expanded"));

// Restore and save the state of the side navigation
const navSideState = JSON.parse(localStorage.getItem("navSideState"));
if (navSideState) {
  leftPane.style.width = navSideState.width;
  if (navSideState.collapsed) document.body.classList.add("sidenav-collapsed");
  navSideState.expanded.forEach((e, i) => { if (e) menuHeads[i].classList.add("expanded"); });
  leftPane.scrollTop = navSideState.scrollTop;
}
window.addEventListener("beforeunload", () => {
  const navSideState = {
    width: leftPane.style.width,
    collapsed: document.body.classList.contains("sidenav-collapsed"),
    expanded: menuHeads.map(e => e.classList.contains("expanded")),
    scrollTop: leftPane.scrollTop,
  };
  localStorage.setItem("navSideState", JSON.stringify(navSideState));
});

function syncSideNavWithLocation() {
  const target = document.querySelector("nav.side .target");
  if (target) target.classList.remove("target");

  let path = location.pathname;
  if (path.endsWith("/")) path = path.slice(0, -1);
  document.querySelectorAll("nav.side a").forEach(a => {
    if (a.href.endsWith(path)) {
      a.classList.add("target");
      for (let parent = a.parentElement; parent; parent = parent.parentElement) {
        if (parent.matches(".menu")) {
          const head = parent.previousSibling;
          if (head?.matches?.(".menu-head")) head.classList.add("expanded");
        } else if (parent.matches(".menu-head")) {
          parent.classList.add("expanded"); // expand for discoverability
        }
      }
    }
  });
}