// Setup PMTiles protocol (same as coverage/index.html)
const protocol = new pmtiles.Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);

// Initialize map with political/vector style (same as coverage map)
const map = new maplibregl.Map({
  container: "map",
  hash: "map",
  style: "style.json",
  center: [8, 47], // Center on Switzerland/Europe
  zoom: 6,
});
map.setMaxZoom(7.9);

// Add Mapterhorn logo control (same as other pages)
class LogoControl {
  onAdd(map) {
    this._map = map;
    this._container = document.createElement("div");
    this._container.className = "maplibregl-ctrl";
    this._container.innerHTML =
      '<a href="/"><img width=240 src="https://mapterhorn.github.io/.github/brand/screen/mapterhorn-logo.png"/></a>';
    this._container.style.marginBottom = "-11px";
    this._container.style.marginLeft = "-5px";
    return this._container;
  }
  onRemove() {
    this._container.parentNode.removeChild(this._container);
    this._map = undefined;
  }
}
map.addControl(new LogoControl(), "bottom-left");

// Setup MapLibre GL Draw with rectangle mode
const modes = MapboxDraw.modes;
modes.draw_rectangle = DrawRectangle;

const draw = new MapboxDraw({
  modes: modes,
  displayControlsDefault: false,
  controls: {
    polygon: false,
    trash: false,
  },
  styles: [
    {
      id: "gl-draw-polygon-fill",
      type: "fill",
      filter: ["all", ["==", "$type", "Polygon"]],
      paint: {
        "fill-color": "#3bb2d0",
        "fill-opacity": 0.3,
      },
    },
    {
      id: "gl-draw-polygon-stroke",
      type: "line",
      filter: ["all", ["==", "$type", "Polygon"]],
      paint: {
        "line-color": "#3bb2d0",
        "line-width": 3,
      },
    },
  ],
});
map.addControl(draw, "top-right");

// Remove the map control - using panel button instead

// Coverage query class
class CoverageQuery {
  constructor() {
    this.tileIndex = new Map();
    this.loaded = false;
  }

  async loadCoverage() {
    const response = await fetch("source-coverage.csv");
    const text = await response.text();
    const lines = text.trim().split("\n").slice(1);

    lines.forEach((line) => {
      const [x, y, z, source, maxzoom] = line.split(",");
      const key = `${z}-${x}-${y}`;
      if (!this.tileIndex.has(key)) {
        this.tileIndex.set(key, []);
      }
      this.tileIndex.get(key).push({
        source,
        maxzoom: parseInt(maxzoom),
      });
    });
    this.loaded = true;
  }

  getTilesAtZoom(bbox, zoom) {
    console.log('Using tilebelt.pointToTile for bbox:', bbox, 'at zoom:', zoom);
    const sw = tilebelt.pointToTile(bbox[0], bbox[1], zoom);
    const ne = tilebelt.pointToTile(bbox[2], bbox[3], zoom);

    console.log('SW tile:', sw, 'NE tile:', ne);

    const tiles = [];
    for (let x = sw[0]; x <= ne[0]; x++) {
      for (let y = ne[1]; y <= sw[1]; y++) {
        tiles.push([x, y, zoom]);
      }
    }
    console.log('Generated', tiles.length, 'tiles at zoom', zoom);
    return tiles;
  }

  /**
   * Query data source coverage for a given bounding box
   *
   * @param {Array} sw - Southwest corner [longitude, latitude]
   * @param {Array} ne - Northeast corner [longitude, latitude]
   * @returns {Array} Array of sources with their maximum zoom levels
   */
  queryCoverage(sw, ne) {
    const sources = new Map();
    const bbox = [sw[0], sw[1], ne[0], ne[1]]; // [west, south, east, north]

    // Search through multiple zoom levels from high to low resolution
    // Starting at zoom 12 because that's the "macrotile" zoom used in Mapterhorn's
    // aggregation pipeline - the primary zoom where most coverage tiles are stored
    for (let zoom = 12; zoom >= 5; zoom--) {
      // Get all tiles that intersect with the bounding box at this zoom level
      const tiles = this.getTilesAtZoom(bbox, zoom);

      // Check each tile to see if we have coverage data for it
      tiles.forEach(([x, y, z]) => {
        const key = `${z}-${x}-${y}`;

        // Look up this tile in our pre-loaded coverage index
        if (this.tileIndex.has(key)) {
          // Process each data source found in this tile
          this.tileIndex.get(key).forEach((entry) => {
            // Keep only the highest resolution (maxzoom) for each source
            // If we haven't seen this source before, or if this entry has
            // higher resolution than what we've seen, store it
            if (
              !sources.has(entry.source) ||
              sources.get(entry.source).maxzoom < entry.maxzoom
            ) {
              sources.set(entry.source, entry);
            }
          });
        }
      });
    }

    // We search multiple zoom levels (12 down to 5) because the coverage data
    // uses hierarchical simplification - some areas may only have coverage
    // stored at lower zoom levels (parent tiles) rather than individual z12 tiles
    // This ensures we don't miss any available data sources

    return Array.from(sources.values());
  }
}

// Initialize coverage query
const coverageQuery = new CoverageQuery();

// Track current drawing state
let isDrawingMode = false;
let currentBboxId = null;

// Handle draw events
map.on("draw.create", async (e) => {
  const feature = e.features[0];
  const coords = feature.geometry.coordinates[0];

  // Store the current bbox ID for clearing later
  currentBboxId = feature.id;

  const lngs = coords.map((c) => c[0]);
  const lats = coords.map((c) => c[1]);
  const sw = [Math.min(...lngs), Math.min(...lats)];
  const ne = [Math.max(...lngs), Math.max(...lats)];

  // Show loading state
  document.getElementById("results").innerHTML =
    '<div class="loading">Querying coverage...</div>';

  // Query and display results
  const results = coverageQuery.queryCoverage(sw, ne);
  displayResults(results);

  // Update button state
  updateButtonState(false);
  document.body.classList.remove("drawing-mode");
  isDrawingMode = false;
});

map.on("draw.delete", () => {
  document.getElementById("results").innerHTML = "";
  document.getElementById("instructions").style.display = "block";
  currentBboxId = null;
  updateButtonState(false);
  document.body.classList.remove("drawing-mode");
  isDrawingMode = false;
});

// Function to update button state
function updateButtonState(drawing) {
  const drawBtn = document.getElementById("draw-bbox-btn");
  if (!drawBtn) return;

  if (drawing) {
    drawBtn.textContent = "Cancel drawing";
    drawBtn.classList.add("active");
  } else if (currentBboxId) {
    drawBtn.textContent = "Clear bounding box";
    drawBtn.classList.remove("active");
  } else {
    drawBtn.textContent = "Draw bounding box";
    drawBtn.classList.remove("active");
  }
}

// Display results function
function displayResults(sources) {
  const resultsDiv = document.getElementById("results");
  document.getElementById("instructions").style.display = "none";

  if (sources.length === 0) {
    resultsDiv.innerHTML =
      '<div class="no-results">No data sources found in this area</div>';
    return;
  }

  // Sort by maxzoom (highest resolution first)
  sources.sort((a, b) => b.maxzoom - a.maxzoom);

  const html = sources
    .map((source) => {
      const resolution = getResolutionForZoom(source.maxzoom);
      return `
            <div class="source-card">
                <div class="source-name">${source.source}</div>
                <div class="source-details">
                    <span class="zoom">Zoom ${source.maxzoom}</span>
                    <span class="resolution">${resolution}</span>
                </div>
            </div>
        `;
    })
    .join("");

  resultsDiv.innerHTML = html;
}

function getResolutionForZoom(zoom) {
  const resolutions = {
    19: "~0.15m",
    18: "~0.3m",
    17: "~0.6m",
    16: "~1.2m",
    15: "~2.4m",
    14: "~4.8m",
    13: "~9.5m",
    12: "~19m",
    11: "~38m",
    10: "~76m",
  };
  return resolutions[zoom] || `z${zoom}`;
}

// Setup draw button click
map.on("load", async () => {
  await coverageQuery.loadCoverage();

  const drawBtn = document.getElementById("draw-bbox-btn");

  if (drawBtn) {
    drawBtn.addEventListener("click", () => {
      if (isDrawingMode) {
        // Cancel drawing mode
        draw.changeMode("simple_select");
        map.getCanvas().style.cursor = "";
        document.body.classList.remove("drawing-mode");
        isDrawingMode = false;
        updateButtonState(false);
      } else if (currentBboxId) {
        // Clear existing bbox
        draw.delete(currentBboxId);
        currentBboxId = null;
        document.getElementById("results").innerHTML = "";
        document.getElementById("instructions").style.display = "block";
        updateButtonState(false);
      } else {
        // Start drawing mode - clear any existing drawings first
        draw.deleteAll();
        currentBboxId = null;

        // Enter drawing mode
        draw.changeMode("draw_rectangle");
        map.getCanvas().style.cursor = "crosshair";
        document.body.classList.add("drawing-mode");
        isDrawingMode = true;
        updateButtonState(true);
      }
    });
  }

  // Handle mode changes
  map.on("draw.modechange", (e) => {
    console.log("Mode changed to:", e.mode);
    if (e.mode === "simple_select") {
      map.getCanvas().style.cursor = "";
      document.body.classList.remove("drawing-mode");
      if (isDrawingMode && !currentBboxId) {
        // User cancelled drawing
        isDrawingMode = false;
        updateButtonState(false);
      }
    }
  });
});
