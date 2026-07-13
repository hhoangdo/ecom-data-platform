# Diagram Editing Notes

## Reviewer Navigation and Arrow Conventions

- **Reviewer-facing primary diagram:** `detailed-architecture.svg`, embedded by the root README for the complete staged platform view.
- **Editable paired lifecycle view:** `detailed-architecture.excalidraw`. Keep the reviewer SVG and its detailed Excalidraw view aligned when a deployable unit or flow changes; the SVG is the review render, not an automatically regenerated artifact.
- **Companion sources:** `detailed-lambda_architecture.puml` gives the textual detailed architecture, while `architecture.excalidraw` and `lambda_architecture.puml` remain the general editable architecture views.
- **Service order:** sources feed ingestion, then branch to the realtime speed path and lakehouse batch truth path; orchestration, quality, governance, and evidence observe those paths rather than replacing them.

### Arrow conventions

Every arrow must identify its source, target, and flow label, and must preserve the shown left-to-right or top-to-bottom execution direction. Solid black arrows are primary data flow. Dashed green arrows are schema, contract, or quality flow; purple arrows are metadata; orange arrows are Airflow control; teal arrows are evidence/audit; blue arrows are local analytics; and gray arrows are BI consumption.

## Excalidraw

- Primary file: `architecture.excalidraw`
- If VS Code cannot open the custom editor, use **Reopen Editor With...** and choose **Text Editor** to inspect/fix raw JSON.
- Keep elements in canonical Excalidraw JSON format (with required element metadata fields), not simplified `label`-only shapes.

## PlantUML

- Primary file: `lambda_architecture.puml`
- The current consumption-layer labels name Apache Pinot and DuckDB as Section `02` implementation targets. Section `01` still owns source contracts and architecture documentation only.
- Workspace defaults in `.vscode/settings.json` use PlantUML server rendering:
  - `"plantuml.render": "PlantUMLServer"`
  - `"plantuml.server": "https://www.plantuml.com/plantuml"`
- Use PlantUML for system and architecture diagrams such as `lambda_architecture.puml`, `detailed-lambda_architecture.puml`, `schema_design.puml`, and `erd/physical_gold_model.puml`.

## Mermaid

- Primary folder for README-ready platform overview diagrams: `mermaid/`
- Each platform overview asset is stored as Mermaid source plus a rendered PNG sibling:
  - `01-data-generation.mmd` -> `01-data-generation.png`
  - `02-ingestion.mmd` -> `02-ingestion.png`
  - `03-lakehouse.mmd` -> `03-lakehouse.png`
  - `04-batch.mmd` -> `04-batch.png`
  - `05-streaming.mmd` -> `05-streaming.png`
  - `06-serving.mmd` -> `06-serving.png`
  - `07-local-analytics.mmd` -> `07-local-analytics.png`
  - `08-orchestration.mmd` -> `08-orchestration.png`
  - `09-governance.mmd` -> `09-governance.png`
- Render any one diagram with Mermaid CLI from the repo root:
  - `npx -y @mermaid-js/mermaid-cli -i architecture/diagrams/mermaid/01-data-generation.mmd -o architecture/diagrams/mermaid/01-data-generation.png -b white -w 1800`
- Keep the Mermaid source files as the editable truth and the PNGs as the README/GitHub embed targets.

## DBML

- Primary file for the readable Gold-only ERD: `erd/gold_layer_ERD.dbml`
- Use the official dbdiagram VS Code extension preview button, or run **DBML: Open Preview to the Side** from the Command Palette.
- `erd/gold_layer_ERD.dbml` is the readable Gold-layer ERD for day-to-day inspection.
- `erd/physical_gold_model.puml` remains the exhaustive physical/logical reference across Bronze, Silver, and Gold.
- `.vscode/extensions.json` recommends the diagram and docs extensions this repo expects contributors to have available.

## Optional Offline PlantUML Setup

If you want local/offline rendering instead of the remote server:

1. Install Java and Graphviz (`dot`).
2. Verify:
   - `java -version`
   - `dot -V`
3. Change setting to `"plantuml.render": "Local"` and reload VS Code.
